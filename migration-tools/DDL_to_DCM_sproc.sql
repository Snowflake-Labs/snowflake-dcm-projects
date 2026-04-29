-- CALL DDL_TO_DCM_DEFINITIONS(
--     'DCM_DEMO',                 -- Database Name
--     NULL, --['RAW', 'SERVE'],   -- option to only process listed Schemas
--     NULL, --['VIEW','DYNAMIC TABLE','SCHEMA','GRANT']  -- option to only process listed object types
--     TRUE,                       -- write multiple definitions in one file (for better performance at scale)
--     'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live/DCM_Migration'    -- target path to workspace or stage folder 
-- );


CREATE OR REPLACE PROCEDURE DDL_TO_DCM_DEFINITIONS(
    db_name STRING,
    schema_list ARRAY,
    object_types ARRAY,
    group_files_by_type BOOLEAN,
    output_path STRING
)
RETURNS TABLE (
    SCHEMA STRING,
    OBJECT_TYPE STRING,
    OBJECT_NAME STRING,
    STATUS STRING,
    FILE_PATH STRING
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import re
import io

_IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_$]*$')

def _qid(name):
    """Safe-quote an identifier for use in dynamic SQL. Escapes internal
    double quotes and wraps the name in double quotes. Accepts names as
    returned from SHOW commands or validated user input."""
    if name is None:
        raise ValueError("identifier is None")
    s = str(name).strip()
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        return s
    return '"' + s.replace('"', '""') + '"'

def _validate_user_ident(name):
    """Strict validation for user-supplied identifiers. Returns the bare
    uppercase form; raises ValueError for anything that does not match a
    standard unquoted identifier or a well-formed quoted one."""
    if name is None:
        raise ValueError("identifier is None")
    s = str(name).strip()
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        return s[1:-1].replace('""', '"')
    if not _IDENT_RE.match(s):
        raise ValueError(f"invalid identifier: {name!r}")
    return s.upper()

# Canonical set of object types supported by this procedure. Values in the
# object_types are normalized (uppercased, spaces -> underscores)
# and must match one of these.
_CANONICAL_TYPES = {
    'TABLE', 'VIEW', 'DYNAMIC_TABLE', 'TASK', 'FUNCTION', 'PROCEDURE',
    'SEQUENCE', 'FILE_FORMAT', 'ALERT', 'TAG', 'STAGE', 'SCHEMA', 'GRANT',
}

def _normalize_type(t):
    return str(t).strip().upper().replace(' ', '_')

def main(session, db_name, schema_list, object_types, group_files_by_type, output_path):
    # 1. Normalize Inputs
    # Validate user-supplied identifiers before any dynamic SQL.
    try:
        db_name = _validate_user_ident(db_name)
    except ValueError as e:
        return session.create_dataframe(
            [("", "DATABASE", str(db_name), "ERROR", f"Invalid database identifier: {e}")],
            schema=["SCHEMA", "OBJECT_TYPE", "OBJECT_NAME", "STATUS", "FILE_PATH"]
        )
    q_db = _qid(db_name)

    allowed_schemas = None
    invalid_schema_rows = []
    if schema_list is not None:
        allowed_schemas = set()
        for s in schema_list:
            try:
                allowed_schemas.add(_validate_user_ident(s))
            except ValueError as e:
                invalid_schema_rows.append((db_name, "SCHEMA", str(s), "ERROR", f"Invalid schema identifier: {e}"))

    # Normalize object_types. None => all supported types. Otherwise every
    # value must map to a canonical type; any unknown value aborts the run
    # before any inventory or DDL work is performed.
    allowed_types = None
    if object_types is not None:
        allowed_types = set()
        invalid = []
        for t in object_types:
            norm = _normalize_type(t)
            if norm in _CANONICAL_TYPES:
                allowed_types.add(norm)
            else:
                invalid.append(str(t))
        if invalid:
            supported = ", ".join(sorted(_CANONICAL_TYPES))
            return session.create_dataframe(
                [(
                    "", "OBJECT_TYPE", ", ".join(invalid), "ERROR",
                    f"Unsupported object type(s): {', '.join(invalid)}. Supported: {supported}"
                )],
                schema=["SCHEMA", "OBJECT_TYPE", "OBJECT_NAME", "STATUS", "FILE_PATH"]
            )

    def type_allowed(t):
        if allowed_types is None:
            return True
        return _normalize_type(t) in allowed_types

    stage_root = output_path.rstrip('/')

    def qfqn(schema, obj=None):
        if obj is None:
            return f"{q_db}.{_qid(schema)}"
        return f"{q_db}.{_qid(schema)}.{_qid(obj)}"

    # 2. Build Inventory (Scan ALL schemas)
    try:
        objects_df = session.sql(f"SHOW OBJECTS IN DATABASE {q_db}").collect()
    except Exception as e:
        return session.create_dataframe(
            [(db_name, "DATABASE", db_name, "ERROR", f"Cannot access database '{db_name}': {e}")],
            schema=["SCHEMA", "OBJECT_TYPE", "OBJECT_NAME", "STATUS", "FILE_PATH"]
        )

    object_map = []
    generated_files = list(invalid_schema_rows)
    schema_comments = {}  # schema_name -> comment

    # Build a set of semantic view FQNs to exclude.
    # SHOW OBJECTS reports semantic views with kind='VIEW', so we need a
    # separate lookup to distinguish them from regular/secure views.
    semantic_view_fqns = set()
    try:
        sv_df = session.sql(f"SHOW SEMANTIC VIEWS IN DATABASE {q_db}").collect()
        for sv_row in sv_df:
            sv_fqn = qfqn(sv_row['schema_name'].upper(), sv_row['name'])
            semantic_view_fqns.add(sv_fqn)
    except Exception as e:
        generated_files.append((db_name, "SEMANTIC_VIEW_LOOKUP", "SEMANTIC_VIEW_LOOKUP", "WARNING", str(e)))

    for row in objects_df:
        s_name = row['schema_name'].upper()
        kind = row['kind']
        fqn_check = qfqn(s_name, row['name'])
        # Skip streams silently (not yet handled by this migration)
        if kind.upper() == 'STREAM':
            continue
        # Stages are handled via SHOW STAGES in the per-schema loop
        if kind.upper() == 'STAGE':
            continue
        # Skip semantic views: not supported by DCM at this point.
        # They appear in SHOW OBJECTS with kind='VIEW', so match by FQN.
        if fqn_check in semantic_view_fqns or 'SEMANTIC' in kind.upper():
            generated_files.append((s_name, kind, row['name'], "UNSUPPORTED", "semantic views"))
            continue
        # Respect object_types. SHOW OBJECTS reports dynamic tables
        # as kind='TABLE', so include tables when either TABLE or
        # DYNAMIC_TABLE is allowed; the actual kind is determined later from
        # GET_DDL and re-checked before emitting.
        if allowed_types is not None:
            k_up = kind.upper()
            if k_up == 'TABLE':
                if 'TABLE' not in allowed_types and 'DYNAMIC_TABLE' not in allowed_types:
                    continue
            elif _normalize_type(k_up) not in allowed_types:
                continue
        if s_name != 'INFORMATION_SCHEMA':
            fqn = qfqn(s_name, row['name'])
            object_map.append({
                "name": row['name'],
                "fqn": fqn,
                "schema": s_name,
                "kind": kind
            })

    # 2b. Scan tasks, functions, and procedures per schema
    #     (SHOW OBJECTS does not include these object types)
    schemas_to_scan = allowed_schemas if allowed_schemas else set()
    try:
        schemas_df = session.sql(f"SHOW SCHEMAS IN DATABASE {q_db}").collect()
        for row in schemas_df:
            s_name = row['name'].upper()
            if s_name == 'INFORMATION_SCHEMA':
                continue
            schema_comments[s_name] = row['comment'] or ''
            if not allowed_schemas:
                schemas_to_scan.add(s_name)
    except Exception as e:
        generated_files.append((db_name, "SCHEMA_LOOKUP", "SCHEMA_LOOKUP", "WARNING", str(e)))

    task_list = []
    callable_list = []  # functions and procedures
    simple_ddl_list = []  # sequences, file formats, alerts
    stage_list = []  # permanent internal stages
    grants_by_schema = {}  # schema -> [grant_stmt_strings]
    grant_failure_counts = {}  # schema -> count of failed SHOW GRANTS calls

    def format_grant_stmt(row):
        priv = row['privilege']
        if priv == 'OWNERSHIP':
            return ('OWNERSHIP', row['granted_on'], row['name'])
        granted_to = row['granted_to']
        if granted_to not in ('ROLE', 'DATABASE_ROLE'):
            return None
        grantee = row['grantee_name']
        granted_on = row['granted_on']
        obj_name = row['name']
        grant_opt = row['grant_option']
        target = f"DATABASE ROLE {grantee}" if granted_to == 'DATABASE_ROLE' else f"ROLE {grantee}"
        stmt = f"GRANT {priv} ON {granted_on} {obj_name} TO {target}"
        if str(grant_opt).upper() == 'TRUE':
            stmt += " WITH GRANT OPTION"
        return stmt + ";"

    def collect_grants(show_cmd, header, schema):
        if not type_allowed('GRANT'):
            return
        try:
            grant_rows = session.sql(show_cmd).collect()
            stmts = []
            for gr in grant_rows:
                result = format_grant_stmt(gr)
                if isinstance(result, tuple):
                    generated_files.append((schema, "GRANT", result[2], "UNSUPPORTED", f"{result[0]} grant"))
                elif result:
                    stmts.append(result)
            if stmts:
                grants_by_schema.setdefault(schema, []).append(header)
                grants_by_schema[schema].extend(stmts)
        except Exception:
            grant_failure_counts[schema] = grant_failure_counts.get(schema, 0) + 1

    # Build a map of callable (schema, name) -> language from INFORMATION_SCHEMA
    # so we can skip non-SQL functions/procedures at discovery time.
    # Key is (SCHEMA, NAME); values can collide for overloaded callables but
    # language is a per-name attribute in practice for our filtering purpose.
    non_sql_callables = {}  # (schema_upper, name_upper, domain) -> language
    try:
        rows = session.sql(
            f"SELECT PROCEDURE_SCHEMA, PROCEDURE_NAME, PROCEDURE_LANGUAGE "
            f"FROM {q_db}.INFORMATION_SCHEMA.PROCEDURES"
        ).collect()
        for r in rows:
            lang = (r['PROCEDURE_LANGUAGE'] or '').upper()
            if lang and lang != 'SQL':
                non_sql_callables[(r['PROCEDURE_SCHEMA'].upper(), r['PROCEDURE_NAME'].upper(), 'PROCEDURE')] = lang
    except Exception as e:
        generated_files.append((db_name, "PROCEDURE_LANGUAGE_LOOKUP", "PROCEDURE_LANGUAGE_LOOKUP", "WARNING", str(e)))
    try:
        rows = session.sql(
            f"SELECT FUNCTION_SCHEMA, FUNCTION_NAME, FUNCTION_LANGUAGE "
            f"FROM {q_db}.INFORMATION_SCHEMA.FUNCTIONS"
        ).collect()
        for r in rows:
            lang = (r['FUNCTION_LANGUAGE'] or '').upper()
            if lang and lang != 'SQL':
                non_sql_callables[(r['FUNCTION_SCHEMA'].upper(), r['FUNCTION_NAME'].upper(), 'FUNCTION')] = lang
    except Exception as e:
        generated_files.append((db_name, "FUNCTION_LANGUAGE_LOOKUP", "FUNCTION_LANGUAGE_LOOKUP", "WARNING", str(e)))

    # 2b-pre. Collect database-level grants
    db_grant_lines = []
    if type_allowed('GRANT'):
        try:
            db_grant_rows = session.sql(f"SHOW GRANTS ON DATABASE {q_db}").collect()
            for gr in db_grant_rows:
                result = format_grant_stmt(gr)
                if isinstance(result, tuple):
                    generated_files.append((db_name, "GRANT", result[2], "UNSUPPORTED", f"{result[0]} grant"))
                elif result:
                    db_grant_lines.append(result)
        except Exception as e:
            generated_files.append((db_name, "GRANTS", "DATABASE", "ERROR", str(e)))
        try:
            db_future_rows = session.sql(f"SHOW FUTURE GRANTS IN DATABASE {q_db}").collect()
            for fr in db_future_rows:
                obj_type = fr['grant_on']
                grantee = fr['grantee_name']
                priv = fr['privilege']
                generated_files.append((db_name, "GRANT", f"FUTURE {obj_type} -> {grantee}", "UNSUPPORTED", f"future grant ({priv})"))
        except Exception:
            pass

    for s_name in schemas_to_scan:
        if type_allowed('GRANT'):
            collect_grants(
                f"SHOW GRANTS ON SCHEMA {qfqn(s_name)}",
                f"-- Schema: {db_name}.{s_name}",
                s_name
            )
            try:
                future_rows = session.sql(f"SHOW FUTURE GRANTS IN SCHEMA {qfqn(s_name)}").collect()
                for fr in future_rows:
                    obj_type = fr['grant_on']
                    grantee = fr['grantee_name']
                    priv = fr['privilege']
                    generated_files.append((s_name, "GRANT", f"FUTURE {obj_type} -> {grantee}", "UNSUPPORTED", f"future grant ({priv})"))
            except Exception:
                pass

        if not type_allowed('TASK'):
            tasks_df = []
        else:
            tasks_df = None
        try:
            if tasks_df is None:
                tasks_df = session.sql(f"SHOW TASKS IN SCHEMA {qfqn(s_name)}").collect()
            for row in tasks_df:
                task_name = row['name']
                fqn = qfqn(s_name, task_name)
                task_list.append({
                    "name": task_name,
                    "fqn": fqn,
                    "schema": s_name,
                    "warehouse": row['warehouse'],
                    "schedule": row['schedule'],
                    "definition": row['definition'],
                    "comment": row['comment'] or '',
                })
                object_map.append({
                    "name": task_name,
                    "fqn": fqn,
                    "schema": s_name,
                    "kind": "TASK"
                })
        except Exception as e:
            generated_files.append((s_name, "TASK", "*", "ERROR", str(e)))

        callable_show_cmds = []
        if type_allowed('FUNCTION'):
            callable_show_cmds.append((f"SHOW USER FUNCTIONS IN SCHEMA {qfqn(s_name)}", "FUNCTION"))
        if type_allowed('PROCEDURE'):
            callable_show_cmds.append((f"SHOW USER PROCEDURES IN SCHEMA {qfqn(s_name)}", "PROCEDURE"))
        for show_cmd, ddl_domain in callable_show_cmds:
            try:
                rows = session.sql(show_cmd).collect()
                for row in rows:
                    row_dict = row.as_dict()
                    obj_name = row_dict['name']
                    # Skip the GENERATE_DEFINITIONS procedure itself
                    if obj_name.upper() == 'GENERATE_DEFINITIONS':
                        continue
                    # Skip non-SQL functions/procedures up-front.
                    non_sql_lang = non_sql_callables.get((s_name.upper(), obj_name.upper(), ddl_domain))
                    if non_sql_lang:
                        generated_files.append((s_name, ddl_domain, obj_name, "UNSUPPORTED", f"language={non_sql_lang}"))
                        continue
                    # Skip Data Metric Functions: GET_DDL signature does not
                    # match what we generate for regular functions.
                    if ddl_domain == 'FUNCTION' and row_dict.get('is_data_metric') == 'Y':
                        generated_files.append((s_name, ddl_domain, obj_name, "UNSUPPORTED", "data metric function"))
                        continue
                    arguments = row_dict.get('arguments', '')
                    fqn = qfqn(s_name, obj_name)
                    callable_list.append({
                        "name": obj_name,
                        "fqn": fqn,
                        "schema": s_name,
                        "domain": ddl_domain,
                        "arguments": arguments,
                    })
                    object_map.append({
                        "name": obj_name,
                        "fqn": fqn,
                        "schema": s_name,
                        "kind": ddl_domain
                    })
            except Exception as e:
                generated_files.append((s_name, ddl_domain, "*", "ERROR", str(e)))

        simple_show_cmds = []
        if type_allowed('SEQUENCE'):
            simple_show_cmds.append((f"SHOW SEQUENCES IN SCHEMA {qfqn(s_name)}", "SEQUENCE"))
        if type_allowed('FILE_FORMAT'):
            simple_show_cmds.append((f"SHOW FILE FORMATS IN SCHEMA {qfqn(s_name)}", "FILE_FORMAT"))
        if type_allowed('ALERT'):
            simple_show_cmds.append((f"SHOW ALERTS IN SCHEMA {qfqn(s_name)}", "ALERT"))
        if type_allowed('TAG'):
            simple_show_cmds.append((f"SHOW TAGS IN SCHEMA {qfqn(s_name)}", "TAG"))
        for show_cmd, ddl_domain in simple_show_cmds:
            try:
                rows = session.sql(show_cmd).collect()
                for row in rows:
                    obj_name = row['name']
                    fqn = qfqn(s_name, obj_name)
                    simple_ddl_list.append({
                        "name": obj_name,
                        "fqn": fqn,
                        "schema": s_name,
                        "domain": ddl_domain,
                    })
                    object_map.append({
                        "name": obj_name,
                        "fqn": fqn,
                        "schema": s_name,
                        "kind": ddl_domain
                    })
            except Exception as e:
                generated_files.append((s_name, ddl_domain, "*", "ERROR", str(e)))

        # Stages: split by external / temporary / permanent
        if not type_allowed('STAGE'):
            stages_df = []
        else:
            stages_df = None
        try:
            if stages_df is None:
                stages_df = session.sql(f"SHOW STAGES IN SCHEMA {qfqn(s_name)}").collect()
            for row in stages_df:
                stage_name = row['name']
                url = row['url'] or ''
                stage_type = (row['type'] or '').upper()
                fqn = qfqn(s_name, stage_name)
                if url:
                    generated_files.append((s_name, "STAGE", stage_name, "UNSUPPORTED", "external stage"))
                elif 'TEMPORARY' in stage_type:
                    continue
                else:
                    stage_list.append({
                        "name": stage_name,
                        "fqn": fqn,
                        "schema": s_name,
                        "directory_enabled": row['directory_enabled'],
                        "comment": row['comment'],
                    })
                    object_map.append({
                        "name": stage_name,
                        "fqn": fqn,
                        "schema": s_name,
                        "kind": "STAGE"
                    })
        except Exception as e:
            generated_files.append((s_name, "STAGE", "*", "ERROR", str(e)))

    # Sort by length (descending)
    object_map.sort(key=lambda x: len(x["name"]), reverse=True)

    grouped_ddl = {}  # (schema, type_folder) -> [ddl_text, ...]

    # Helper: map object kind to a folder name
    def kind_to_folder(kind):
        mapping = {
            'TABLE': 'tables', 'VIEW': 'views', 'DYNAMIC TABLE': 'dynamic_tables',
            'TASK': 'tasks', 'FUNCTION': 'functions', 'PROCEDURE': 'procedures',
            'SEQUENCE': 'sequences', 'FILE_FORMAT': 'file_formats', 'ALERT': 'alerts',
            'STAGE': 'stages', 'TAG': 'tags',
        }
        return mapping.get(kind.upper(), 'other')

    def upload_file(schema, obj_type_folder, file_name, ddl_text):
        full_stage_path = f"{stage_root}/{db_name}/{schema}/{obj_type_folder}/{file_name}"
        input_stream = io.BytesIO(ddl_text.encode('utf-8'))
        session.file.put_stream(input_stream, full_stage_path, auto_compress=False, overwrite=True)
        return full_stage_path

    def fqn_expand(text, source_schema):
        # Pass 1: expand SCHEMA.OBJECT references (any schema in this database)
        # to the fully qualified "DB"."SCHEMA"."OBJECT" form. Skip if already
        # preceded by another qualifier (e.g. "DB"."SCHEMA".NAME).
        for target_obj in object_map:
            t_schema = target_obj['schema']
            t_name = target_obj['name']
            t_fqn = target_obj['fqn']
            pattern = r'(?i)(?<!\.|")\b{}\.{}\b'.format(
                re.escape(t_schema), re.escape(t_name)
            )
            text = re.sub(pattern, t_fqn, text)
        # Pass 2: expand bare OBJECT references in the source schema.
        for target_obj in object_map:
            if target_obj['schema'] != source_schema:
                continue
            t_name = target_obj['name']
            t_fqn = target_obj['fqn']
            pattern = r'(?i)(?<!\.|")\b{}\b'.format(re.escape(t_name))
            text = re.sub(pattern, t_fqn, text)
        return text

    # 2c. Generate DEFINE SCHEMA statements (all schemas in one file under db folder)
    schema_ddl_parts = []
    if type_allowed('SCHEMA'):
        for s_name in sorted(schemas_to_scan):
            fqn = qfqn(s_name)
            parts = [f"DEFINE SCHEMA {fqn}"]
            if schema_comments.get(s_name):
                escaped = schema_comments[s_name].replace("'", "''")
                parts.append(f"    COMMENT = '{escaped}'")
            schema_ddl_parts.append((s_name, "\n".join(parts) + ";"))

    if schema_ddl_parts:
        combined = "\n\n".join(ddl for _, ddl in schema_ddl_parts)
        full_stage_path = f"{stage_root}/{db_name}/schemas.sql"
        input_stream = io.BytesIO(combined.encode('utf-8'))
        session.file.put_stream(input_stream, full_stage_path, auto_compress=False, overwrite=True)
        for s_name, _ in schema_ddl_parts:
            generated_files.append((s_name, "SCHEMA", s_name, "SAVED", full_stage_path))

    # 3. Generate DDL and Stream to Stage for tables/views/dynamic tables
    for obj in object_map:
        short_name = obj['name']
        schema = obj['schema']
        fqn = obj['fqn']
        kind = obj['kind']

        if allowed_schemas is not None and (schema not in allowed_schemas):
            continue
        if kind in ('TASK', 'FUNCTION', 'PROCEDURE', 'SEQUENCE', 'FILE_FORMAT', 'ALERT', 'STAGE', 'TAG'):
            continue

        try:
            res = session.sql(f"SELECT GET_DDL('TABLE', '{fqn}', TRUE) as DDL").collect()
            ddl_text = res[0]['DDL']
        except Exception:
            try:
                res = session.sql(f"SELECT GET_DDL('VIEW', '{fqn}', TRUE) as DDL").collect()
                ddl_text = res[0]['DDL']
            except Exception as e:
                generated_files.append((schema, kind, short_name, "ERROR", str(e)))
                continue

        # Detect actual kind from DDL (SHOW OBJECTS reports dynamic tables as TABLE)
        if re.match(r'\s*create\s+or\s+replace\s+DYNAMIC\s+TABLE', ddl_text, re.IGNORECASE):
            kind = 'DYNAMIC TABLE'

        # Re-check against object_types now that we know the actual
        # kind. A table detected as DYNAMIC TABLE must be dropped here if the
        # caller asked only for TABLE (and vice versa).
        if not type_allowed(kind):
            continue
        ddl_text = re.sub(r'^\s*CREATE\s+OR\s+REPLACE\s+', 'DEFINE ', ddl_text, flags=re.IGNORECASE)
        ddl_text = re.sub(r'^\s*CREATE\s+', 'DEFINE ', ddl_text, flags=re.IGNORECASE)
        ddl_text = fqn_expand(ddl_text, schema)

        collect_grants(f"SHOW GRANTS ON TABLE {fqn}", f"\n-- {kind} {fqn}", schema)

        folder = kind_to_folder(kind)
        if group_files_by_type:
            key = (schema, folder)
            grouped_ddl.setdefault(key, []).append(ddl_text)
            generated_files.append((schema, kind, short_name, "SAVED", key))
        else:
            file_name = f"{short_name}.sql"
            path = upload_file(schema, folder, file_name, ddl_text)
            generated_files.append((schema, kind, short_name, "SAVED", path))

    # 3b. Generate DEFINE TASK statements
    for task in task_list:
        short_name = task['name']
        schema = task['schema']
        fqn = task['fqn']

        task_def = fqn_expand(task['definition'], schema)

        parts = [f"DEFINE TASK {fqn}"]
        if task['warehouse']:
            parts.append(f"    WAREHOUSE = {task['warehouse']}")
        if task['schedule']:
            parts.append(f"    SCHEDULE = '{task['schedule']}'")
        if task.get('comment'):
            escaped = task['comment'].replace("'", "''")
            parts.append(f"    COMMENT = '{escaped}'")
        parts.append(f"    AS {task_def};")
        ddl_text = "\n".join(parts)

        collect_grants(f"SHOW GRANTS ON TASK {fqn}", f"\n-- TASK {fqn}", schema)

        if group_files_by_type:
            key = (schema, 'tasks')
            grouped_ddl.setdefault(key, []).append(ddl_text)
            generated_files.append((schema, "TASK", short_name, "SAVED", key))
        else:
            file_name = f"{short_name}.sql"
            path = upload_file(schema, 'tasks', file_name, ddl_text)
            generated_files.append((schema, "TASK", short_name, "SAVED", path))

    # 3c. Generate DEFINE statements for functions and procedures via GET_DDL
    for c in callable_list:
        short_name = c['name']
        schema = c['schema']
        fqn = c['fqn']
        domain = c['domain']
        arguments = c['arguments']

        sig_for_ddl = fqn
        if arguments:
            # Extract the balanced-paren argument list, handling nested parens
            # like TABLE(NUMBER, NUMBER). The `arguments` column looks like:
            #   "NAME(TABLE(NUMBER, NUMBER)) RETURN TABLE(...)"
            start = arguments.find('(')
            if start != -1:
                depth = 0
                end = -1
                for i in range(start, len(arguments)):
                    ch = arguments[i]
                    if ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
                        if depth == 0:
                            end = i
                            break
                if end != -1:
                    sig_for_ddl = f"{fqn}{arguments[start:end + 1]}"
                else:
                    sig_for_ddl = f"{fqn}()"
            else:
                sig_for_ddl = f"{fqn}()"

        try:
            res = session.sql(f"SELECT GET_DDL('{domain}', '{sig_for_ddl}', TRUE) as DDL").collect()
            ddl_text = res[0]['DDL']
        except Exception as e:
            generated_files.append((schema, domain, short_name, "ERROR", str(e)))
            continue

        ddl_text = re.sub(r'^\s*CREATE\s+OR\s+REPLACE\s+', 'DEFINE ', ddl_text, flags=re.IGNORECASE)
        ddl_text = re.sub(r'^\s*CREATE\s+', 'DEFINE ', ddl_text, flags=re.IGNORECASE)

        # Replace the quoted short name in the DEFINE header with the fully
        # qualified (already-quoted) FQN. GET_DDL returns the object name as
        # "NAME" at the top of the DDL; swap it for the quoted FQN.
        ddl_text = ddl_text.replace(f'"{short_name}"', fqn, 1)
        # Do NOT apply fqn_expand to the full body — it would corrupt column
        # aliases and parameter names that happen to match object names.

        collect_grants(f"SHOW GRANTS ON {domain} {sig_for_ddl}", f"\n-- {domain} {fqn}", schema)

        folder = kind_to_folder(domain)
        if group_files_by_type:
            key = (schema, folder)
            grouped_ddl.setdefault(key, []).append(ddl_text)
            generated_files.append((schema, domain, short_name, "SAVED", key))
        else:
            file_name = f"{short_name}.sql"
            path = upload_file(schema, folder, file_name, ddl_text)
            generated_files.append((schema, domain, short_name, "SAVED", path))

    # 3d. Generate DEFINE statements for sequences, file formats, and alerts via GET_DDL
    for obj in simple_ddl_list:
        short_name = obj['name']
        schema = obj['schema']
        fqn = obj['fqn']
        domain = obj['domain']

        try:
            res = session.sql(f"SELECT GET_DDL('{domain}', '{fqn}', TRUE) as DDL").collect()
            ddl_text = res[0]['DDL']
        except Exception as e:
            generated_files.append((schema, domain, short_name, "ERROR", str(e)))
            continue

        ddl_text = re.sub(r'^\s*CREATE\s+OR\s+REPLACE\s+', 'DEFINE ', ddl_text, flags=re.IGNORECASE)
        ddl_text = re.sub(r'^\s*CREATE\s+', 'DEFINE ', ddl_text, flags=re.IGNORECASE)
        ddl_text = fqn_expand(ddl_text, schema)

        show_type = domain.replace('_', ' ')
        collect_grants(f"SHOW GRANTS ON {show_type} {fqn}", f"\n-- {show_type} {fqn}", schema)

        folder = kind_to_folder(domain)
        if group_files_by_type:
            key = (schema, folder)
            grouped_ddl.setdefault(key, []).append(ddl_text)
            generated_files.append((schema, domain, short_name, "SAVED", key))
        else:
            file_name = f"{short_name}.sql"
            path = upload_file(schema, folder, file_name, ddl_text)
            generated_files.append((schema, domain, short_name, "SAVED", path))

    # 3e. Generate DEFINE STAGE statements from SHOW STAGES metadata
    for stg in stage_list:
        short_name = stg['name']
        schema = stg['schema']
        fqn = stg['fqn']
        parts = [f"DEFINE STAGE {fqn}"]
        if stg['directory_enabled'] == 'Y':
            parts.append("    DIRECTORY = ( ENABLE = TRUE )")
        if stg['comment']:
            escaped = stg['comment'].replace("'", "''")
            parts.append(f"    COMMENT = '{escaped}'")
        ddl_text = "\n".join(parts) + ";"

        collect_grants(f"SHOW GRANTS ON STAGE {fqn}", f"\n-- STAGE {fqn}", schema)

        if group_files_by_type:
            key = (schema, 'stages')
            grouped_ddl.setdefault(key, []).append(ddl_text)
            generated_files.append((schema, "STAGE", short_name, "SAVED", key))
        else:
            file_name = f"{short_name}.sql"
            path = upload_file(schema, 'stages', file_name, ddl_text)
            generated_files.append((schema, "STAGE", short_name, "SAVED", path))

    # 3f. Upload grouped files and resolve paths
    if group_files_by_type and grouped_ddl:
        group_paths = {}
        for (schema, folder), ddl_list in grouped_ddl.items():
            combined = "\n\n".join(ddl_list)
            full_stage_path = f"{stage_root}/{db_name}/{schema}/{folder}.sql"
            input_stream = io.BytesIO(combined.encode('utf-8'))
            session.file.put_stream(input_stream, full_stage_path, auto_compress=False, overwrite=True)
            group_paths[(schema, folder)] = full_stage_path
        generated_files = [
            (schema, obj_type, obj_name, status, group_paths[key] if isinstance(key, tuple) else key)
            for schema, obj_type, obj_name, status, key in generated_files
        ]

    # 3g. Upload database-level grants.sql
    if db_grant_lines:
        db_grants_text = f"-- Database: {db_name}\n" + "\n".join(db_grant_lines)
        full_stage_path = f"{stage_root}/{db_name}/grants.sql"
        input_stream = io.BytesIO(db_grants_text.encode('utf-8'))
        session.file.put_stream(input_stream, full_stage_path, auto_compress=False, overwrite=True)
        generated_files.append((db_name, "GRANTS", "GRANTS", "SAVED", full_stage_path))

    # 3h. Upload grants.sql files per schema
    for s_name, grant_lines in grants_by_schema.items():
        if not grant_lines:
            continue
        grants_text = "\n".join(grant_lines)
        full_stage_path = f"{stage_root}/{db_name}/{s_name}/grants.sql"
        input_stream = io.BytesIO(grants_text.encode('utf-8'))
        session.file.put_stream(input_stream, full_stage_path, auto_compress=False, overwrite=True)
        generated_files.append((s_name, "GRANTS", "GRANTS", "SAVED", full_stage_path))

    # 3i. Aggregate grant-collection failures into one WARNING row per schema
    for schema, count in grant_failure_counts.items():
        generated_files.append((schema, "GRANTS", "GRANTS", "WARNING", f"{count} SHOW GRANTS call(s) failed in this schema"))

    # 4. Return Result
    if generated_files:
        # 4a. Cosmetic: display FILE_FORMAT as "FILE FORMAT" in the OBJECT_TYPE
        #              column to match other multi-word kinds like DYNAMIC TABLE.
        generated_files = [
            (r[0], "FILE FORMAT" if r[1] == "FILE_FORMAT" else r[1], r[2], r[3], r[4])
            for r in generated_files
        ]

        # 4b. Sort: ERROR first, then WARNING, then UNSUPPORTED, then SAVED.
        #          Within each status group, sort by schema then object name.
        status_order = {"ERROR": 0, "WARNING": 1, "UNSUPPORTED": 2, "SAVED": 3}
        sorted_files = sorted(
            generated_files,
            key=lambda r: (status_order.get(r[3], 99), r[0] or "", r[2] or "")
        )

        # 4c. Build summary rows: one row per distinct status with counts.
        status_counts = {}
        for r in sorted_files:
            status_counts[r[3]] = status_counts.get(r[3], 0) + 1
        total = sum(status_counts.values())

        summary_rows = [("", "SUMMARY", "TOTAL", str(total), f"{total} objects processed")]
        for status in ("ERROR", "WARNING", "UNSUPPORTED", "SAVED"):
            if status in status_counts:
                summary_rows.append(("", "SUMMARY", status, str(status_counts[status]), ""))
        for status, count in status_counts.items():
            if status not in ("ERROR", "WARNING", "UNSUPPORTED", "SAVED"):
                summary_rows.append(("", "SUMMARY", status, str(count), ""))

        return session.create_dataframe(
            summary_rows + sorted_files,
            schema=["SCHEMA", "OBJECT_TYPE", "OBJECT_NAME", "STATUS", "FILE_PATH"]
        )
    else:
        return session.create_dataframe(
            [("", "", "", "NONE", "No files generated")],
            schema=["SCHEMA", "OBJECT_TYPE", "OBJECT_NAME", "STATUS", "FILE_PATH"]
        )
$$;