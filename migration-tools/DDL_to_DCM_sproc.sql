-- CALL DDL_TO_DCM_DEFINITIONS(
--     'DCM_DEMO',                 -- Database Name
--     NULL, --['RAW', 'SERVE'],   -- option to only process listed Schemas
--     'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live/DCM_Migration',    -- target path to workspace or stage folder 
--     TRUE                        -- write multiple definitions in one file (for better performance at scale)
-- );


CREATE OR REPLACE PROCEDURE DDL_TO_DCM_DEFINITIONS(
    db_name STRING,
    schema_allow_list ARRAY,
    output_path STRING,
    group_by_type BOOLEAN DEFAULT FALSE
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

# Spans where FQN qualification must not happen: string literals, quoted
# identifiers, and comments. These are masked out before the bare-name
# replace and restored afterward, so only actual code is rewritten.
_PROTECTED_RE = re.compile(
    r"/\*.*?\*/"             # block comment
    r"|--[^\n]*"             # line comment
    r"|'(?:[^']|'')*'"       # single-quoted string literal
    r"|\"(?:[^\"]|\"\")*\""  # double-quoted identifier
    r"|\$\$.*?\$\$",         # dollar-quoted string
    re.DOTALL,
)

# Supported object types and how each is handled. 'show' is the discovery
# command for callable/simple types. Order is significant: it sets the order
# of definitions within grouped (group_by_type) files.
OBJECT_TYPES = {
    'TABLE':         {'folder': 'tables',         'category': 'tableview'},
    'VIEW':          {'folder': 'views',          'category': 'tableview'},
    'DYNAMIC TABLE': {'folder': 'dynamic_tables', 'category': 'tableview'},
    'TASK':          {'folder': 'tasks',          'category': 'task'},
    'FUNCTION':      {'folder': 'functions',      'category': 'callable', 'show': 'SHOW USER FUNCTIONS'},
    'PROCEDURE':     {'folder': 'procedures',     'category': 'callable', 'show': 'SHOW USER PROCEDURES'},
    'SEQUENCE':      {'folder': 'sequences',      'category': 'simple',   'show': 'SHOW SEQUENCES'},
    'FILE_FORMAT':   {'folder': 'file_formats',   'category': 'simple',   'show': 'SHOW FILE FORMATS'},
    'ALERT':         {'folder': 'alerts',         'category': 'simple',   'show': 'SHOW ALERTS'},
    'TAG':           {'folder': 'tags',           'category': 'simple',   'show': 'SHOW TAGS'},
    'STAGE':         {'folder': 'stages',         'category': 'stage'},
}

# Types handled by dedicated loops below (all except the table/view family).
NON_TABLEVIEW_KINDS = {k for k, v in OBJECT_TYPES.items() if v['category'] != 'tableview'}

# Types discovered via their 'show' command.
CALLABLE_TYPES = {k: v for k, v in OBJECT_TYPES.items() if v['category'] == 'callable'}
SIMPLE_DDL_TYPES = {k: v for k, v in OBJECT_TYPES.items() if v['category'] == 'simple'}


def main(session, db_name, schema_allow_list, output_path, group_by_type):
    # 1. Normalize Inputs
    allowed_schemas = None
    if schema_allow_list is not None:
        allowed_schemas = set([s.upper() for s in schema_allow_list])

    stage_root = output_path.rstrip('/')

    # 2. Build Inventory (Scan ALL schemas)
    try:
        objects_df = session.sql(f"SHOW OBJECTS IN DATABASE {db_name}").collect()
    except Exception as e:
        return session.create_dataframe(
            [(db_name, "DATABASE", db_name, "ERROR", f"Cannot access database '{db_name}': {e}")],
            schema=["SCHEMA", "OBJECT_TYPE", "OBJECT_NAME", "STATUS", "FILE_PATH"]
        )

    object_map = []
    generated_files = []
    schema_comments = {}  # schema_name -> comment

    # Semantic views are unsupported and report as kind='VIEW' in SHOW OBJECTS,
    # so collect their FQNs here to exclude them below.
    semantic_view_fqns = set()
    try:
        sv_df = session.sql(f"SHOW SEMANTIC VIEWS IN DATABASE {db_name}").collect()
        for sv_row in sv_df:
            sv_fqn = f"{db_name}.{sv_row['schema_name'].upper()}.{sv_row['name']}"
            semantic_view_fqns.add(sv_fqn)
    except Exception as e:
        generated_files.append((db_name, "WARNING", "SEMANTIC_VIEW_LOOKUP", "ERROR", str(e)))

    for row in objects_df:
        s_name = row['schema_name'].upper()
        kind = row['kind']
        fqn_check = f"{db_name}.{s_name}.{row['name']}"
        # Skip streams silently (not yet handled by this migration)
        if kind.upper() == 'STREAM':
            continue
        # Stages are handled via SHOW STAGES in the per-schema loop
        if kind.upper() == 'STAGE':
            continue
        # Exclude semantic views (matched by FQN or kind).
        if fqn_check in semantic_view_fqns or 'SEMANTIC' in kind.upper():
            generated_files.append((s_name, kind, row['name'], "UNSUPPORTED", "semantic views"))
            continue
        if s_name != 'INFORMATION_SCHEMA':
            fqn = f"{db_name}.{s_name}.{row['name']}"
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
        schemas_df = session.sql(f"SHOW SCHEMAS IN DATABASE {db_name}").collect()
        for row in schemas_df:
            s_name = row['name'].upper()
            if s_name == 'INFORMATION_SCHEMA':
                continue
            schema_comments[s_name] = row['comment'] or ''
            if not allowed_schemas:
                schemas_to_scan.add(s_name)
    except Exception as e:
        generated_files.append((db_name, "WARNING", "SCHEMA_LOOKUP", "ERROR", str(e)))

    task_list = []
    callable_list = []  # functions and procedures
    simple_ddl_list = []  # sequences, file formats, alerts
    stage_list = []  # permanent internal stages
    grants_by_schema = {}  # schema -> [grant_stmt_strings]

    def format_grant_stmt(row):
        priv = row['privilege']
        if priv == 'OWNERSHIP':
            return ('OWNERSHIP', row['granted_on'], row['name'])
        granted_to = row['granted_to']
        target_keyword = {
            'ROLE': 'ROLE',
            'DATABASE_ROLE': 'DATABASE ROLE',
            'USER': 'USER',
            'SHARE': 'SHARE',
        }.get(granted_to)
        if target_keyword is None:
            return None
        grantee = row['grantee_name']
        granted_on = row['granted_on']
        obj_name = row['name']
        grant_opt = row['grant_option']
        stmt = f"GRANT {priv} ON {granted_on} {obj_name} TO {target_keyword} {grantee}"
        if str(grant_opt).upper() == 'TRUE':
            stmt += " WITH GRANT OPTION"
        return stmt + ";"

    def collect_grants(show_cmd, header, schema):
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
            pass

    # 2b-pre. Collect database-level grants
    db_grant_lines = []
    try:
        db_grant_rows = session.sql(f"SHOW GRANTS ON DATABASE {db_name}").collect()
        for gr in db_grant_rows:
            result = format_grant_stmt(gr)
            if isinstance(result, tuple):
                generated_files.append((db_name, "GRANT", result[2], "UNSUPPORTED", f"{result[0]} grant"))
            elif result:
                db_grant_lines.append(result)
    except Exception as e:
        generated_files.append((db_name, "GRANTS", "DATABASE", "ERROR", str(e)))
    try:
        db_future_rows = session.sql(f"SHOW FUTURE GRANTS IN DATABASE {db_name}").collect()
        for fr in db_future_rows:
            obj_type = fr['grant_on']
            grantee = fr['grantee_name']
            priv = fr['privilege']
            generated_files.append((db_name, "GRANT", f"FUTURE {obj_type} -> {grantee}", "UNSUPPORTED", f"future grant ({priv})"))
    except Exception:
        pass

    for s_name in schemas_to_scan:
        collect_grants(
            f"SHOW GRANTS ON SCHEMA {db_name}.{s_name}",
            f"-- Schema: {db_name}.{s_name}",
            s_name
        )
        try:
            future_rows = session.sql(f"SHOW FUTURE GRANTS IN SCHEMA {db_name}.{s_name}").collect()
            for fr in future_rows:
                obj_type = fr['grant_on']
                grantee = fr['grantee_name']
                priv = fr['privilege']
                generated_files.append((s_name, "GRANT", f"FUTURE {obj_type} -> {grantee}", "UNSUPPORTED", f"future grant ({priv})"))
        except Exception:
            pass

        try:
            tasks_df = session.sql(f"SHOW TASKS IN SCHEMA {db_name}.{s_name}").collect()
            for row in tasks_df:
                task_name = row['name']
                fqn = f"{db_name}.{s_name}.{task_name}"
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

        for ddl_domain, type_spec in CALLABLE_TYPES.items():
            show_cmd = f"{type_spec['show']} IN SCHEMA {db_name}.{s_name}"
            try:
                rows = session.sql(show_cmd).collect()
                for row in rows:
                    row_dict = row.as_dict()
                    obj_name = row_dict['name']
                    # Skip the GENERATE_DEFINITIONS procedure itself
                    if obj_name.upper() == 'GENERATE_DEFINITIONS':
                        continue
                    arguments = row_dict.get('arguments', '')
                    fqn = f"{db_name}.{s_name}.{obj_name}"
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

        for ddl_domain, type_spec in SIMPLE_DDL_TYPES.items():
            show_cmd = f"{type_spec['show']} IN SCHEMA {db_name}.{s_name}"
            try:
                rows = session.sql(show_cmd).collect()
                for row in rows:
                    obj_name = row['name']
                    fqn = f"{db_name}.{s_name}.{obj_name}"
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
        try:
            stages_df = session.sql(f"SHOW STAGES IN SCHEMA {db_name}.{s_name}").collect()
            for row in stages_df:
                stage_name = row['name']
                url = row['url'] or ''
                stage_type = (row['type'] or '').upper()
                fqn = f"{db_name}.{s_name}.{stage_name}"
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

    # Longest names first so fqn_expand replaces them before shorter substrings.
    object_map.sort(key=lambda x: len(x["name"]), reverse=True)

    grouped_ddl = {}  # (schema, type_folder) -> [ddl_text, ...]

    # Helper: map object kind to a folder name (see OBJECT_TYPES).
    def kind_to_folder(kind):
        type_spec = OBJECT_TYPES.get(kind.upper())
        return type_spec['folder'] if type_spec else 'other'

    def upload_file(schema, obj_type_folder, file_name, ddl_text):
        full_stage_path = f"{stage_root}/{db_name}/{schema}/{obj_type_folder}/{file_name}"
        input_stream = io.BytesIO(ddl_text.encode('utf-8'))
        session.file.put_stream(input_stream, full_stage_path, auto_compress=False, overwrite=True)
        return full_stage_path

    def fqn_expand(text, source_schema, replaced=None):
        # Mask string literals, quoted identifiers, and comments so the
        # bare-name replace below only touches actual code, then restore them.
        # If 'replaced' is a set, every object name actually substituted is
        # added to it (uppercased) so callers can detect column-name collisions.
        masked = []
        def _mask(m):
            masked.append(m.group(0))
            return f"\x00{len(masked) - 1}\x00"
        text = _PROTECTED_RE.sub(_mask, text)

        for target_obj in object_map:
            if target_obj['schema'] != source_schema:
                continue
            t_name = target_obj['name']
            t_fqn = target_obj['fqn']
            pattern = r'(?i)(?<!\.|")\b{}\b'.format(re.escape(t_name))
            text, n = re.subn(pattern, t_fqn, text)
            if n and replaced is not None:
                replaced.add(t_name.upper())

        if masked:
            text = re.sub(r'\x00(\d+)\x00', lambda m: masked[int(m.group(1))], text)
        return text

    # 2c. Generate DEFINE SCHEMA statements (all schemas in one file under db folder)
    schema_ddl_parts = []
    for s_name in sorted(schemas_to_scan):
        fqn = f"{db_name}.{s_name}"
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

    # Build a map of each object's own column names so we can flag when
    # fqn_expand qualifies a bare token that is actually a column of the
    # object being defined (a same-schema name collision that breaks the
    # DDL). Best-effort: skip the check if the lookup fails.
    columns_by_object = {}
    try:
        col_rows = session.sql(
            f"SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME "
            f"FROM {db_name}.INFORMATION_SCHEMA.COLUMNS"
        ).collect()
        for cr in col_rows:
            key = (cr['TABLE_SCHEMA'].upper(), cr['TABLE_NAME'].upper())
            columns_by_object.setdefault(key, set()).add(cr['COLUMN_NAME'].upper())
    except Exception as e:
        generated_files.append((db_name, "WARNING", "COLUMN_LOOKUP", "ERROR", str(e)))

    # 3. Generate DDL and Stream to Stage for tables/views/dynamic tables
    for obj in object_map:
        short_name = obj['name']
        schema = obj['schema']
        fqn = obj['fqn']
        kind = obj['kind']

        if allowed_schemas is not None and (schema not in allowed_schemas):
            continue
        if kind in NON_TABLEVIEW_KINDS:
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

        ddl_text = re.sub(r'^\s*CREATE\s+OR\s+REPLACE\s+', 'DEFINE ', ddl_text, flags=re.IGNORECASE)
        ddl_text = re.sub(r'^\s*CREATE\s+', 'DEFINE ', ddl_text, flags=re.IGNORECASE)
        replaced_names = set()
        ddl_text = fqn_expand(ddl_text, schema, replaced_names)

        # Flag names that were qualified but are also columns of this object:
        # likely a false positive from the word-boundary replace.
        collisions = sorted(replaced_names & columns_by_object.get((schema.upper(), short_name.upper()), set()))
        if collisions:
            generated_files.append((schema, kind, short_name, "WARNING", f"qualified name(s) also column(s) of this object, verify in PLAN: {', '.join(collisions)}"))

        collect_grants(f"SHOW GRANTS ON TABLE {fqn}", f"\n-- {kind} {fqn}", schema)

        folder = kind_to_folder(kind)
        if group_by_type:
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

        folder = kind_to_folder('TASK')
        if group_by_type:
            key = (schema, folder)
            grouped_ddl.setdefault(key, []).append(ddl_text)
            generated_files.append((schema, "TASK", short_name, "SAVED", key))
        else:
            file_name = f"{short_name}.sql"
            path = upload_file(schema, folder, file_name, ddl_text)
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
            # Extract the argument signature, allowing nested parens like
            # TABLE(NUMBER, NUMBER).
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
            res = session.sql(f"SELECT GET_DDL('{domain}', '{sig_for_ddl}') as DDL").collect()
            ddl_text = res[0]['DDL']
        except Exception as e:
            generated_files.append((schema, domain, short_name, "ERROR", str(e)))
            continue

        ddl_text = re.sub(r'^\s*CREATE\s+OR\s+REPLACE\s+', 'DEFINE ', ddl_text, flags=re.IGNORECASE)
        ddl_text = re.sub(r'^\s*CREATE\s+', 'DEFINE ', ddl_text, flags=re.IGNORECASE)

        # Rewrite the DEFINE header's quoted name to a quoted FQN so special
        # characters and reserved words in identifiers are preserved.
        quoted_fqn = '.'.join(f'"{part}"' for part in fqn.split('.'))
        ddl_text = ddl_text.replace(f'"{short_name}"', quoted_fqn, 1)
        # Do not fqn_expand the body: it would corrupt column aliases and
        # parameter names that match object names.

        collect_grants(f"SHOW GRANTS ON {domain} {sig_for_ddl}", f"\n-- {domain} {fqn}", schema)

        folder = kind_to_folder(domain)
        if group_by_type:
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
        if group_by_type:
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

        folder = kind_to_folder('STAGE')
        if group_by_type:
            key = (schema, folder)
            grouped_ddl.setdefault(key, []).append(ddl_text)
            generated_files.append((schema, "STAGE", short_name, "SAVED", key))
        else:
            file_name = f"{short_name}.sql"
            path = upload_file(schema, folder, file_name, ddl_text)
            generated_files.append((schema, "STAGE", short_name, "SAVED", path))

    # 3f. Upload grouped files and resolve paths
    if group_by_type and grouped_ddl:
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

    # 4. Return Result
    if generated_files:
        # 4a. Display FILE_FORMAT as "FILE FORMAT" to match other multi-word kinds.
        generated_files = [
            (r[0], "FILE FORMAT" if r[1] == "FILE_FORMAT" else r[1], r[2], r[3], r[4])
            for r in generated_files
        ]

        # 4b. Sort: ERROR first, then UNSUPPORTED, then SAVED, then everything else.
        #          Within each status group, sort by schema then object name.
        status_order = {"ERROR": 0, "UNSUPPORTED": 1, "SAVED": 2}
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
        for status in ("ERROR", "UNSUPPORTED", "SAVED"):
            if status in status_counts:
                summary_rows.append(("", "SUMMARY", status, str(status_counts[status]), ""))
        for status, count in status_counts.items():
            if status not in ("ERROR", "UNSUPPORTED", "SAVED"):
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