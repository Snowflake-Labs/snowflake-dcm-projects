-- CALL DDL_TO_DCM_DEFINITIONS(
--     'DCM_DEMO',                 -- Database Name
--     NULL, --['RAW', 'SERVE'],   -- option to only process listed Schemas
--     'snow://workspace/USER$.PUBLIC.DEFAULT/versions/live/DCM_Migration',    -- target path to workspace or stage folder 
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
    for row in objects_df:
        s_name = row['schema_name'].upper()
        if s_name != 'INFORMATION_SCHEMA':
            fqn = f"{db_name}.{s_name}.{row['name']}"
            object_map.append({
                "name": row['name'],
                "fqn": fqn,
                "schema": s_name,
                "kind": row['kind']
            })

    # 2b. Scan tasks, functions, and procedures per schema
    #     (SHOW OBJECTS does not include these object types)
    schemas_to_scan = allowed_schemas if allowed_schemas else set()
    if not schemas_to_scan:
        schemas_df = session.sql(f"SHOW SCHEMAS IN DATABASE {db_name}").collect()
        for row in schemas_df:
            s_name = row['name'].upper()
            if s_name != 'INFORMATION_SCHEMA':
                schemas_to_scan.add(s_name)

    task_list = []
    callable_list = []  # functions and procedures
    generated_files = []
    for s_name in schemas_to_scan:
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
                })
                object_map.append({
                    "name": task_name,
                    "fqn": fqn,
                    "schema": s_name,
                    "kind": "TASK"
                })
        except Exception as e:
            generated_files.append((s_name, "TASK", "*", "SKIPPED", str(e)))

        for show_cmd, ddl_domain in [
            (f"SHOW USER FUNCTIONS IN SCHEMA {db_name}.{s_name}", "FUNCTION"),
            (f"SHOW USER PROCEDURES IN SCHEMA {db_name}.{s_name}", "PROCEDURE"),
        ]:
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
                generated_files.append((s_name, ddl_domain, "*", "SKIPPED", str(e)))

    # Sort by length (descending)
    object_map.sort(key=lambda x: len(x["name"]), reverse=True)

    grouped_ddl = {}  # (schema, type_folder) -> [ddl_text, ...]

    # Helper: map object kind to a folder name
    def kind_to_folder(kind):
        mapping = {
            'TABLE': 'tables', 'VIEW': 'views', 'DYNAMIC TABLE': 'dynamic_tables',
            'TASK': 'tasks', 'FUNCTION': 'functions', 'PROCEDURE': 'procedures',
        }
        return mapping.get(kind.upper(), 'other')

    def upload_file(schema, obj_type_folder, file_name, ddl_text):
        full_stage_path = f"{stage_root}/{db_name}/{schema}/{obj_type_folder}/{file_name}"
        input_stream = io.BytesIO(ddl_text.encode('utf-8'))
        session.file.put_stream(input_stream, full_stage_path, auto_compress=False, overwrite=True)
        return full_stage_path

    def fqn_expand(text, source_schema):
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
    for s_name in sorted(schemas_to_scan):
        fqn = f"{db_name}.{s_name}"
        schema_ddl_parts.append((s_name, f"DEFINE SCHEMA {fqn};"))

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
        if kind in ('TASK', 'FUNCTION', 'PROCEDURE'):
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
        ddl_text = fqn_expand(ddl_text, schema)

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
        parts.append(f"    AS {task_def};")
        ddl_text = "\n".join(parts)

        if group_by_type:
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
            paren_match = re.search(r'\(([^)]*)\)', arguments)
            if paren_match:
                sig_for_ddl = f"{fqn}({paren_match.group(1)})"
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

        # Replace the quoted name in the DEFINE header with a properly quoted FQN.
        # GET_DDL returns quoted names like "NAME" — replace with quoted FQN
        # to handle special characters and reserved words in identifiers.
        quoted_fqn = '.'.join(f'"{part}"' for part in fqn.split('.'))
        ddl_text = ddl_text.replace(f'"{short_name}"', quoted_fqn, 1)
        # Do NOT apply fqn_expand to the full body — it would corrupt column
        # aliases and parameter names that happen to match object names.

        folder = kind_to_folder(domain)
        if group_by_type:
            key = (schema, folder)
            grouped_ddl.setdefault(key, []).append(ddl_text)
            generated_files.append((schema, domain, short_name, "SAVED", key))
        else:
            file_name = f"{short_name}.sql"
            path = upload_file(schema, folder, file_name, ddl_text)
            generated_files.append((schema, domain, short_name, "SAVED", path))

    # 3d. Upload grouped files and resolve paths
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

    # 4. Return Result
    if generated_files:
        return session.create_dataframe(
            generated_files,
            schema=["SCHEMA", "OBJECT_TYPE", "OBJECT_NAME", "STATUS", "FILE_PATH"]
        )
    else:
        return session.create_dataframe(
            [("", "", "", "NONE", "No files generated")],
            schema=["SCHEMA", "OBJECT_TYPE", "OBJECT_NAME", "STATUS", "FILE_PATH"]
        )
$$;