#!/usr/bin/env python3
"""
ddl_to_dcm.py — Convert existing Snowflake object DDL to DCM DEFINE syntax.

Scans a database, retrieves DDL for all objects, converts CREATE to DEFINE,
expands references to fully qualified names, and writes definition files
to a local output directory.

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/ddl_to_dcm.py \
        --db-name <DB_NAME> \
        [--schema-list SCHEMA1 SCHEMA2 ...] \
        [--object-types TYPE1 TYPE2 ...] \
        [--group-files-by-type] \
        --output-path <OUTPUT_DIR> \
        [--connection <CONNECTION_NAME>]
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

from snowflake.snowpark import Session


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

# Canonical set of object types supported by this script. Values passed via
# --object-types are normalized (uppercased, spaces -> underscores) and must
# match one of these.
_CANONICAL_TYPES = {
    "TABLE", "VIEW", "DYNAMIC_TABLE", "TASK", "FUNCTION", "PROCEDURE",
    "SEQUENCE", "FILE_FORMAT", "ALERT", "TAG", "STAGE", "SCHEMA", "GRANT",
}


def normalize_type(t):
    return str(t).strip().upper().replace(" ", "_")


def qid(name):
    """Safe-quote a Snowflake identifier for use in dynamic SQL."""
    if name is None:
        raise ValueError("identifier is None")
    s = str(name).strip()
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        return s
    return '"' + s.replace('"', '""') + '"'


def validate_user_ident(name):
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


def get_session(connection_name):
    return Session.builder.config("connection_name", connection_name).create()


def kind_to_folder(kind):
    mapping = {
        "TABLE": "tables",
        "VIEW": "views",
        "DYNAMIC TABLE": "dynamic_tables",
        "TASK": "tasks",
        "FUNCTION": "functions",
        "PROCEDURE": "procedures",
        "SEQUENCE": "sequences",
        "FILE_FORMAT": "file_formats",
        "ALERT": "alerts",
        "STAGE": "stages",
        "TAG": "tags",
    }
    return mapping.get(kind.upper(), "other")


def normalize_define_keyword(ddl_text):
    return re.sub(
        r"^(DEFINE\s+)(dynamic\s+table|file\s+format|table|view|schema|task|function|procedure|sequence|alert)",
        lambda m: m.group(1) + m.group(2).upper(),
        ddl_text,
        count=1,
        flags=re.IGNORECASE,
    )


def escape_jinja_conflicts(ddl_text):
    # Detect `{{` or `}}` that are NOT part of a well-formed `{{ name }}` Jinja
    # variable reference. Uses a fixed-width-safe approach: find all `{{` and
    # `}}` occurrences, then check whether each is part of a `{{ word }}` pair.
    well_formed = set()
    for m in re.finditer(r"\{\{\s*\w+\s*\}\}", ddl_text):
        well_formed.add(m.start())           # position of opening {{
        well_formed.add(m.end() - 2)         # position of closing }}
    has_stray = False
    for m in re.finditer(r"\{\{|\}\}", ddl_text):
        if m.start() not in well_formed:
            has_stray = True
            break
    if has_stray:
        lines = ddl_text.split("\n")
        header = lines[0] if lines else ""
        body = "\n".join(lines[1:]) if len(lines) > 1 else ""
        if "{{" in body or "}}" in body:
            body = "{% raw %}\n" + body + "\n{% endraw %}"
            return header + "\n" + body
    return ddl_text


def fqn_expand(text, source_schema, object_map):
    # Pass 1: expand SCHEMA.OBJECT references (any schema in this database)
    # to the fully qualified "DB"."SCHEMA"."OBJECT" form. Skip if already
    # preceded by another qualifier (e.g. "DB"."SCHEMA".NAME).
    for target_obj in object_map:
        t_schema = target_obj["schema"]
        t_name = target_obj["name"]
        t_fqn = target_obj["fqn"]
        pattern = r'(?i)(?<!\.|")\b{}\.{}\b'.format(
            re.escape(t_schema), re.escape(t_name)
        )
        text = re.sub(pattern, t_fqn, text)
    # Pass 2: expand bare OBJECT references in the source schema.
    for target_obj in object_map:
        if target_obj["schema"] != source_schema:
            continue
        t_name = target_obj["name"]
        t_fqn = target_obj["fqn"]
        pattern = r'(?i)(?<!\.|")\b{}\b'.format(re.escape(t_name))
        text = re.sub(pattern, t_fqn, text)
    return text


def write_file(output_dir, db_name, schema, obj_type_folder, file_name, content):
    path = Path(output_dir) / db_name / schema / obj_type_folder / file_name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return str(path)


def main():
    parser = argparse.ArgumentParser(description="Convert Snowflake DDL to DCM DEFINE syntax")
    parser.add_argument("--db-name", required=True, help="Source database name")
    parser.add_argument("--schema-list", nargs="*", default=None, help="Optional schema allow-list")
    parser.add_argument(
        "--object-types",
        nargs="*",
        default=None,
        help=(
            "Optional object-type allow-list. Accepted values (case-insensitive, "
            "spaces or underscores): TABLE, VIEW, DYNAMIC TABLE, TASK, FUNCTION, "
            "PROCEDURE, SEQUENCE, FILE FORMAT, ALERT, TAG, STAGE, SCHEMA, GRANT. "
            "Omit for all supported types."
        ),
    )
    parser.add_argument("--group-files-by-type", action="store_true", help="Group definitions by object type per schema")
    parser.add_argument("--output-path", required=True, help="Local output directory for generated files")
    parser.add_argument("--connection", default=None, help="Snowflake connection name")
    parser.add_argument("--role", default=None, help="Only migrate objects owned by this role (filters by owner column)")
    args = parser.parse_args()

    try:
        db_name = validate_user_ident(args.db_name)
    except ValueError as e:
        print(json.dumps([{"schema": "", "object_type": "DATABASE", "object_name": str(args.db_name), "status": "ERROR", "file_path": f"Invalid database identifier: {e}"}], indent=2))
        sys.exit(1)
    q_db = qid(db_name)
    output_dir = args.output_path
    allowed_schemas = None
    invalid_schema_rows = []
    if args.schema_list:
        allowed_schemas = set()
        for s in args.schema_list:
            try:
                allowed_schemas.add(validate_user_ident(s))
            except ValueError as e:
                invalid_schema_rows.append({"schema": db_name, "object_type": "SCHEMA", "object_name": str(s), "status": "ERROR", "file_path": f"Invalid schema identifier: {e}"})
    group_files_by_type = args.group_files_by_type
    connection_name = args.connection or os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default_connection_name"
    role_filter = args.role.upper() if args.role else None

    # Normalize --object-types. None => all supported types. Otherwise every
    # value must map to a canonical type; any unknown value aborts the run.
    allowed_types = None
    if args.object_types is not None:
        allowed_types = set()
        invalid = []
        for t in args.object_types:
            norm = normalize_type(t)
            if norm in _CANONICAL_TYPES:
                allowed_types.add(norm)
            else:
                invalid.append(str(t))
        if invalid:
            supported = ", ".join(sorted(_CANONICAL_TYPES))
            print(json.dumps([{
                "schema": "",
                "object_type": "OBJECT_TYPE",
                "object_name": ", ".join(invalid),
                "status": "ERROR",
                "file_path": f"Unsupported object type(s): {', '.join(invalid)}. Supported: {supported}",
            }], indent=2))
            sys.exit(1)

    def type_allowed(t):
        if allowed_types is None:
            return True
        return normalize_type(t) in allowed_types

    session = get_session(connection_name)
    results = list(invalid_schema_rows)

    def qfqn(schema, obj=None):
        if obj is None:
            return f"{q_db}.{qid(schema)}"
        return f"{q_db}.{qid(schema)}.{qid(obj)}"

    try:
        objects_df = session.sql(f"SHOW OBJECTS IN DATABASE {q_db}").collect()
    except Exception as e:
        print(json.dumps([{"schema": db_name, "object_type": "DATABASE", "object_name": db_name, "status": "ERROR", "file_path": str(e)}], indent=2))
        sys.exit(1)

    total_object_count = 0
    matched_object_count = 0
    object_map = []

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
        results.append({"schema": db_name, "object_type": "SEMANTIC_VIEW_LOOKUP", "object_name": "SEMANTIC_VIEW_LOOKUP", "status": "WARNING", "file_path": str(e)})

    for row in objects_df:
        s_name = row["schema_name"].upper()
        fqn_check = qfqn(s_name, row['name'])
        kind = row["kind"]
        # Skip streams silently (not yet handled by this migration)
        if kind.upper() == "STREAM":
            continue
        # Stages are handled via SHOW STAGES in the per-schema loop
        if kind.upper() == "STAGE":
            continue
        # Skip semantic views: not supported by DCM at this point.
        if fqn_check in semantic_view_fqns or "SEMANTIC" in kind.upper():
            results.append({"schema": s_name, "object_type": kind, "object_name": row["name"], "status": "UNSUPPORTED", "file_path": "semantic views"})
            continue
        # Respect --object-types. SHOW OBJECTS reports dynamic tables as
        # kind='TABLE', so include tables when either TABLE or DYNAMIC_TABLE
        # is allowed; the actual kind is re-checked after GET_DDL.
        if allowed_types is not None:
            k_up = kind.upper()
            if k_up == "TABLE":
                if "TABLE" not in allowed_types and "DYNAMIC_TABLE" not in allowed_types:
                    continue
            elif normalize_type(k_up) not in allowed_types:
                continue
        if s_name != "INFORMATION_SCHEMA":
            total_object_count += 1
            if role_filter and row.get("owner", "").upper() != role_filter:
                continue
            matched_object_count += 1
            fqn = qfqn(s_name, row['name'])
            object_map.append({"name": row["name"], "fqn": fqn, "schema": s_name, "kind": kind})

    schemas_to_scan = set(allowed_schemas) if allowed_schemas else set()
    schema_comments = {}  # schema_name -> comment
    try:
        schemas_df = session.sql(f"SHOW SCHEMAS IN DATABASE {q_db}").collect()
        for row in schemas_df:
            s_name = row["name"].upper()
            if s_name == "INFORMATION_SCHEMA":
                continue
            if role_filter and row.as_dict().get("owner", "").upper() != role_filter:
                continue
            schema_comments[s_name] = row["comment"] or ""
            if not allowed_schemas:
                schemas_to_scan.add(s_name)
    except Exception as e:
        results.append({"schema": db_name, "object_type": "SCHEMA_LOOKUP", "object_name": "SCHEMA_LOOKUP", "status": "WARNING", "file_path": str(e)})

    task_list = []
    callable_list = []
    simple_ddl_list = []  # sequences, file formats, alerts
    stage_list = []  # permanent internal stages
    grants_by_schema = {}  # schema -> [grant_stmt_strings]
    grant_failure_counts = {}  # schema -> count of failed SHOW GRANTS calls
    db_grant_lines = []

    def format_grant_stmt(row):
        priv = row["privilege"]
        if priv == "OWNERSHIP":
            return ("OWNERSHIP", row["granted_on"], row["name"])
        granted_to = row["granted_to"]
        if granted_to not in ("ROLE", "DATABASE_ROLE"):
            return None
        grantee = row["grantee_name"]
        granted_on = row["granted_on"]
        obj_name = row["name"]
        grant_opt = row["grant_option"]
        target = f"DATABASE ROLE {grantee}" if granted_to == "DATABASE_ROLE" else f"ROLE {grantee}"
        stmt = f"GRANT {priv} ON {granted_on} {obj_name} TO {target}"
        if str(grant_opt).upper() == "TRUE":
            stmt += " WITH GRANT OPTION"
        return stmt + ";"

    def collect_grants(show_cmd, header, schema):
        if not type_allowed("GRANT"):
            return
        try:
            grant_rows = session.sql(show_cmd).collect()
            stmts = []
            for gr in grant_rows:
                result = format_grant_stmt(gr)
                if isinstance(result, tuple):
                    results.append({"schema": schema, "object_type": "GRANT", "object_name": result[2], "status": "UNSUPPORTED", "file_path": f"{result[0]} grant"})
                elif result:
                    stmts.append(result)
            if stmts:
                grants_by_schema.setdefault(schema, []).append(header)
                grants_by_schema[schema].extend(stmts)
        except Exception:
            grant_failure_counts[schema] = grant_failure_counts.get(schema, 0) + 1

    def collect_db_grants(show_cmd):
        if not type_allowed("GRANT"):
            return
        try:
            grant_rows = session.sql(show_cmd).collect()
            for gr in grant_rows:
                result = format_grant_stmt(gr)
                if isinstance(result, tuple):
                    results.append({"schema": db_name, "object_type": "GRANT", "object_name": result[2], "status": "UNSUPPORTED", "file_path": f"{result[0]} grant"})
                elif result:
                    db_grant_lines.append(result)
        except Exception as e:
            results.append({"schema": db_name, "object_type": "GRANTS", "object_name": "DATABASE", "status": "ERROR", "file_path": str(e)})

    def collect_future_grants(show_cmd, schema):
        if not type_allowed("GRANT"):
            return
        try:
            rows = session.sql(show_cmd).collect()
            for fr in rows:
                obj_type = fr["grant_on"]
                grantee = fr["grantee_name"]
                priv = fr["privilege"]
                results.append({"schema": schema, "object_type": "GRANT", "object_name": f"FUTURE {obj_type} -> {grantee}", "status": "UNSUPPORTED", "file_path": f"future grant ({priv})"})
        except Exception:
            pass

    # Build a map of non-SQL callables from INFORMATION_SCHEMA so we can skip
    # Python/Java/JavaScript/Scala functions and procedures at discovery time.
    non_sql_callables = {}  # (schema_upper, name_upper, domain) -> language
    try:
        rows = session.sql(
            f"SELECT PROCEDURE_SCHEMA, PROCEDURE_NAME, PROCEDURE_LANGUAGE "
            f"FROM {q_db}.INFORMATION_SCHEMA.PROCEDURES"
        ).collect()
        for r in rows:
            lang = (r["PROCEDURE_LANGUAGE"] or "").upper()
            if lang and lang != "SQL":
                non_sql_callables[(r["PROCEDURE_SCHEMA"].upper(), r["PROCEDURE_NAME"].upper(), "PROCEDURE")] = lang
    except Exception as e:
        results.append({"schema": db_name, "object_type": "PROCEDURE_LANGUAGE_LOOKUP", "object_name": "PROCEDURE_LANGUAGE_LOOKUP", "status": "WARNING", "file_path": str(e)})
    try:
        rows = session.sql(
            f"SELECT FUNCTION_SCHEMA, FUNCTION_NAME, FUNCTION_LANGUAGE "
            f"FROM {q_db}.INFORMATION_SCHEMA.FUNCTIONS"
        ).collect()
        for r in rows:
            lang = (r["FUNCTION_LANGUAGE"] or "").upper()
            if lang and lang != "SQL":
                non_sql_callables[(r["FUNCTION_SCHEMA"].upper(), r["FUNCTION_NAME"].upper(), "FUNCTION")] = lang
    except Exception as e:
        results.append({"schema": db_name, "object_type": "FUNCTION_LANGUAGE_LOOKUP", "object_name": "FUNCTION_LANGUAGE_LOOKUP", "status": "WARNING", "file_path": str(e)})

    collect_db_grants(f"SHOW GRANTS ON DATABASE {q_db}")
    collect_future_grants(f"SHOW FUTURE GRANTS IN DATABASE {q_db}", db_name)

    for s_name in schemas_to_scan:
        collect_grants(f"SHOW GRANTS ON SCHEMA {qfqn(s_name)}", f"-- Schema: {db_name}.{s_name}", s_name)
        collect_future_grants(f"SHOW FUTURE GRANTS IN SCHEMA {qfqn(s_name)}", s_name)

        if type_allowed("TASK"):
            try:
                tasks_df = session.sql(f"SHOW TASKS IN SCHEMA {qfqn(s_name)}").collect()
                for row in tasks_df:
                    if role_filter and row.get("owner", "").upper() != role_filter:
                        continue
                    task_name = row["name"]
                    fqn = qfqn(s_name, task_name)
                    task_list.append({
                        "name": task_name, "fqn": fqn, "schema": s_name,
                        "warehouse": row["warehouse"], "schedule": row["schedule"],
                        "definition": row["definition"],
                        "comment": row["comment"] or "",
                    })
                    object_map.append({"name": task_name, "fqn": fqn, "schema": s_name, "kind": "TASK"})
            except Exception as e:
                results.append({"schema": s_name, "object_type": "TASK", "object_name": "*", "status": "ERROR", "file_path": str(e)})

        callable_show_cmds = []
        if type_allowed("FUNCTION"):
            callable_show_cmds.append((f"SHOW USER FUNCTIONS IN SCHEMA {qfqn(s_name)}", "FUNCTION"))
        if type_allowed("PROCEDURE"):
            callable_show_cmds.append((f"SHOW USER PROCEDURES IN SCHEMA {qfqn(s_name)}", "PROCEDURE"))
        for show_cmd, ddl_domain in callable_show_cmds:
            try:
                rows = session.sql(show_cmd).collect()
                for row in rows:
                    row_dict = row.as_dict()
                    if role_filter and row_dict.get("owner", "").upper() != role_filter:
                        continue
                    obj_name = row_dict["name"]
                    # Skip non-SQL functions/procedures up-front.
                    non_sql_lang = non_sql_callables.get((s_name.upper(), obj_name.upper(), ddl_domain))
                    if non_sql_lang:
                        results.append({"schema": s_name, "object_type": ddl_domain, "object_name": obj_name, "status": "UNSUPPORTED", "file_path": f"language={non_sql_lang}"})
                        continue
                    # Skip Data Metric Functions: GET_DDL signature does not
                    # match what we generate for regular functions.
                    if ddl_domain == "FUNCTION" and row_dict.get("is_data_metric") == "Y":
                        results.append({"schema": s_name, "object_type": ddl_domain, "object_name": obj_name, "status": "UNSUPPORTED", "file_path": "data metric function"})
                        continue
                    arguments = row_dict.get("arguments", "")
                    fqn = qfqn(s_name, obj_name)
                    callable_list.append({
                        "name": obj_name, "fqn": fqn, "schema": s_name,
                        "domain": ddl_domain, "arguments": arguments,
                    })
                    object_map.append({"name": obj_name, "fqn": fqn, "schema": s_name, "kind": ddl_domain})
            except Exception as e:
                results.append({"schema": s_name, "object_type": ddl_domain, "object_name": "*", "status": "ERROR", "file_path": str(e)})

        # Stages: split by external / temporary / permanent
        if type_allowed("STAGE"):
            try:
                stages_df = session.sql(f"SHOW STAGES IN SCHEMA {qfqn(s_name)}").collect()
                for row in stages_df:
                    row_dict = row.as_dict()
                    if role_filter and row_dict.get("owner", "").upper() != role_filter:
                        continue
                    stage_name = row_dict["name"]
                    url = row_dict.get("url") or ""
                    stage_type = (row_dict.get("type") or "").upper()
                    fqn = qfqn(s_name, stage_name)
                    if url:
                        results.append({"schema": s_name, "object_type": "STAGE", "object_name": stage_name, "status": "UNSUPPORTED", "file_path": "external stage"})
                    elif "TEMPORARY" in stage_type:
                        continue
                    else:
                        stage_list.append({
                            "name": stage_name,
                            "fqn": fqn,
                            "schema": s_name,
                            "directory_enabled": row_dict.get("directory_enabled"),
                            "comment": row_dict.get("comment"),
                        })
                        object_map.append({"name": stage_name, "fqn": fqn, "schema": s_name, "kind": "STAGE"})
            except Exception as e:
                results.append({"schema": s_name, "object_type": "STAGE", "object_name": "*", "status": "ERROR", "file_path": str(e)})

        simple_show_cmds = []
        if type_allowed("SEQUENCE"):
            simple_show_cmds.append((f"SHOW SEQUENCES IN SCHEMA {qfqn(s_name)}", "SEQUENCE"))
        if type_allowed("FILE_FORMAT"):
            simple_show_cmds.append((f"SHOW FILE FORMATS IN SCHEMA {qfqn(s_name)}", "FILE_FORMAT"))
        if type_allowed("ALERT"):
            simple_show_cmds.append((f"SHOW ALERTS IN SCHEMA {qfqn(s_name)}", "ALERT"))
        if type_allowed("TAG"):
            simple_show_cmds.append((f"SHOW TAGS IN SCHEMA {qfqn(s_name)}", "TAG"))
        for show_cmd, ddl_domain in simple_show_cmds:
            try:
                rows = session.sql(show_cmd).collect()
                for row in rows:
                    row_dict = row.as_dict()
                    if role_filter and row_dict.get("owner", "").upper() != role_filter:
                        continue
                    obj_name = row_dict["name"]
                    fqn = qfqn(s_name, obj_name)
                    simple_ddl_list.append({
                        "name": obj_name, "fqn": fqn, "schema": s_name, "domain": ddl_domain,
                    })
                    object_map.append({"name": obj_name, "fqn": fqn, "schema": s_name, "kind": ddl_domain})
            except Exception as e:
                results.append({"schema": s_name, "object_type": ddl_domain, "object_name": "*", "status": "ERROR", "file_path": str(e)})

    object_map.sort(key=lambda x: len(x["name"]), reverse=True)

    grouped_ddl = {}

    schema_ddl_parts = []
    if type_allowed("SCHEMA"):
        for s_name in sorted(schemas_to_scan):
            fqn = qfqn(s_name)
            parts = [f"DEFINE SCHEMA {fqn}"]
            if schema_comments.get(s_name):
                escaped = schema_comments[s_name].replace("'", "''")
                parts.append(f"    COMMENT = '{escaped}'")
            schema_ddl_parts.append((s_name, "\n".join(parts) + ";"))

    if schema_ddl_parts:
        combined = "\n\n".join(ddl for _, ddl in schema_ddl_parts)
        path = Path(output_dir) / db_name / "schemas.sql"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(combined, encoding="utf-8")
        for s_name, _ in schema_ddl_parts:
            results.append({"schema": s_name, "object_type": "SCHEMA", "object_name": s_name, "status": "SAVED", "file_path": str(path)})

    for obj in object_map:
        short_name = obj["name"]
        schema = obj["schema"]
        fqn = obj["fqn"]
        kind = obj["kind"]

        if allowed_schemas is not None and schema not in allowed_schemas:
            continue
        if kind in ("TASK", "FUNCTION", "PROCEDURE", "SEQUENCE", "FILE_FORMAT", "ALERT", "STAGE", "TAG"):
            continue

        try:
            res = session.sql(f"SELECT GET_DDL('TABLE', '{fqn}', TRUE) as DDL").collect()
            ddl_text = res[0]["DDL"]
        except Exception:
            try:
                res = session.sql(f"SELECT GET_DDL('VIEW', '{fqn}', TRUE) as DDL").collect()
                ddl_text = res[0]["DDL"]
            except Exception as e:
                results.append({"schema": schema, "object_type": kind, "object_name": short_name, "status": "ERROR", "file_path": str(e)})
                continue

        if re.match(r"\s*create\s+or\s+replace\s+DYNAMIC\s+TABLE", ddl_text, re.IGNORECASE):
            kind = "DYNAMIC TABLE"

        # Re-check against --object-types now that the actual kind is known.
        if not type_allowed(kind):
            continue

        ddl_text = re.sub(r"^\s*CREATE\s+OR\s+REPLACE\s+", "DEFINE ", ddl_text, flags=re.IGNORECASE)
        ddl_text = re.sub(r"^\s*CREATE\s+", "DEFINE ", ddl_text, flags=re.IGNORECASE)
        ddl_text = normalize_define_keyword(ddl_text)
        ddl_text = fqn_expand(ddl_text, schema, object_map)
        ddl_text = escape_jinja_conflicts(ddl_text)

        collect_grants(f"SHOW GRANTS ON TABLE {fqn}", f"\n-- {kind} {fqn}", schema)

        folder = kind_to_folder(kind)
        if group_files_by_type:
            key = (schema, folder)
            grouped_ddl.setdefault(key, []).append(ddl_text)
            abs_path = str(Path(output_dir) / db_name / schema / f"{folder}.sql")
            results.append({"schema": schema, "object_type": kind, "object_name": short_name, "status": "SAVED", "file_path": abs_path})
        else:
            file_name = f"{short_name}.sql"
            path = write_file(output_dir, db_name, schema, folder, file_name, ddl_text)
            results.append({"schema": schema, "object_type": kind, "object_name": short_name, "status": "SAVED", "file_path": path})

    for task in task_list:
        short_name = task["name"]
        schema = task["schema"]
        fqn = task["fqn"]
        task_def = fqn_expand(task["definition"], schema, object_map)

        parts = [f"DEFINE TASK {fqn}"]
        if task["warehouse"]:
            parts.append(f"    WAREHOUSE = {task['warehouse']}")
        if task["schedule"]:
            parts.append(f"    SCHEDULE = '{task['schedule']}'")
        if task.get("comment"):
            escaped = task["comment"].replace("'", "''")
            parts.append(f"    COMMENT = '{escaped}'")
        parts.append(f"    AS {task_def};")
        ddl_text = "\n".join(parts)
        ddl_text = escape_jinja_conflicts(ddl_text)

        collect_grants(f"SHOW GRANTS ON TASK {fqn}", f"\n-- TASK {fqn}", schema)

        if group_files_by_type:
            key = (schema, "tasks")
            grouped_ddl.setdefault(key, []).append(ddl_text)
            abs_path = str(Path(output_dir) / db_name / schema / "tasks.sql")
            results.append({"schema": schema, "object_type": "TASK", "object_name": short_name, "status": "SAVED", "file_path": abs_path})
        else:
            file_name = f"{short_name}.sql"
            path = write_file(output_dir, db_name, schema, "tasks", file_name, ddl_text)
            results.append({"schema": schema, "object_type": "TASK", "object_name": short_name, "status": "SAVED", "file_path": path})

    for c in callable_list:
        short_name = c["name"]
        schema = c["schema"]
        fqn = c["fqn"]
        domain = c["domain"]
        arguments = c["arguments"]

        sig_for_ddl = fqn
        if arguments:
            # Extract the balanced-paren argument list, handling nested parens
            # like TABLE(NUMBER, NUMBER). The `arguments` column looks like:
            #   "NAME(TABLE(NUMBER, NUMBER)) RETURN TABLE(...)"
            start = arguments.find("(")
            if start != -1:
                depth = 0
                end = -1
                for i in range(start, len(arguments)):
                    ch = arguments[i]
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
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
            ddl_text = res[0]["DDL"]
        except Exception as e:
            results.append({"schema": schema, "object_type": domain, "object_name": short_name, "status": "ERROR", "file_path": str(e)})
            continue

        ddl_text = re.sub(r"^\s*CREATE\s+OR\s+REPLACE\s+", "DEFINE ", ddl_text, flags=re.IGNORECASE)
        ddl_text = re.sub(r"^\s*CREATE\s+", "DEFINE ", ddl_text, flags=re.IGNORECASE)
        ddl_text = normalize_define_keyword(ddl_text)

        quoted_fqn = fqn
        ddl_text = ddl_text.replace(f'"{short_name}"', quoted_fqn, 1)
        ddl_text = escape_jinja_conflicts(ddl_text)

        collect_grants(f"SHOW GRANTS ON {domain} {sig_for_ddl}", f"\n-- {domain} {fqn}", schema)

        folder = kind_to_folder(domain)
        if group_files_by_type:
            key = (schema, folder)
            grouped_ddl.setdefault(key, []).append(ddl_text)
            abs_path = str(Path(output_dir) / db_name / schema / f"{folder}.sql")
            results.append({"schema": schema, "object_type": domain, "object_name": short_name, "status": "SAVED", "file_path": abs_path})
        else:
            file_name = f"{short_name}.sql"
            path = write_file(output_dir, db_name, schema, folder, file_name, ddl_text)
            results.append({"schema": schema, "object_type": domain, "object_name": short_name, "status": "SAVED", "file_path": path})

    for obj in simple_ddl_list:
        short_name = obj["name"]
        schema = obj["schema"]
        fqn = obj["fqn"]
        domain = obj["domain"]

        try:
            res = session.sql(f"SELECT GET_DDL('{domain}', '{fqn}', TRUE) as DDL").collect()
            ddl_text = res[0]["DDL"]
        except Exception as e:
            results.append({"schema": schema, "object_type": domain, "object_name": short_name, "status": "ERROR", "file_path": str(e)})
            continue

        ddl_text = re.sub(r"^\s*CREATE\s+OR\s+REPLACE\s+", "DEFINE ", ddl_text, flags=re.IGNORECASE)
        ddl_text = re.sub(r"^\s*CREATE\s+", "DEFINE ", ddl_text, flags=re.IGNORECASE)
        ddl_text = normalize_define_keyword(ddl_text)
        ddl_text = fqn_expand(ddl_text, schema, object_map)
        ddl_text = escape_jinja_conflicts(ddl_text)

        show_type = domain.replace("_", " ")
        collect_grants(f"SHOW GRANTS ON {show_type} {fqn}", f"\n-- {show_type} {fqn}", schema)

        folder = kind_to_folder(domain)
        if group_files_by_type:
            key = (schema, folder)
            grouped_ddl.setdefault(key, []).append(ddl_text)
            abs_path = str(Path(output_dir) / db_name / schema / f"{folder}.sql")
            results.append({"schema": schema, "object_type": domain, "object_name": short_name, "status": "SAVED", "file_path": abs_path})
        else:
            file_name = f"{short_name}.sql"
            path = write_file(output_dir, db_name, schema, folder, file_name, ddl_text)
            results.append({"schema": schema, "object_type": domain, "object_name": short_name, "status": "SAVED", "file_path": path})

    # Internal stages: build DEFINE STAGE from SHOW STAGES metadata
    for stg in stage_list:
        short_name = stg["name"]
        schema = stg["schema"]
        fqn = stg["fqn"]
        parts = [f"DEFINE STAGE {fqn}"]
        if stg["directory_enabled"] == "Y":
            parts.append("    DIRECTORY = ( ENABLE = TRUE )")
        if stg["comment"]:
            escaped = stg["comment"].replace("'", "''")
            parts.append(f"    COMMENT = '{escaped}'")
        ddl_text = "\n".join(parts) + ";"

        collect_grants(f"SHOW GRANTS ON STAGE {fqn}", f"\n-- STAGE {fqn}", schema)

        if group_files_by_type:
            key = (schema, "stages")
            grouped_ddl.setdefault(key, []).append(ddl_text)
            abs_path = str(Path(output_dir) / db_name / schema / "stages.sql")
            results.append({"schema": schema, "object_type": "STAGE", "object_name": short_name, "status": "SAVED", "file_path": abs_path})
        else:
            file_name = f"{short_name}.sql"
            path = write_file(output_dir, db_name, schema, "stages", file_name, ddl_text)
            results.append({"schema": schema, "object_type": "STAGE", "object_name": short_name, "status": "SAVED", "file_path": path})

    if group_files_by_type and grouped_ddl:
        for (schema, folder), ddl_list in grouped_ddl.items():
            combined = "\n\n".join(ddl_list)
            path = Path(output_dir) / db_name / schema / f"{folder}.sql"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(combined, encoding="utf-8")

    if db_grant_lines:
        db_grants_text = f"-- Database: {db_name}\n" + "\n".join(db_grant_lines)
        path = Path(output_dir) / db_name / "grants.sql"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(db_grants_text, encoding="utf-8")
        results.append({"schema": db_name, "object_type": "GRANTS", "object_name": "GRANTS", "status": "SAVED", "file_path": str(path)})

    for s_name, grant_lines in grants_by_schema.items():
        if not grant_lines:
            continue
        grants_text = "\n".join(grant_lines)
        path = Path(output_dir) / db_name / s_name / "grants.sql"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(grants_text, encoding="utf-8")
        results.append({"schema": s_name, "object_type": "GRANTS", "object_name": "GRANTS", "status": "SAVED", "file_path": str(path)})

    # Aggregate grant-collection failures into one WARNING row per schema
    for schema, count in grant_failure_counts.items():
        results.append({"schema": schema, "object_type": "GRANTS", "object_name": "GRANTS", "status": "WARNING", "file_path": f"{count} SHOW GRANTS call(s) failed in this schema"})

    if not results:
        results.append({"schema": "", "object_type": "", "object_name": "", "status": "NONE", "file_path": "No files generated"})

    # Cosmetic: display FILE_FORMAT as "FILE FORMAT" to match other multi-word
    # kinds like DYNAMIC TABLE.
    for r in results:
        if r["object_type"] == "FILE_FORMAT":
            r["object_type"] = "FILE FORMAT"

    warnings = []
    checked_files = set()
    for r in results:
        if r["status"] == "SAVED" and r["file_path"].endswith(".sql"):
            fpath = Path(r["file_path"])
            if fpath in checked_files or not fpath.exists():
                continue
            checked_files.add(fpath)
            content = fpath.read_text(encoding="utf-8")
            for m in re.finditer(r"BACKFILL\s+FROM\s+(\w+)", content, re.IGNORECASE):
                ref = m.group(1)
                if "." not in ref:
                    warnings.append(f"BACKFILL FROM {ref} in {fpath.name} — bare name not FQN-expanded (object may not exist in scan)")

    session.close()

    print(json.dumps(results, indent=2))

    saved = [r for r in results if r["status"] == "SAVED"]
    errors = [r for r in results if r["status"] == "ERROR"]
    warnings_rows = [r for r in results if r["status"] == "WARNING"]
    unsupported = [r for r in results if r["status"] == "UNSUPPORTED"]
    print(f"\nSummary: {len(saved)} saved, {len(errors)} errors, {len(warnings_rows)} warnings, {len(unsupported)} unsupported", file=sys.stderr)
    if role_filter:
        print(f"  Role filter: {matched_object_count} of {total_object_count} objects matched role {role_filter}", file=sys.stderr)
    for w in warnings:
        print(f"  WARNING: {w}", file=sys.stderr)


if __name__ == "__main__":
    main()
