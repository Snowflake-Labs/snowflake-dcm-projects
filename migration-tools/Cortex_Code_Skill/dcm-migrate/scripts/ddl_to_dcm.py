#!/usr/bin/env python3
"""
ddl_to_dcm.py - Convert existing Snowflake object DDL to DCM DEFINE syntax.

Scans a database, retrieves DDL for all objects, converts CREATE to DEFINE,
expands references to fully qualified names, and writes definition files
to a local output directory (one file per object type per schema).

Roles and grants are out of scope here; use grants_to_dcm.py for those.

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/ddl_to_dcm.py \
        --db-name <DB_NAME> \
        --output-path <OUTPUT_DIR> \
        [--schema-list SCHEMA1 SCHEMA2 ...] \
        [--object-types TYPE1 TYPE2 ...] \
        [--role <ROLE_NAME>] \
        [--connection <CONNECTION_NAME>]
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

from snowflake.snowpark import Session


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

# --db-name and the --schema-list entries are interpolated into the discovery
# commands below, so both must be ordinary unquoted identifiers. Anything else is
# rejected at entry rather than reaching SQL.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

# Settable scalar params that may be gap-filled from DESCRIBE AS RESOURCE when
# GET_DDL omits them (generic diff; the allowlist keeps emission safe by never
# emitting read-only metadata or keys whose DDL form isn't KEY = value). Nested /
# structural properties (columns, cluster_by, predecessors, ...) come from GET_DDL.
_RESOURCE_FILL_PARAMS = {
    "DATA_RETENTION_TIME_IN_DAYS",
    "MAX_DATA_EXTENSION_TIME_IN_DAYS",
    "CHANGE_TRACKING",
    "DEFAULT_DDL_COLLATION",
    "COMMENT",
    "DATA_METRIC_SCHEDULE",
}

# Supported object types and how each is handled. 'show' is the discovery
# command for callable/simple/policy types. Order is significant: it sets the
# order of definitions within each type's grouped file.
OBJECT_TYPES = {
    "TABLE":                 {"folder": "tables",         "category": "tableview"},
    "VIEW":                  {"folder": "views",          "category": "tableview"},
    "DYNAMIC TABLE":         {"folder": "dynamic_tables", "category": "tableview"},
    "TASK":                  {"folder": "tasks",          "category": "task"},
    "FUNCTION":              {"folder": "functions",      "category": "callable", "show": "SHOW USER FUNCTIONS"},
    "PROCEDURE":             {"folder": "procedures",     "category": "callable", "show": "SHOW USER PROCEDURES"},
    "SEQUENCE":              {"folder": "sequences",      "category": "simple",   "show": "SHOW SEQUENCES"},
    "FILE_FORMAT":           {"folder": "file_formats",   "category": "simple",   "show": "SHOW FILE FORMATS"},
    "ALERT":                 {"folder": "alerts",         "category": "simple",   "show": "SHOW ALERTS"},
    "TAG":                   {"folder": "tags",           "category": "simple",   "show": "SHOW TAGS"},
    "MASKING_POLICY":        {"folder": "policies",       "category": "policy",   "show": "SHOW MASKING POLICIES",        "get_ddl_domain": "POLICY"},
    "AUTHENTICATION_POLICY": {"folder": "policies",       "category": "policy",   "show": "SHOW AUTHENTICATION POLICIES", "get_ddl_domain": "POLICY"},
    "PIPE":                  {"folder": "pipes",          "category": "pipe"},
    "STREAM":                {"folder": "streams",        "category": "stream"},
    "STAGE":                 {"folder": "stages",         "category": "stage"},
}

# Types handled by dedicated loops below (all except the table/view family).
NON_TABLEVIEW_KINDS = {k for k, v in OBJECT_TYPES.items() if v["category"] != "tableview"}

# Types discovered via their own 'show' command.
CALLABLE_TYPES = {k: v for k, v in OBJECT_TYPES.items() if v["category"] == "callable"}
SIMPLE_DDL_TYPES = {k: v for k, v in OBJECT_TYPES.items() if v["category"] == "simple"}
POLICY_TYPES = {k: v for k, v in OBJECT_TYPES.items() if v["category"] == "policy"}


def _flex(s):
    # Case/whitespace/underscore-insensitive match key, e.g. "File Format",
    # "FILE_FORMAT", and "fileformat" all normalize to the same token.
    return re.sub(r"[\s_]+", "", str(s).upper())


# Every type name the --object-types filter recognizes: all OBJECT_TYPES keys,
# plus the two container-level tokens SCHEMA and DATABASE.
_ALL_TYPE_TOKENS = {_flex(k): k for k in OBJECT_TYPES}
_ALL_TYPE_TOKENS[_flex("SCHEMA")] = "SCHEMA"
_ALL_TYPE_TOKENS[_flex("DATABASE")] = "DATABASE"


def get_session(connection_name):
    return Session.builder.config("connection_name", connection_name).create()


def qi(ident):
    # Quote an identifier so mixed-case, reserved-word, and special-character
    # names round-trip correctly through GET_DDL and into the DEFINE header.
    return '"' + str(ident).replace('"', '""') + '"'


def qfqn(*parts):
    return ".".join(qi(p) for p in parts)


def esc(s):
    # Escape a single-quoted SQL string literal's embedded quotes.
    return str(s).replace("'", "''")


def kind_to_folder(kind):
    # Helper: map object kind to a folder name (see OBJECT_TYPES).
    type_spec = OBJECT_TYPES.get(kind.upper())
    return type_spec["folder"] if type_spec else "other"


def owner_of(row):
    try:
        return (row.as_dict().get("owner") or "").upper()
    except Exception:
        return ""


def to_define(ddl_text):
    # CREATE [OR REPLACE] <obj> ...  ->  DEFINE <obj> ...
    ddl_text = re.sub(r"^\s*CREATE\s+OR\s+REPLACE\s+", "DEFINE ", ddl_text, flags=re.IGNORECASE)
    return re.sub(r"^\s*CREATE\s+", "DEFINE ", ddl_text, flags=re.IGNORECASE)


def normalize_define_keyword(ddl_text):
    return re.sub(
        r"^(DEFINE\s+)(dynamic\s+table|file\s+format|masking\s+policy|authentication\s+policy|table|view|schema|task|function|procedure|sequence|alert|policy|pipe|stage|tag)",
        lambda m: m.group(1) + m.group(2).upper(),
        ddl_text,
        count=1,
        flags=re.IGNORECASE,
    )


def qualify_header(ddl_text, short_name, q_fqn, keyword):
    # GET_DDL renders TASK/POLICY/PIPE headers with an unqualified name;
    # qualify it to the full FQN. Prefer a straight replace of the quoted
    # short name (handles special characters); fall back to matching the
    # keyword + bare name for cases where GET_DDL didn't quote it.
    quoted_tok = qi(short_name)
    if quoted_tok in ddl_text:
        return ddl_text.replace(quoted_tok, q_fqn, 1)
    return re.sub(
        r"(?i)(\b" + keyword + r"\s+)" + re.escape(short_name) + r"\b",
        lambda m: m.group(1) + q_fqn,
        ddl_text,
        count=1,
    )


def escape_jinja_conflicts(ddl_text):
    # Detect `{{` or `}}` that are NOT part of a well-formed `{{ name }}` Jinja
    # variable reference. Stray braces (e.g. SQL string manipulation) would fail
    # DCM ANALYZE, so the body is wrapped in {% raw %}.
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


def strip_attachments(text):
    # Remove tag/policy attachment clauses that GET_DDL embeds in table/view
    # DDL so this script emits pure object structure. DCM does not support
    # setting tags/policies via CREATE OR ALTER, so their associations are out
    # of scope here. Mask strings/quoted idents/comments first so literals are
    # never touched. Returns (text, count_removed).
    masked = []

    def _mask(m):
        masked.append(m.group(0))
        return f"\x00{len(masked) - 1}\x00"

    t = _PROTECTED_RE.sub(_mask, text)
    n = 0
    t, c = re.subn(r"(?i)\s+WITH\s+TAG\s*\([^)]*\)", "", t)
    n += c
    t, c = re.subn(r"(?i)\s+WITH\s+MASKING\s+POLICY\s+[^\s(),;]+(?:\s+USING\s*\([^)]*\))?", "", t)
    n += c
    t, c = re.subn(r"(?i)\s+WITH\s+ROW\s+ACCESS\s+POLICY\s+[^\s(),;]+\s+ON\s*\([^)]*\)", "", t)
    n += c
    if masked:
        t = re.sub(r"\x00(\d+)\x00", lambda m: masked[int(m.group(1))], t)
    return t, n


def insert_before_keyword(text, insertion, boundary_kw):
    # Generic insert immediately before the first occurrence of boundary_kw
    # (e.g. 'as', 'if'), used for header properties GET_DDL omits that must
    # land before the object's body rather than at the end of the statement.
    # Protected spans are masked so a keyword inside a string/comment is
    # never matched.
    masked = []

    def _mask(m):
        masked.append(m.group(0))
        return f"\x00{len(masked) - 1}\x00"

    mt = _PROTECTED_RE.sub(_mask, text)
    m = re.search(r"(?i)\b" + boundary_kw + r"\b", mt)
    if not m:
        return text
    mt = mt[: m.start()] + insertion + mt[m.start():]
    if masked:
        mt = re.sub(r"\x00(\d+)\x00", lambda mm: masked[int(mm.group(1))], mt)
    return mt


def insert_target_state(text, state, boundary_kw):
    # DCM tasks/alerts carry a target state (STARTED/SUSPENDED) that GET_DDL
    # does not expose; newly deployed objects default to SUSPENDED. To preserve
    # a currently-running object, emit STARTED immediately before its body
    # keyword (AS for tasks, IF for alerts). Suspended objects need no keyword
    # (matches the default), so they are left unchanged.
    if (state or "").lower() != "started":
        return text
    return insert_before_keyword(text, "STARTED ", boundary_kw)


def schedule_to_str(sched):
    # DESCRIBE AS RESOURCE returns data_metric_schedule as a nested object
    # (e.g. {"schedule_type": "CRON", "cron_expr": "...", "timezone": "..."}).
    # Flatten it to the string literal DATA_METRIC_SCHEDULE expects.
    # Returns None if the shape isn't recognized (safer than guessing).
    if not isinstance(sched, dict):
        return None
    st = (sched.get("schedule_type") or "").upper()
    if st == "TRIGGER_ON_CHANGES":
        return "TRIGGER_ON_CHANGES"
    if st == "CRON":
        expr = sched.get("cron_expr")
        tz = sched.get("timezone")
        return f"USING CRON {expr} {tz}" if expr and tz else None
    if st in ("MINUTES", "MINUTE"):
        n = sched.get("minutes")
        return f"{n} MINUTE" if n else None
    if st in ("HOURS", "HOUR"):
        n = sched.get("hours")
        return f"{n} HOUR" if n else None
    return None


def is_ambiguous_schedule(sched):
    # MINUTES:60 is Snowflake's platform default reported by DESCRIBE AS
    # RESOURCE even when no schedule was ever explicitly configured
    # (confirmed on tables/views with zero attached DMFs), so a MINUTES or
    # HOURS interval cannot be trusted at face value. CRON and
    # TRIGGER_ON_CHANGES never appear unless genuinely configured.
    return isinstance(sched, dict) and (sched.get("schedule_type") or "").upper() in ("MINUTES", "MINUTE", "HOURS", "HOUR")


def _fmt_val(v):
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + esc(v) + "'"


def resource_gap_fill(ddl_text, resource):
    # Generic diff: append any allowlisted scalar param present in the resource
    # JSON but absent from the GET_DDL text. For non-AS-bodied objects only
    # (params inserted before the trailing ';'). Returns (ddl_text, [names]).
    if not resource:
        return ddl_text, []
    upper = ddl_text.upper()
    extras, names = [], []
    for k, v in resource.items():
        param = k.upper()
        if param not in _RESOURCE_FILL_PARAMS:
            continue
        if v is None or v == "" or v is False:
            continue
        if isinstance(v, (dict, list)):
            continue
        if param in upper:
            continue
        extras.append(f"    {param} = {_fmt_val(v)}")
        names.append(param)
    if not extras:
        return ddl_text, []
    body = ddl_text.rstrip()
    trailing = ""
    if body.endswith(";"):
        body = body[:-1].rstrip()
        trailing = ";"
    return body + "\n" + "\n".join(extras) + trailing, names


def extract_arg_signature(arguments):
    # Extract the argument signature, allowing nested parens like
    # TABLE(NUMBER, NUMBER).
    if not arguments:
        return "()"
    start = arguments.find("(")
    if start == -1:
        return "()"
    depth, end = 0, -1
    for i in range(start, len(arguments)):
        ch = arguments[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                end = i
                break
    return arguments[start:end + 1] if end != -1 else "()"


def main():
    parser = argparse.ArgumentParser(description="Convert Snowflake DDL to DCM DEFINE syntax")
    parser.add_argument("--db-name", required=True, help="Source database name")
    parser.add_argument("--output-path", required=True, help="Local output directory for generated files")
    parser.add_argument("--schema-list", nargs="*", default=None, help="Optional schema allow-list")
    parser.add_argument("--object-types", nargs="*", default=None, help="Optional object-type allow-list")
    parser.add_argument("--connection", default=None, help="Snowflake connection name")
    parser.add_argument("--role", default=None, help="Only migrate objects owned by this role (filters by owner column)")
    args = parser.parse_args()

    # Validate identifiers before any SQL is built.
    invalid = []
    if not _IDENT_RE.match(str(args.db_name or "")):
        invalid.append(("DATABASE", str(args.db_name)))
    for s in (args.schema_list or []):
        if not _IDENT_RE.match(str(s or "")):
            invalid.append(("SCHEMA", str(s)))
    if invalid:
        print(json.dumps([{
            "schema": "", "object_type": kind, "object_name": name, "status": "ERROR",
            "file_path": "invalid identifier: expected an ordinary unquoted name matching [A-Za-z_][A-Za-z0-9_$]*",
        } for kind, name in invalid], indent=2))
        sys.exit(1)

    db_name = args.db_name.upper()
    output_dir = args.output_path
    allowed_schemas = set(s.upper() for s in args.schema_list) if args.schema_list else None
    connection_name = args.connection or os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default_connection_name"
    role_filter = args.role.upper() if args.role else None

    results = []

    def add(schema, object_type, object_name, status, file_path):
        results.append({
            "schema": schema,
            "object_type": object_type,
            "object_name": object_name,
            "status": status,
            "file_path": file_path,
        })

    # --object-types: omitted/empty = allow every type. Unrecognized names are
    # reported as an ERROR row rather than silently ignored or failing the run.
    allowed_types = None
    if args.object_types:
        allowed_types = set()
        for t in args.object_types:
            canon = _ALL_TYPE_TOKENS.get(_flex(t))
            if canon:
                allowed_types.add(canon)
            else:
                add("", "OBJECT_TYPE_FILTER", str(t), "ERROR", "unknown object type in --object-types")

    def type_allowed(name):
        return allowed_types is None or name in allowed_types

    session = get_session(connection_name)

    # 2. Build inventory (scan all schemas)
    try:
        objects_df = session.sql(f"SHOW OBJECTS IN DATABASE {db_name}").collect()
    except Exception as e:
        print(json.dumps([{
            "schema": db_name, "object_type": "DATABASE", "object_name": db_name,
            "status": "ERROR", "file_path": f"Cannot access database '{db_name}': {e}",
        }], indent=2))
        session.close()
        sys.exit(1)

    object_map = []
    schema_comments = {}  # schema_name -> comment
    total_object_count = 0
    matched_object_count = 0

    # Semantic views are unsupported and report as kind='VIEW' in SHOW OBJECTS,
    # so collect their FQNs here to exclude them below.
    semantic_view_fqns = set()
    try:
        sv_df = session.sql(f"SHOW SEMANTIC VIEWS IN DATABASE {db_name}").collect()
        for sv_row in sv_df:
            semantic_view_fqns.add(f"{db_name}.{sv_row['schema_name'].upper()}.{sv_row['name']}")
    except Exception as e:
        add(db_name, "WARNING", "SEMANTIC_VIEW_LOOKUP", "ERROR", str(e))

    for row in objects_df:
        s_name = row["schema_name"].upper()
        kind = row["kind"]
        k_upper = kind.upper()
        fqn_check = f"{db_name}.{s_name}.{row['name']}"
        # Streams are handled via SHOW STREAMS in the per-schema loop
        if k_upper == "STREAM":
            continue
        # Stages are handled via SHOW STAGES in the per-schema loop
        if k_upper == "STAGE":
            continue
        # Exclude semantic views (matched by FQN or kind). Only reported when
        # the schema is actually in scope for this run.
        if fqn_check in semantic_view_fqns or "SEMANTIC" in k_upper:
            if allowed_schemas is None or s_name in allowed_schemas:
                add(s_name, kind, row["name"], "UNSUPPORTED", "semantic views")
            continue
        # Type filter: VIEW kind is unambiguous here. TABLE kind is not (a
        # dynamic table also reports as TABLE until its DDL is inspected), so
        # only drop it now if neither TABLE nor DYNAMIC TABLE is allowed.
        if k_upper == "VIEW" and not type_allowed("VIEW"):
            continue
        if k_upper == "TABLE" and not type_allowed("TABLE") and not type_allowed("DYNAMIC TABLE"):
            continue
        if s_name != "INFORMATION_SCHEMA":
            total_object_count += 1
            if role_filter and owner_of(row) != role_filter:
                continue
            matched_object_count += 1
            object_map.append({
                "name": row["name"],
                "fqn": f"{db_name}.{s_name}.{row['name']}",
                "schema": s_name,
                "kind": kind,
            })

    # 2b. Scan per-schema object types (SHOW OBJECTS does not include these)
    schemas_to_scan = set(allowed_schemas) if allowed_schemas else set()
    try:
        schemas_df = session.sql(f"SHOW SCHEMAS IN DATABASE {db_name}").collect()
        for row in schemas_df:
            s_name = row["name"].upper()
            if s_name == "INFORMATION_SCHEMA":
                continue
            if role_filter and owner_of(row) != role_filter:
                continue
            schema_comments[s_name] = row["comment"] or ""
            if not allowed_schemas:
                schemas_to_scan.add(s_name)
    except Exception as e:
        add(db_name, "WARNING", "SCHEMA_LOOKUP", "ERROR", str(e))

    task_list = []
    callable_list = []      # functions and procedures
    simple_ddl_list = []    # sequences, file formats, alerts, tags
    stage_list = []         # permanent stages
    policy_list = []        # masking and authentication policies
    pipe_list = []          # pipes
    stream_list = []        # streams

    # Restrict each per-schema discovery loop to types the caller allowed,
    # so an excluded type never issues its SHOW command at all.
    allowed_callable_types = {k: v for k, v in CALLABLE_TYPES.items() if type_allowed(k)}
    allowed_simple_ddl_types = {k: v for k, v in SIMPLE_DDL_TYPES.items() if type_allowed(k)}
    allowed_policy_types = {k: v for k, v in POLICY_TYPES.items() if type_allowed(k)}

    for s_name in schemas_to_scan:
        if type_allowed("TASK"):
            try:
                tasks_df = session.sql(f"SHOW TASKS IN SCHEMA {db_name}.{s_name}").collect()
                for row in tasks_df:
                    if role_filter and owner_of(row) != role_filter:
                        continue
                    task_name = row["name"]
                    fqn = f"{db_name}.{s_name}.{task_name}"
                    task_list.append({
                        "name": task_name,
                        "fqn": fqn,
                        "schema": s_name,
                        "state": row["state"],
                    })
                    object_map.append({"name": task_name, "fqn": fqn, "schema": s_name, "kind": "TASK"})
            except Exception as e:
                add(s_name, "TASK", "*", "ERROR", str(e))

        for ddl_domain, type_spec in allowed_callable_types.items():
            try:
                rows = session.sql(f"{type_spec['show']} IN SCHEMA {db_name}.{s_name}").collect()
                for row in rows:
                    row_dict = row.as_dict()
                    if role_filter and (row_dict.get("owner") or "").upper() != role_filter:
                        continue
                    obj_name = row_dict["name"]
                    fqn = f"{db_name}.{s_name}.{obj_name}"
                    # SHOW USER FUNCTIONS also returns data metric functions
                    # (is_data_metric='Y'). A DMF's TABLE argument requires a column
                    # name to be (re)created, but GET_DDL and DESCRIBE both drop it
                    # (they render TABLE(NUMBER), not TABLE(col NUMBER)), so the DDL
                    # cannot be regenerated. Report as UNSUPPORTED rather than emit a
                    # file that fails to compile and aborts the whole plan.
                    if str(row_dict.get("is_data_metric", "")).upper() == "Y":
                        add(s_name, "DATA_METRIC_FUNCTION", obj_name, "UNSUPPORTED",
                            "TABLE-argument column name not exposed by GET_DDL/DESCRIBE - cannot regenerate")
                        continue
                    callable_list.append({
                        "name": obj_name,
                        "fqn": fqn,
                        "schema": s_name,
                        "domain": ddl_domain,
                        "arguments": row_dict.get("arguments", ""),
                    })
                    object_map.append({"name": obj_name, "fqn": fqn, "schema": s_name, "kind": ddl_domain})
            except Exception as e:
                add(s_name, ddl_domain, "*", "ERROR", str(e))

        for ddl_domain, type_spec in allowed_simple_ddl_types.items():
            try:
                rows = session.sql(f"{type_spec['show']} IN SCHEMA {db_name}.{s_name}").collect()
                for row in rows:
                    row_dict = row.as_dict()
                    if role_filter and (row_dict.get("owner") or "").upper() != role_filter:
                        continue
                    obj_name = row_dict["name"]
                    fqn = f"{db_name}.{s_name}.{obj_name}"
                    simple_ddl_list.append({
                        "name": obj_name,
                        "fqn": fqn,
                        "schema": s_name,
                        "domain": ddl_domain,
                        "state": row_dict.get("state", ""),
                        "comment": row_dict.get("comment", "") or "",
                    })
                    object_map.append({"name": obj_name, "fqn": fqn, "schema": s_name, "kind": ddl_domain})
            except Exception as e:
                add(s_name, ddl_domain, "*", "ERROR", str(e))

        # Policies: masking and authentication (GET_DDL domain 'POLICY').
        for ddl_domain, type_spec in allowed_policy_types.items():
            try:
                rows = session.sql(f"{type_spec['show']} IN SCHEMA {db_name}.{s_name}").collect()
                for row in rows:
                    if role_filter and owner_of(row) != role_filter:
                        continue
                    obj_name = row["name"]
                    fqn = f"{db_name}.{s_name}.{obj_name}"
                    policy_list.append({
                        "name": obj_name,
                        "fqn": fqn,
                        "schema": s_name,
                        "domain": ddl_domain,
                        "get_ddl_domain": type_spec["get_ddl_domain"],
                        "comment": row["comment"] or "",
                    })
                    object_map.append({"name": obj_name, "fqn": fqn, "schema": s_name, "kind": ddl_domain})
            except Exception as e:
                add(s_name, ddl_domain, "*", "ERROR", str(e))

        # Stages: split by external / temporary / permanent
        if type_allowed("STAGE"):
            try:
                stages_df = session.sql(f"SHOW STAGES IN SCHEMA {db_name}.{s_name}").collect()
                for row in stages_df:
                    if role_filter and owner_of(row) != role_filter:
                        continue
                    stage_name = row["name"]
                    url = row["url"] or ""
                    stage_type = (row["type"] or "").upper()
                    has_creds = str(row["has_credentials"] or "").upper() == "Y"
                    storage_integration = row["storage_integration"] or ""
                    fqn = f"{db_name}.{s_name}.{stage_name}"
                    if "TEMPORARY" in stage_type:
                        continue
                    # External stages (have a URL): only those backed by a storage
                    # integration are reconstructable and secret-free. Inline-credential
                    # or integration-less external stages are reported UNSUPPORTED
                    # (secrets must never be written into definition artifacts).
                    if url and has_creds:
                        add(s_name, "STAGE", stage_name, "UNSUPPORTED",
                            "external stage with inline credentials (use a storage integration)")
                        continue
                    if url and not storage_integration:
                        add(s_name, "STAGE", stage_name, "UNSUPPORTED",
                            "external stage without a storage integration")
                        continue
                    stage_list.append({
                        "name": stage_name,
                        "fqn": fqn,
                        "schema": s_name,
                        "url": url,
                        "storage_integration": storage_integration,
                        "directory_enabled": row["directory_enabled"],
                        "comment": row["comment"],
                    })
                    object_map.append({"name": stage_name, "fqn": fqn, "schema": s_name, "kind": "STAGE"})
            except Exception as e:
                add(s_name, "STAGE", "*", "ERROR", str(e))

        # Pipes (SHOW PIPES; GET_DDL('PIPE', ...) in the generation loop).
        if type_allowed("PIPE"):
            try:
                pipes_df = session.sql(f"SHOW PIPES IN SCHEMA {db_name}.{s_name}").collect()
                for row in pipes_df:
                    if role_filter and owner_of(row) != role_filter:
                        continue
                    pipe_name = row["name"]
                    fqn = f"{db_name}.{s_name}.{pipe_name}"
                    pipe_list.append({"name": pipe_name, "fqn": fqn, "schema": s_name})
                    object_map.append({"name": pipe_name, "fqn": fqn, "schema": s_name, "kind": "PIPE"})
            except Exception as e:
                add(s_name, "PIPE", "*", "ERROR", str(e))

        # Streams. Built from SHOW STREAMS metadata rather than GET_DDL: for a
        # stream on a dynamic table, GET_DDL('STREAM') renders the source as a
        # single quoted identifier containing dots ("DB.SCHEMA.NAME") instead of
        # a qualified name, which does not resolve. SHOW exposes source_type,
        # a fully qualified table_name, and mode, which is everything a stream
        # persists, so the DEFINE is assembled from those instead.
        if type_allowed("STREAM"):
            try:
                streams_df = session.sql(f"SHOW STREAMS IN SCHEMA {db_name}.{s_name}").collect()
                for row in streams_df:
                    if role_filter and owner_of(row) != role_filter:
                        continue
                    stream_name = row["name"]
                    fqn = f"{db_name}.{s_name}.{stream_name}"
                    stream_list.append({
                        "name": stream_name,
                        "fqn": fqn,
                        "schema": s_name,
                        "source_type": row["source_type"] or "",
                        "source_name": row["table_name"] or "",
                        "mode": row["mode"] or "",
                        "comment": row["comment"] or "",
                    })
                    object_map.append({"name": stream_name, "fqn": fqn, "schema": s_name, "kind": "STREAM"})
            except Exception as e:
                add(s_name, "STREAM", "*", "ERROR", str(e))

    # Longest names first so fqn_expand replaces them before shorter substrings.
    object_map.sort(key=lambda x: len(x["name"]), reverse=True)

    # Bucket by schema once so fqn_expand scans only same-schema names instead of
    # the whole map on every object (avoids O(N^2) work at scale).
    objects_by_schema = {}
    for o in object_map:
        objects_by_schema.setdefault(o["schema"], []).append(o)

    def fqn_expand(text, source_schema, exclude_names=frozenset()):
        # Mask string literals, quoted identifiers, and comments so the
        # bare-name replace below only touches actual code, then restore them.
        masked = []

        def _mask(m):
            masked.append(m.group(0))
            return f"\x00{len(masked) - 1}\x00"

        text = _PROTECTED_RE.sub(_mask, text)

        for target_obj in objects_by_schema.get(source_schema, ()):
            t_name = target_obj["name"]
            t_fqn = target_obj["fqn"]
            # Only qualify it in genuine reference positions (after FROM/JOIN/REFERENCES),
            # so e.g. `SELECT price ... FROM price` keeps the column but qualifies the table.
            if t_name.upper() in exclude_names:
                ref_pat = re.compile(r"(?i)\b(from|join|references)(\s+){}\b".format(re.escape(t_name)))
                text, _ = ref_pat.subn(lambda m, f=t_fqn: m.group(1) + m.group(2) + f, text)
            else:
                pattern = r'(?i)(?<!\.|")\b{}\b'.format(re.escape(t_name))
                text, _ = re.subn(pattern, t_fqn, text)

        if masked:
            text = re.sub(r"\x00(\d+)\x00", lambda m: masked[int(m.group(1))], text)
        return text

    def describe_as_resource(domain_kw, fqn_quoted):
        # DCM-native resource model (JSON) for an object. Used as a generic
        # gap-fill source for scalar params GET_DDL omits. Returns {} on failure.
        try:
            row = session.sql(f"DESCRIBE AS RESOURCE {domain_kw} {fqn_quoted}").collect()
            return json.loads(row[0][0])
        except Exception:
            return {}

    def has_dmf(ref_domain, fqn_quoted):
        # DESCRIBE AS RESOURCE reports data_metric_schedule = MINUTES:60 even
        # for objects with zero attached DMFs (Snowflake's platform default,
        # not a genuinely configured value), so it cannot be trusted on its
        # own. Only gap-fill DATA_METRIC_SCHEDULE when a DMF is actually
        # attached, confirmed via DATA_METRIC_FUNCTION_REFERENCES.
        try:
            rows = session.sql(
                "SELECT COUNT(*) AS N FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES("
                f"REF_ENTITY_NAME => '{fqn_quoted}', REF_ENTITY_DOMAIN => '{ref_domain}'))"
            ).collect()
            return rows[0]["N"] > 0
        except Exception:
            return False

    grouped_ddl = {}  # (schema, type_folder) -> [ddl_text, ...]

    def emit(schema, kind, short_name, ddl_text):
        # Route one generated DEFINE into its type's grouped file (one file
        # per object type per schema) and record the SAVED row.
        folder = kind_to_folder(kind)
        key = (schema, folder)
        grouped_ddl.setdefault(key, []).append(escape_jinja_conflicts(ddl_text))
        results.append({
            "schema": schema, "object_type": kind, "object_name": short_name,
            "status": "SAVED", "file_path": key,
        })

    def write_local(rel_parts, content):
        path = Path(output_dir).joinpath(*rel_parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return str(path)

    # 2c. Generate DEFINE DATABASE / DEFINE SCHEMA statements (one file under the db folder)
    container_ddl_parts = []  # ('__DATABASE__' or schema_name, ddl_text)

    if type_allowed("DATABASE"):
        db_comment = ""
        try:
            db_rows = session.sql(f"SHOW DATABASES LIKE '{esc(db_name)}'").collect()
            if db_rows:
                db_comment = db_rows[0]["comment"] or ""
        except Exception as e:
            add(db_name, "WARNING", "DATABASE_LOOKUP", "ERROR", str(e))
        parts = [f"DEFINE DATABASE {qi(db_name)}"]
        if db_comment:
            parts.append(f"    COMMENT = '{esc(db_comment)}'")
        container_ddl_parts.append(("__DATABASE__", "\n".join(parts) + ";"))

    if type_allowed("SCHEMA"):
        for s_name in sorted(schemas_to_scan):
            parts = [f"DEFINE SCHEMA {db_name}.{s_name}"]
            if schema_comments.get(s_name):
                parts.append(f"    COMMENT = '{esc(schema_comments[s_name])}'")
            container_ddl_parts.append((s_name, "\n".join(parts) + ";"))

    if container_ddl_parts:
        combined = "\n\n".join(ddl for _, ddl in container_ddl_parts)
        schemas_path = write_local([db_name, "schemas.sql"], combined)
        for name, _ in container_ddl_parts:
            if name == "__DATABASE__":
                add("", "DATABASE", db_name, "SAVED", schemas_path)
            else:
                add(name, "SCHEMA", name, "SAVED", schemas_path)

    # Build a map of each object's own column names so fqn_expand can avoid
    # qualifying a bare token that is actually a column of the object being
    # defined (a same-schema name collision that breaks the DDL).
    # Best-effort: skip the check if the lookup fails.
    columns_by_object = {}
    try:
        col_rows = session.sql(
            f"SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM {db_name}.INFORMATION_SCHEMA.COLUMNS"
        ).collect()
        for cr in col_rows:
            key = (cr["TABLE_SCHEMA"].upper(), cr["TABLE_NAME"].upper())
            columns_by_object.setdefault(key, set()).add(cr["COLUMN_NAME"].upper())
    except Exception as e:
        add(db_name, "WARNING", "COLUMN_LOOKUP", "ERROR", str(e))

    # 3. Generate DDL for tables / views / dynamic tables
    for obj in object_map:
        short_name = obj["name"]
        schema = obj["schema"]
        kind = obj["kind"]

        if allowed_schemas is not None and schema not in allowed_schemas:
            continue
        if kind in NON_TABLEVIEW_KINDS:
            continue

        q_fqn = qfqn(db_name, schema, short_name)
        try:
            res = session.sql(f"SELECT GET_DDL('TABLE', '{q_fqn}', TRUE) as DDL").collect()
            ddl_text = res[0]["DDL"]
        except Exception:
            try:
                res = session.sql(f"SELECT GET_DDL('VIEW', '{q_fqn}', TRUE) as DDL").collect()
                ddl_text = res[0]["DDL"]
            except Exception as e:
                add(schema, kind, short_name, "ERROR", str(e))
                continue

        # Detect actual kind from DDL (SHOW OBJECTS reports dynamic tables as TABLE)
        if re.match(r"\s*create\s+or\s+replace\s+DYNAMIC\s+TABLE", ddl_text, re.IGNORECASE):
            kind = "DYNAMIC TABLE"

        # Catches the ambiguous case the discovery-time pre-filter couldn't
        # decide (e.g. only DYNAMIC TABLE allowed, but this turned out to be
        # a plain TABLE once GET_DDL revealed the real kind).
        if not type_allowed(kind):
            continue

        ddl_text = normalize_define_keyword(to_define(ddl_text))
        # Strip tag/policy attachment clauses GET_DDL embeds. DCM does not support
        # setting tags/policies via CREATE OR ALTER, so their associations are out
        # of scope here (the tag/policy objects themselves are emitted).
        ddl_text, n_strip = strip_attachments(ddl_text)
        if n_strip:
            add(schema, kind, short_name, "INFO",
                f"stripped {n_strip} tag/policy attachment clause(s) - associations are out of scope for this script")
        # GET_DDL(..., TRUE) already returns a qualified header; fqn_expand only
        # qualifies unqualified BODY references, with the object's own columns
        # excluded so column names/aliases are never rewritten.
        own_cols = columns_by_object.get((schema.upper(), short_name.upper()), set())
        ddl_text = fqn_expand(ddl_text, schema, exclude_names=own_cols)

        # Generic gap-fill: append settable scalar params GET_DDL omitted (e.g.
        # DATA_RETENTION_TIME_IN_DAYS), diffed from DESCRIBE AS RESOURCE. Tables
        # are non-AS-bodied so extras can be appended before the trailing ';'.
        if kind == "TABLE":
            _res = describe_as_resource("TABLE", q_fqn)
            raw_sched = _res.get("data_metric_schedule")
            if is_ambiguous_schedule(raw_sched) and not has_dmf("table", q_fqn):
                _res["data_metric_schedule"] = None
            else:
                _res["data_metric_schedule"] = schedule_to_str(raw_sched)
            ddl_text, _filled = resource_gap_fill(ddl_text, _res)
            if _filled:
                add(schema, kind, short_name, "INFO",
                    f"gap-filled from DESCRIBE AS RESOURCE: {', '.join(_filled)}")

        # Views and dynamic tables are AS-bodied, so DATA_METRIC_SCHEDULE (the
        # only settable scalar param relevant to them) must be inserted before
        # AS rather than appended at the end. Without it, any attached DMF
        # expectations silently stop running on adoption. A MINUTES/HOURS
        # interval is only trusted when a DMF is actually attached (see
        # is_ambiguous_schedule); CRON/TRIGGER_ON_CHANGES are always captured.
        if kind in ("VIEW", "DYNAMIC TABLE") and "DATA_METRIC_SCHEDULE" not in ddl_text.upper():
            _res = describe_as_resource(kind, q_fqn)
            raw_sched = _res.get("data_metric_schedule")
            dmf_domain = "view" if kind == "VIEW" else "dynamic table"
            if is_ambiguous_schedule(raw_sched) and not has_dmf(dmf_domain, q_fqn):
                sched = None
            else:
                sched = schedule_to_str(raw_sched)
            if sched:
                ddl_text = insert_before_keyword(ddl_text, f"    DATA_METRIC_SCHEDULE = '{sched}'\n", "as")
                add(schema, kind, short_name, "INFO", f"gap-filled DATA_METRIC_SCHEDULE: {sched}")

        emit(schema, kind, short_name, ddl_text)

    # 3b. Generate DEFINE TASK statements via GET_DDL (captures AFTER, WHEN, and
    #     task parameters that SHOW TASKS metadata omits).
    for task in task_list:
        short_name = task["name"]
        schema = task["schema"]

        q_fqn = qfqn(db_name, schema, short_name)
        try:
            res = session.sql(f"SELECT GET_DDL('TASK', '{q_fqn}') as DDL").collect()
            ddl_text = res[0]["DDL"]
        except Exception as e:
            add(schema, "TASK", short_name, "ERROR", str(e))
            continue

        ddl_text = normalize_define_keyword(to_define(ddl_text))
        # GET_DDL renders the task name unqualified; qualify the header name.
        ddl_text = qualify_header(ddl_text, short_name, q_fqn, "TASK")
        # Qualify unqualified same-schema references in the task body.
        ddl_text = fqn_expand(ddl_text, schema)
        # Preserve a currently-running task's state (STARTED before AS).
        ddl_text = insert_target_state(ddl_text, task["state"], "as")

        emit(schema, "TASK", short_name, ddl_text)

    # 3c. Generate DEFINE statements for functions and procedures via GET_DDL
    for c in callable_list:
        short_name = c["name"]
        schema = c["schema"]
        domain = c["domain"]

        q_base = qfqn(db_name, schema, short_name)
        sig_for_ddl = f"{q_base}{extract_arg_signature(c['arguments'])}"

        try:
            res = session.sql(f"SELECT GET_DDL('{domain}', '{sig_for_ddl}') as DDL").collect()
            ddl_text = res[0]["DDL"]
        except Exception as e:
            add(schema, domain, short_name, "ERROR", str(e))
            continue

        ddl_text = normalize_define_keyword(to_define(ddl_text))

        # Rewrite the DEFINE header's quoted name to a quoted FQN so special
        # characters and reserved words in identifiers are preserved.
        ddl_text = ddl_text.replace(qi(short_name), q_base, 1)
        # Do not fqn_expand the body: it would corrupt column aliases and
        # parameter names that match object names.

        emit(schema, domain, short_name, ddl_text)

    # 3d. Generate DEFINE statements for sequences, file formats, alerts, and tags
    for obj in simple_ddl_list:
        short_name = obj["name"]
        schema = obj["schema"]
        domain = obj["domain"]

        q_fqn = qfqn(db_name, schema, short_name)
        try:
            res = session.sql(f"SELECT GET_DDL('{domain}', '{q_fqn}', TRUE) as DDL").collect()
            ddl_text = res[0]["DDL"]
        except Exception as e:
            add(schema, domain, short_name, "ERROR", str(e))
            continue

        ddl_text = normalize_define_keyword(to_define(ddl_text))
        # NOTE: GET_DDL already returns a qualified name for simple types
        # (sequence/file format/alert/tag), so no header rewrite is needed here.
        ddl_text = fqn_expand(ddl_text, schema)

        # Generic gap-fill from DESCRIBE AS RESOURCE (e.g. sequence COMMENT). Skip
        # ALERT (AS-bodied: appended params would land after the THEN action).
        if domain in ("SEQUENCE", "FILE_FORMAT", "TAG"):
            _res = describe_as_resource(domain.replace("_", " "), q_fqn)
            ddl_text, _filled = resource_gap_fill(ddl_text, _res)
            if _filled:
                add(schema, domain, short_name, "INFO",
                    f"gap-filled from DESCRIBE AS RESOURCE: {', '.join(_filled)}")

        if domain == "ALERT":
            # GET_DDL('ALERT') omits the COMMENT; insert it in the header,
            # before IF, since the generic gap-fill (append before trailing ';')
            # would land it after the THEN action instead.
            if obj.get("comment") and "COMMENT" not in ddl_text.upper():
                ddl_text = insert_before_keyword(ddl_text, f"    COMMENT = '{esc(obj['comment'])}'\n", "if")
                add(schema, domain, short_name, "INFO", "gap-filled COMMENT (not exposed by GET_DDL for ALERT)")
            # Preserve a currently-running alert's state (STARTED before IF).
            ddl_text = insert_target_state(ddl_text, obj.get("state", ""), "if")

        emit(schema, domain, short_name, ddl_text)

    # 3e. Generate DEFINE STAGE statements from SHOW STAGES metadata
    for stg in stage_list:
        short_name = stg["name"]
        schema = stg["schema"]
        parts = [f"DEFINE STAGE {qfqn(db_name, schema, short_name)}"]
        # External stage backed by a storage integration (URL + integration only;
        # never inline credentials - those are filtered out at discovery).
        if stg.get("url"):
            parts.append(f"    URL = '{stg['url']}'")
            parts.append(f"    STORAGE_INTEGRATION = {stg['storage_integration']}")
        if stg["directory_enabled"] == "Y":
            parts.append("    DIRECTORY = ( ENABLE = TRUE )")
        if stg["comment"]:
            parts.append(f"    COMMENT = '{esc(stg['comment'])}'")

        emit(schema, "STAGE", short_name, "\n".join(parts) + ";")

    # 3g. Generate DEFINE statements for masking and authentication policies
    for pol in policy_list:
        short_name = pol["name"]
        schema = pol["schema"]
        domain = pol["domain"]

        q_fqn = qfqn(db_name, schema, short_name)
        try:
            res = session.sql(f"SELECT GET_DDL('{pol['get_ddl_domain']}', '{q_fqn}') as DDL").collect()
            ddl_text = res[0]["DDL"]
        except Exception as e:
            add(schema, domain, short_name, "ERROR", str(e))
            continue

        ddl_text = normalize_define_keyword(to_define(ddl_text))
        # GET_DDL renders the policy name unqualified; qualify the header name.
        ddl_text = qualify_header(ddl_text, short_name, q_fqn, "POLICY")
        # Policy bodies are not fqn_expanded: like functions, they contain typed
        # argument names / expressions where blanket qualification is unsafe.

        # GET_DDL('POLICY') omits the policy COMMENT; append it from SHOW metadata
        # so the object adopts without a comment diff.
        ddl_text, _ = resource_gap_fill(ddl_text, {"COMMENT": pol["comment"]})

        emit(schema, domain, short_name, ddl_text)

    # 3h. Generate DEFINE PIPE statements via GET_DDL (body references qualified
    #     like tasks; header name rendered unqualified so it is re-qualified).
    for pipe in pipe_list:
        short_name = pipe["name"]
        schema = pipe["schema"]

        q_fqn = qfqn(db_name, schema, short_name)
        try:
            res = session.sql(f"SELECT GET_DDL('PIPE', '{q_fqn}') as DDL").collect()
            ddl_text = res[0]["DDL"]
        except Exception as e:
            add(schema, "PIPE", short_name, "ERROR", str(e))
            continue

        ddl_text = normalize_define_keyword(to_define(ddl_text))
        # GET_DDL renders the pipe name unqualified; qualify the header name.
        ddl_text = qualify_header(ddl_text, short_name, q_fqn, "PIPE")
        # Qualify unqualified same-schema references in the COPY INTO body.
        ddl_text = fqn_expand(ddl_text, schema)

        emit(schema, "PIPE", short_name, ddl_text)

    # 3i. Generate DEFINE STREAM statements from SHOW STREAMS metadata.
    #     A stream persists only its source object, APPEND_ONLY / INSERT_ONLY,
    #     and its comment. SHOW_INITIAL_ROWS and the AT/BEFORE point-of-time are
    #     creation-time behaviors that Snowflake does not retain (DESCRIBE AS
    #     RESOURCE reports both as null), so there is nothing to carry across.
    for stm in stream_list:
        short_name = stm["name"]
        schema = stm["schema"]

        # SHOW's source_type maps directly onto the ON clause keyword:
        # Table, View, Dynamic Table, External Table, Event Table, Stage.
        src_kind = stm["source_type"].upper().strip()
        if not src_kind:
            add(schema, "STREAM", short_name, "ERROR", "SHOW STREAMS returned no source_type")
            continue

        # table_name is already fully qualified but unquoted. Split off the
        # database and schema (both assumed to be ordinary unquoted names, as
        # elsewhere in this script) and leave the remainder as the object name,
        # so a name containing a dot survives.
        src_parts = stm["source_name"].split(".", 2)
        if len(src_parts) != 3:
            add(schema, "STREAM", short_name, "ERROR",
                f"cannot qualify stream source '{stm['source_name']}'")
            continue

        parts = [f"DEFINE STREAM {qfqn(db_name, schema, short_name)}"]
        parts.append(f"    ON {src_kind} {qfqn(*src_parts)}")
        mode = stm["mode"].upper().strip()
        if mode == "APPEND_ONLY":
            parts.append("    APPEND_ONLY = TRUE")
        elif mode == "INSERT_ONLY":
            parts.append("    INSERT_ONLY = TRUE")
        if stm["comment"]:
            parts.append(f"    COMMENT = '{esc(stm['comment'])}'")

        emit(schema, "STREAM", short_name, "\n".join(parts) + ";")

    # 3f. Write grouped files and resolve paths
    written_files = []
    if grouped_ddl:
        group_paths = {}
        for (schema, folder), ddl_list in grouped_ddl.items():
            path = write_local([db_name, schema, f"{folder}.sql"], "\n\n".join(ddl_list))
            group_paths[(schema, folder)] = path
            written_files.append(Path(path))
        for r in results:
            if isinstance(r["file_path"], tuple):
                r["file_path"] = group_paths[r["file_path"]]

    session.close()

    # 4. Result model
    if not results:
        results.append({
            "schema": "", "object_type": "", "object_name": "",
            "status": "NONE", "file_path": "No files generated",
        })
        print(json.dumps(results, indent=2))
        print("\nSummary: no files generated", file=sys.stderr)
        return

    # 4a. Display multi-word kinds with spaces (FILE_FORMAT -> "FILE FORMAT",
    #     DATA_METRIC_FUNCTION -> "DATA METRIC FUNCTION", etc.).
    for r in results:
        if r["object_type"]:
            r["object_type"] = r["object_type"].replace("_", " ")

    # 4b. Sort: ERROR first, then UNSUPPORTED, then SAVED, then everything else.
    #     Within each status group, sort by schema then object name.
    status_order = {"ERROR": 0, "UNSUPPORTED": 1, "SAVED": 2}
    results.sort(key=lambda r: (status_order.get(r["status"], 99), r["schema"] or "", r["object_name"] or ""))

    # 4c. Build summary rows: one row per distinct status with counts.
    status_counts = {}
    for r in results:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1
    total = sum(status_counts.values())

    summary_rows = [{
        "schema": "", "object_type": "SUMMARY", "object_name": "TOTAL",
        "status": str(total), "file_path": f"{total} objects processed",
    }]
    for status in ("ERROR", "UNSUPPORTED", "SAVED"):
        if status in status_counts:
            summary_rows.append({
                "schema": "", "object_type": "SUMMARY", "object_name": status,
                "status": str(status_counts[status]), "file_path": "",
            })
    for status, count in status_counts.items():
        if status not in ("ERROR", "UNSUPPORTED", "SAVED"):
            summary_rows.append({
                "schema": "", "object_type": "SUMMARY", "object_name": status,
                "status": str(count), "file_path": "",
            })

    print(json.dumps(summary_rows + results, indent=2))

    # BACKFILL FROM references that could not be qualified need manual attention.
    warnings = []
    for fpath in written_files:
        if not fpath.exists():
            continue
        content = fpath.read_text(encoding="utf-8")
        for m in re.finditer(r"BACKFILL\s+FROM\s+(\w+)", content, re.IGNORECASE):
            ref = m.group(1)
            if "." not in ref:
                warnings.append(
                    f"BACKFILL FROM {ref} in {fpath.name} - bare name not FQN-expanded (object may not exist in scan)"
                )

    parts = [f"{status_counts.get(s, 0)} {s.lower()}" for s in ("SAVED", "ERROR", "UNSUPPORTED", "INFO") if s in status_counts]
    print(f"\nSummary: {total} rows ({', '.join(parts)})", file=sys.stderr)
    if role_filter:
        print(f"  Role filter: {matched_object_count} of {total_object_count} objects matched role {role_filter}", file=sys.stderr)
    for w in warnings:
        print(f"  WARNING: {w}", file=sys.stderr)


if __name__ == "__main__":
    main()
