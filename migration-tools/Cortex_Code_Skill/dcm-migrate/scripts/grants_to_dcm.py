#!/usr/bin/env python3
"""
grants_to_dcm.py - Convert existing Snowflake roles and grants to DCM DEFINE/GRANT syntax.

Companion to ddl_to_dcm.py, which covers object structure only. Point this at an
ACCOUNT, DATABASE, or SCHEMA scope and it writes DEFINE ROLE / DEFINE DATABASE ROLE
statements plus the GRANT statements needed to reproduce the caller's role setup.

Only roles OWNED by the calling role, and only grants MADE by the calling role
(granted_by = CURRENT_ROLE()), are emitted. Grants made by another role are
reported as UNSUPPORTED, naming the granter.

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/grants_to_dcm.py \
        --scope-type ACCOUNT|DATABASE|SCHEMA \
        [--scope-name <DB> | <DB>.<SCHEMA>] \
        --output-path <OUTPUT_DIR> \
        [--consolidate-inherited] \
        [--connection <CONNECTION_NAME>]
"""

import argparse
import json
import os
import sys
from pathlib import Path

from snowflake.snowpark import Session


TARGET_KW = {"ROLE": "ROLE", "DATABASE_ROLE": "DATABASE ROLE", "USER": "USER", "SHARE": "SHARE"}
EXCLUDED_INH_TYPES = {"SHARE", "INTEGRATION", "APPLICATION", "APPLICATION_PACKAGE", "ORGANIZATION"}


def get_session(connection_name):
    return Session.builder.config("connection_name", connection_name).create()


def qi(ident):
    return '"' + str(ident).replace('"', '""') + '"'


def qfqn(*parts):
    return ".".join(qi(p) for p in parts)


def split_fqn(s):
    parts, cur, inq = [], "", False
    for ch in str(s):
        if ch == '"':
            inq = not inq
            cur += ch
        elif ch == "." and not inq:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    parts.append(cur)
    return parts


def requote(name_str):
    out = []
    for p in split_fqn(name_str):
        p = p.strip()
        if len(p) >= 2 and p.startswith('"') and p.endswith('"'):
            out.append(p)
        else:
            out.append(qi(p))
    return ".".join(out)


def esc(s):
    return (s or "").replace("'", "''")


def ntype(t):
    return (t or "").upper().replace(" ", "_")


def plural(t):
    return (t or "").replace("_", " ") + "S"


def emit_error(msg):
    print(json.dumps([{
        "schema": "", "object_type": "SCOPE", "object_name": "",
        "status": "ERROR", "file_path": msg,
    }], indent=2))
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Convert Snowflake roles and grants to DCM syntax")
    parser.add_argument("--scope-type", required=True, help="ACCOUNT, DATABASE, or SCHEMA")
    parser.add_argument("--scope-name", default=None, help="Omit for ACCOUNT; DB for DATABASE; DB.SCHEMA for SCHEMA")
    parser.add_argument("--output-path", required=True, help="Local output directory for generated files")
    parser.add_argument("--consolidate-inherited", action="store_true",
                        help="Collapse full ON ALL + ON FUTURE coverage into GRANT INHERITED (requires FEATURE_RBAC_INHERITED_GRANTS)")
    parser.add_argument("--connection", default=None, help="Snowflake connection name")
    args = parser.parse_args()

    output_dir = args.output_path
    consolidate_inherited = args.consolidate_inherited
    connection_name = args.connection or os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default_connection_name"

    # --- parse scope ---
    level = (args.scope_type or "").upper().strip()
    scope_db = None
    scope_schema = None
    if level == "ACCOUNT":
        pass
    elif level == "DATABASE":
        if not args.scope_name:
            emit_error("--scope-name (database) required for DATABASE scope")
        scope_db = split_fqn(args.scope_name)[0].strip().strip('"')
    elif level == "SCHEMA":
        parts = split_fqn(args.scope_name or "")
        if len(parts) < 2:
            emit_error("--scope-name must be 'DB.SCHEMA' for SCHEMA scope")
        scope_db = parts[0].strip().strip('"')
        scope_schema = parts[1].strip().strip('"').upper()
    else:
        emit_error(f"--scope-type must be ACCOUNT | DATABASE | SCHEMA (got '{args.scope_type}')")

    session = get_session(connection_name)
    caller_role = (session.sql("SELECT CURRENT_ROLE()").collect()[0][0] or "").upper()

    results = []
    role_lines = []
    seen = set()

    # (db_or_None, schema_or_None) -> {'grants': [stmt, ...], 'role_grants': [stmt, ...]}
    # Container is derived from each grant's own object name (see container_for),
    # not from scope_type, so the same object always lands in the same file
    # regardless of which scope produced it.
    containers = {}

    obj_grants = []      # per-object privilege grants (for consolidation)
    future_grants = []   # future grants granted by caller (for consolidation)
    current_objs = {}    # (schema|'*DB*', TYPE) -> set(quoted FQN)

    def add(schema, object_type, object_name, status, file_path):
        results.append({
            "schema": schema, "object_type": object_type, "object_name": object_name,
            "status": status, "file_path": file_path,
        })

    def add_grant(stmt, container, kind="grants"):
        if stmt and stmt not in seen:
            seen.add(stmt)
            containers.setdefault(container, {"grants": [], "role_grants": []})[kind].append(stmt)

    unsupported = {}  # category -> count (aggregated instead of one row per object)

    def mark_unsupported(cat):
        unsupported[cat] = unsupported.get(cat, 0) + 1

    def container_for(granted_on, name):
        # Container is derived purely from the object's own qualified name, so
        # it's the same regardless of scope_type. 1-part names with no db/schema
        # qualifier (WAREHOUSE, INTEGRATION, ...) land at the account level.
        got = ntype(granted_on)
        parts = [p.strip().strip('"') for p in split_fqn(name or "") if p.strip()]
        if got == "DATABASE":
            return (parts[0], None) if parts else (None, None)
        if got == "SCHEMA":
            if len(parts) >= 2:
                return (parts[0], parts[1])
            return (parts[0], None) if parts else (None, None)
        if len(parts) >= 3:
            return (parts[0], parts[1])
        return (None, None)

    def container_rel_parts(container):
        db, schema = container
        if db is None:
            return ["_account"]
        if schema is None:
            return [db]
        return [db, schema]

    def container_label(container):
        db, schema = container
        return schema or db or ""

    def write_local(rel_parts, content):
        path = Path(output_dir).joinpath(*rel_parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return str(path)

    def gid_of(granted_to, grantee_raw):
        bare = split_fqn(grantee_raw)[-1].strip().strip('"').upper()
        return (granted_to, bare)

    def make_grantee(granted_to, grantee_raw):
        kw = TARGET_KW.get(granted_to)
        if kw is None:
            return None, None
        if granted_to == "DATABASE_ROLE" and len(split_fqn(grantee_raw)) == 1 and scope_db:
            return kw, qfqn(scope_db, grantee_raw.strip().strip('"'))
        return kw, requote(grantee_raw)

    def priv_grant(row):
        d = row.as_dict()
        # Role memberships surface here as USAGE / granted_on=ROLE|DATABASE_ROLE;
        # they are emitted by membership_grant (from SHOW GRANTS OF). Emitting them
        # here would produce the invalid 'GRANT USAGE ON [DATABASE] ROLE'.
        if ntype(d.get("granted_on")) in ("ROLE", "DATABASE_ROLE"):
            return
        # Account-level grants (privileges granted ON ACCOUNT) are ignored.
        if ntype(d.get("granted_on")) == "ACCOUNT":
            mark_unsupported("account-level grant (ignored)")
            return
        priv = d.get("privilege")
        if priv == "OWNERSHIP":
            mark_unsupported("OWNERSHIP (skipped)")
            return
        gb = (d.get("granted_by") or "").upper()
        if gb != caller_role:
            # Only grants made by the calling role are emitted. Report grants made
            # by another named role; skip implicit/system grants (empty granter).
            if gb:
                mark_unsupported(f"grant by another role (not {caller_role})")
            return
        gt = d.get("granted_to")
        kw, grantee = make_grantee(gt, d.get("grantee_name") or "")
        if kw is None:
            mark_unsupported(f"{gt} grantee (unsupported)")
            return
        on_kw = (d.get("granted_on") or "").replace("_", " ")
        go = str(d.get("grant_option")).upper() == "TRUE"
        stmt = f"GRANT {priv} ON {on_kw} {requote(d.get('name'))} TO {kw} {grantee}"
        if go:
            stmt += " WITH GRANT OPTION"
        stmt += ";"
        add_grant(stmt, container_for(d.get("granted_on"), d.get("name")))
        if consolidate_inherited:
            nm_parts = split_fqn(d.get("name") or "")
            og_schema = nm_parts[1].strip().strip('"').upper() if len(nm_parts) >= 3 else None
            obj_grants.append({
                "gid": gid_of(gt, d.get("grantee_name") or ""),
                "priv": priv, "type": ntype(d.get("granted_on")),
                "schema": og_schema, "fqn": requote(d.get("name")),
                "go": go, "stmt": stmt,
            })

    def membership_grant(row, role_kind_kw):
        d = row.as_dict()
        gb = (d.get("granted_by") or "").upper()
        if gb != caller_role:
            if gb:
                mark_unsupported(f"grant by another role (not {caller_role})")
            return
        kw = TARGET_KW.get(d.get("granted_to"))
        if kw is None:
            mark_unsupported(f"{d.get('granted_to')} grantee (unsupported)")
            return
        # DATABASE ROLE membership only ever arises for DATABASE-scope calls
        # (SCHEMA scope never populates owned_roles), so scope_db is always
        # the right container; account ROLE membership has none.
        container = (scope_db, None) if role_kind_kw == "DATABASE ROLE" else (None, None)
        add_grant(
            f"GRANT {role_kind_kw} {requote(d.get('role'))} TO {kw} {requote(d.get('grantee_name'))};",
            container, kind="role_grants",
        )

    def run_collect(cmd, fn, *fn_args):
        try:
            for r in session.sql(cmd).collect():
                fn(r, *fn_args)
        except Exception:
            pass

    def handle_future(cmd, ckind, cschema, src):
        try:
            rows = session.sql(cmd).collect()
        except Exception:
            return
        for r in rows:
            d = r.as_dict()
            # GRANT OWNERSHIP ON FUTURE is unsupported (DCM manages ownership
            # separately; inherited grants cannot carry OWNERSHIP). Flag in both modes.
            if (d.get("privilege") or "").upper() == "OWNERSHIP":
                mark_unsupported("OWNERSHIP ON FUTURE (skipped)")
                continue
            if consolidate_inherited:
                # SHOW FUTURE GRANTS columns differ (grant_to, grant_on) and have
                # NO granted_by. Capture all in-scope future grants; caller
                # attribution is enforced on the per-object (granted_by) side.
                future_grants.append({
                    "gid": gid_of(d.get("grant_to"), d.get("grantee_name") or ""),
                    "granted_to": d.get("grant_to"), "grantee_raw": d.get("grantee_name") or "",
                    "priv": d.get("privilege"), "type": ntype(d.get("grant_on")),
                    "ckind": ckind, "cschema": cschema, "src": src,
                })
            else:
                mark_unsupported("future grant (enable --consolidate-inherited to convert)")

    owned_roles = []

    # ============================================================
    # ROLES (scope-aligned)
    # ============================================================
    if level == "ACCOUNT":
        try:
            for r in session.sql("SHOW ROLES").collect():
                d = r.as_dict()
                if (d.get("owner") or "").upper() != caller_role:
                    continue
                nm = d["name"]
                s = f"DEFINE ROLE {qi(nm)}"
                if d.get("comment"):
                    s += f"\n    COMMENT = '{esc(d['comment'])}'"
                role_lines.append(s + ";")
                owned_roles.append(("ROLE", qi(nm)))
                add("", "ROLE", nm, "SAVED", "roles.sql")
        except Exception as e:
            add("", "ROLE", "*", "ERROR", str(e))
    elif level == "DATABASE":
        try:
            for r in session.sql(f"SHOW DATABASE ROLES IN DATABASE {qi(scope_db)}").collect():
                d = r.as_dict()
                if (d.get("owner") or "").upper() != caller_role:
                    continue
                nm = d["name"]
                fq = qfqn(scope_db, nm)
                s = f"DEFINE DATABASE ROLE {fq}"
                if d.get("comment"):
                    s += f"\n    COMMENT = '{esc(d['comment'])}'"
                role_lines.append(s + ";")
                owned_roles.append(("DATABASE ROLE", fq))
                add(scope_db, "DATABASE ROLE", nm, "SAVED", "roles.sql")
        except Exception as e:
            add(scope_db, "DATABASE ROLE", "*", "ERROR", str(e))

    for kind_kw, target in owned_roles:
        run_collect(f"SHOW GRANTS TO {kind_kw} {target}", priv_grant)
        run_collect(f"SHOW GRANTS OF {kind_kw} {target}", membership_grant, kind_kw)

    # ============================================================
    # OBJECT DISCOVERY + OBJECT-LEVEL GRANTS
    # ============================================================
    def extract_sig(name, arguments, s_name):
        base = qfqn(scope_db, s_name, name)
        if not arguments:
            return f"{base}()"
        start = arguments.find("(")
        if start == -1:
            return f"{base}()"
        depth, end = 0, -1
        for i in range(start, len(arguments)):
            if arguments[i] == "(":
                depth += 1
            elif arguments[i] == ")":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        return f"{base}{arguments[start:end + 1]}" if end != -1 else f"{base}()"

    def record_current(s_name, on_kw, name):
        current_objs.setdefault((s_name, ntype(on_kw)), set()).add(qfqn(scope_db, s_name, name))

    def scan_objects(schemas):
        semantic = set()
        try:
            for r in session.sql(f"SHOW SEMANTIC VIEWS IN DATABASE {qi(scope_db)}").collect():
                semantic.add(f"{r['schema_name'].upper()}.{r['name']}")
        except Exception:
            pass
        try:
            for r in session.sql(f"SHOW OBJECTS IN DATABASE {qi(scope_db)}").collect():
                s_name = r["schema_name"].upper()
                if s_name not in schemas:
                    continue
                kind = (r["kind"] or "").upper()
                if kind in ("STREAM", "STAGE") or "SEMANTIC" in kind:
                    continue
                if f"{s_name}.{r['name']}" in semantic:
                    continue
                on_kw = "TABLE" if kind in ("TABLE", "DYNAMIC_TABLE") else kind.replace("_", " ")
                record_current(s_name, on_kw, r["name"])
                run_collect(f"SHOW GRANTS ON {on_kw} {qfqn(scope_db, s_name, r['name'])}", priv_grant)
        except Exception as e:
            add(scope_db, "OBJECTS", "*", "ERROR", str(e))

        per_schema = [
            ("SEQUENCE", "SHOW SEQUENCES"), ("FILE FORMAT", "SHOW FILE FORMATS"),
            ("ALERT", "SHOW ALERTS"), ("TAG", "SHOW TAGS"), ("TASK", "SHOW TASKS"),
            ("PIPE", "SHOW PIPES"), ("STREAM", "SHOW STREAMS"),
            ("MASKING POLICY", "SHOW MASKING POLICIES"),
            ("AUTHENTICATION POLICY", "SHOW AUTHENTICATION POLICIES"),
            ("NETWORK RULE", "SHOW NETWORK RULES"),
        ]
        for s_name in schemas:
            for on_kw, show in per_schema:
                try:
                    for r in session.sql(f"{show} IN SCHEMA {qfqn(scope_db, s_name)}").collect():
                        record_current(s_name, on_kw, r["name"])
                        run_collect(f"SHOW GRANTS ON {on_kw} {qfqn(scope_db, s_name, r['name'])}", priv_grant)
                except Exception:
                    pass
            try:
                for r in session.sql(f"SHOW STAGES IN SCHEMA {qfqn(scope_db, s_name)}").collect():
                    if (r["type"] or "").upper().find("TEMPORARY") >= 0:
                        continue
                    run_collect(f"SHOW GRANTS ON STAGE {qfqn(scope_db, s_name, r['name'])}", priv_grant)
            except Exception:
                pass
            for domain, show in (("FUNCTION", "SHOW USER FUNCTIONS"), ("PROCEDURE", "SHOW USER PROCEDURES")):
                try:
                    for r in session.sql(f"{show} IN SCHEMA {qfqn(scope_db, s_name)}").collect():
                        d = r.as_dict()
                        if d["name"].upper() in ("DDL_TO_DCM_DEFINITIONS", "GRANTS_TO_DCM_DEFINITIONS", "GENERATE_DEFINITIONS"):
                            continue
                        run_collect(
                            f"SHOW GRANTS ON {domain} {extract_sig(d['name'], d.get('arguments', ''), s_name)}",
                            priv_grant,
                        )
                except Exception:
                    pass

    if level == "ACCOUNT":
        run_collect("SHOW GRANTS ON ACCOUNT", priv_grant)
    elif level == "DATABASE":
        run_collect(f"SHOW GRANTS ON DATABASE {qi(scope_db)}", priv_grant)
        handle_future(f"SHOW FUTURE GRANTS IN DATABASE {qi(scope_db)}", "DATABASE", None, scope_db)
        schemas = set()
        try:
            for r in session.sql(f"SHOW SCHEMAS IN DATABASE {qi(scope_db)}").collect():
                s = r["name"].upper()
                if s != "INFORMATION_SCHEMA":
                    schemas.add(s)
                    current_objs.setdefault(("*DB*", "SCHEMA"), set()).add(qfqn(scope_db, s))
        except Exception as e:
            add(scope_db, "SCHEMAS", "*", "ERROR", str(e))
        for s_name in schemas:
            run_collect(f"SHOW GRANTS ON SCHEMA {qfqn(scope_db, s_name)}", priv_grant)
            handle_future(f"SHOW FUTURE GRANTS IN SCHEMA {qfqn(scope_db, s_name)}", "SCHEMA", s_name, s_name)
        scan_objects(schemas)
    elif level == "SCHEMA":
        run_collect(f"SHOW GRANTS ON SCHEMA {qfqn(scope_db, scope_schema)}", priv_grant)
        handle_future(f"SHOW FUTURE GRANTS IN SCHEMA {qfqn(scope_db, scope_schema)}", "SCHEMA", scope_schema, scope_schema)
        scan_objects({scope_schema})

    # ============================================================
    # FINAL STEP: consolidate ON ALL + ON FUTURE -> INHERITED
    # ============================================================
    if consolidate_inherited:
        suppressed = set()
        inherited = []  # (container, stmt)
        for fg in future_grants:
            Y = fg["type"]
            if fg["priv"] == "OWNERSHIP" or Y in EXCLUDED_INH_TYPES:
                add("", "GRANT", f"FUTURE {Y} -> {fg['grantee_raw']}", "UNSUPPORTED",
                    "future grant (ineligible for inherited)")
                continue
            kw, grantee = make_grantee(fg["granted_to"], fg["grantee_raw"])
            if kw is None:
                continue
            if fg["ckind"] == "SCHEMA":
                s = fg["cschema"]
                current = set(current_objs.get((s, Y), set()))
                covered = {g["fqn"] for g in obj_grants
                           if g["type"] == Y and g["schema"] == s and g["gid"] == fg["gid"]
                           and g["priv"] == fg["priv"] and not g["go"]}
                container_sql = f"SCHEMA {qfqn(scope_db, s)}"

                def match(g, s=s, Y=Y, fg=fg):
                    return (g["type"] == Y and g["schema"] == s and g["gid"] == fg["gid"]
                            and g["priv"] == fg["priv"] and not g["go"])

                inh_container = (scope_db, s)
            else:  # DATABASE container
                if Y == "SCHEMA":
                    current = set(current_objs.get(("*DB*", "SCHEMA"), set()))
                else:
                    current = set()
                    for (k_s, k_t), vals in current_objs.items():
                        if k_t == Y and k_s != "*DB*":
                            current |= vals
                covered = {g["fqn"] for g in obj_grants
                           if g["type"] == Y and g["gid"] == fg["gid"]
                           and g["priv"] == fg["priv"] and not g["go"]}
                container_sql = f"DATABASE {qi(scope_db)}"

                def match(g, Y=Y, fg=fg):
                    return (g["type"] == Y and g["gid"] == fg["gid"]
                            and g["priv"] == fg["priv"] and not g["go"])

                inh_container = (scope_db, None)
            if current and current <= covered:
                inh = f"GRANT INHERITED {fg['priv']} ON ALL {plural(Y)} IN {container_sql} TO {kw} {grantee};"
                inherited.append((inh_container, inh))
                for g in obj_grants:
                    if match(g):
                        suppressed.add(g["stmt"])
                add(fg.get("cschema") or scope_db or "", "INHERITED GRANT",
                    f"{fg['priv']} ON ALL {plural(Y)}", "SAVED", container_sql)
            else:
                mark_unsupported("future grant (not collapsible - partial coverage)")
        if suppressed:
            for cdata in containers.values():
                cdata["grants"] = [s for s in cdata["grants"] if s not in suppressed]
        for inh_container, inh in inherited:
            add_grant(inh, inh_container)

    # ============================================================
    # WRITE FILES
    # ============================================================
    if level == "ACCOUNT":
        roles_base = ["_account"]
    elif level == "DATABASE":
        roles_base = [scope_db]
    else:
        roles_base = None  # SCHEMA scope never owns roles

    if role_lines and roles_base:
        path = write_local(roles_base + ["roles.sql"], "\n\n".join(role_lines))
        add(scope_db or "ACCOUNT", "ROLES", "ROLES", "SAVED", path)
        for r in results:
            if r["file_path"] == "roles.sql":
                r["file_path"] = path

    total_grants = 0
    for container in sorted(containers.keys(), key=lambda c: (c[0] or "", c[1] or "")):
        cdata = containers[container]
        base = container_rel_parts(container)
        label = container_label(container) or "ACCOUNT"
        if cdata["grants"]:
            total_grants += len(cdata["grants"])
            path = write_local(base + ["grants.sql"], "\n".join(cdata["grants"]))
            add(label, "GRANTS", "GRANTS", "SAVED", path)
        if cdata["role_grants"]:
            total_grants += len(cdata["role_grants"])
            path = write_local(base + ["role_grants.sql"], "\n".join(cdata["role_grants"]))
            add(label, "ROLE GRANTS", "ROLE_GRANTS", "SAVED", path)

    session.close()

    # ============================================================
    # SUMMARY
    # ============================================================
    # Aggregate unsupported categories into a single row each (with a count),
    # instead of one row per object.
    for cat, cnt in sorted(unsupported.items()):
        add("", "UNSUPPORTED", cat, "UNSUPPORTED", f"{cnt} occurrence(s)")

    if not results:
        print(json.dumps([{
            "schema": "", "object_type": "", "object_name": "", "status": "NONE",
            "file_path": f"No roles/grants for {caller_role} at {level} scope",
        }], indent=2))
        print(f"\nSummary: no roles/grants for {caller_role} at {level} scope", file=sys.stderr)
        return

    status_order = {"ERROR": 0, "UNSUPPORTED": 1, "SAVED": 2}
    results.sort(key=lambda r: (status_order.get(r["status"], 99), r["object_type"] or "",
                                r["schema"] or "", r["object_name"] or ""))
    counts = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    summary_rows = [{
        "schema": "", "object_type": "SUMMARY", "object_name": f"{level} / CALLER={caller_role}",
        "status": str(total_grants), "file_path": "grant statements",
    }]
    for st in ("ERROR", "UNSUPPORTED", "SAVED"):
        if st in counts:
            summary_rows.append({
                "schema": "", "object_type": "SUMMARY", "object_name": st,
                "status": str(counts[st]), "file_path": "",
            })

    print(json.dumps(summary_rows + results, indent=2))
    print(f"\nSummary: {total_grants} grant statements, caller role {caller_role}, {level} scope", file=sys.stderr)
    for st in ("ERROR", "UNSUPPORTED", "SAVED"):
        if st in counts:
            print(f"  {st}: {counts[st]}", file=sys.stderr)


if __name__ == "__main__":
    main()
