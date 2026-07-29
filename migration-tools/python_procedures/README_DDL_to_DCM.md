# DDL to DCM Migration Procedures

This folder has two companion procedures for bringing an existing Snowflake objects and grants under
[DCM Project management](https://docs.snowflake.com/en/user-guide/database-change-management):

- **`DDL_TO_DCM_DEFINITIONS`** — migrates object structure (tables, views, tasks, and so on). Covered below.
- **`GRANTS_TO_DCM_DEFINITIONS`** — migrates roles and grants. Covered in its own section further down.

Run them separately; the outputs land in different files (`roles.sql` / `grants.sql` vs. the object
definition files) so they can be adopted independently.

---

## DDL_TO_DCM_DEFINITIONS

`DDL_TO_DCM_DEFINITIONS` brings a database that was built manually (or by another tool) under
[Database Change Management (DCM)](https://docs.snowflake.com/en/user-guide/database-change-management)
— without recreating anything. Point it at a database and it produces ready-to-use DCM `DEFINE`
files for the objects it contains.

**The outcome:** on your first `snow dcm plan`, every existing object shows up as a clean
**adoption** — DCM recognizes the object you already have and simply attaches it to your project,
with **no property drift and no rebuilds**. Your live objects keep working exactly as they are; you
just gain declarative, version-controlled management going forward.

It also protects you from the usual migration papercuts:

- **Nothing silently changes.** Storage settings, comments, clustering, and similar properties are
  carried across, so adoption doesn't quietly reset a table's retention or drop a comment.
- **Running tasks and alerts stay running.** Objects that are currently started are kept started on
  deploy, instead of being suspended.
- **Secrets are never written out.** Credential-bearing objects are reported rather than emitted, so
  no keys or tokens end up in your definition files.
- **One bad object won't block the rest.** Anything that can't be migrated is clearly reported so you
  can handle it, rather than failing the whole run.

---

### What gets migrated

| Category | Types |
|---|---|
| Structure | Database, Schemas, Tables, Views, Dynamic Tables |
| Programmatic | Tasks, Functions, Procedures (including overloads) |
| Ingestion | Pipes |
| Utility | Sequences, File Formats, Alerts, Tags |
| Governance | Masking Policies, Authentication Policies |
| Storage | Internal Stages; External Stages backed by a storage integration |

Every type can be migrated selectively via `object_type_allow_list` — see [Parameters](#parameters) below.

### Reported instead of migrated

These are surfaced in the result table (as `UNSUPPORTED` or `INFO`) so you can act on them — they are
never emitted in a way that would be unsafe or fail to deploy:

- **Grants, roles, and tag/policy attachments** — out of scope here (this procedure covers object
  structure); policy and tag *objects* themselves are migrated, just not their associations. Roles
  and grants are covered by `GRANTS_TO_DCM_DEFINITIONS`, below.
- **External stages with embedded credentials** — flagged rather than emitted, to keep secrets out of
  definition files. Use a storage integration instead (those are fully supported).
- **Data Metric Functions** and **Semantic Views** — flagged as unsupported.
- **Streams** and **temporary stages** — skipped.

### Good to know

- A stage's inline file-format / copy options can't be read back from Snowflake, so stages are
  migrated without them — re-add those by hand if you depend on them.
- Databases and schemas are expected to have ordinary (unquoted) names.
- A view written with a trailing semicolon or unqualified references may show a harmless one-time
  difference on the first plan that clears itself after the first deploy.

---

### Parameters

```sql
DDL_TO_DCM_DEFINITIONS(
    db_name                STRING,   -- database to migrate
    schema_allow_list      ARRAY,    -- schemas to include, or NULL for all
    object_type_allow_list ARRAY,    -- object types to include, or NULL for all
    output_path            STRING    -- target stage / workspace folder
)
RETURNS TABLE (SCHEMA, OBJECT_TYPE, OBJECT_NAME, STATUS, FILE_PATH)
```

| Parameter | Description |
|---|---|
| `db_name` | Database whose objects are migrated. |
| `schema_allow_list` | An `ARRAY` of schema names to limit the scan to (e.g. `['RAW','SERVE']`), or `NULL` for **all** schemas. |
| `object_type_allow_list` | An `ARRAY` of object type names to limit the migration to (e.g. `['TABLE','VIEW']`), or `NULL`/empty for **all** types. Case-, space-, and underscore-insensitive (`'file format'` = `'FILE_FORMAT'`). Includes `'SCHEMA'` and `'DATABASE'` as valid tokens (see below). An unrecognized name is reported as an `ERROR` row rather than failing the call. |
| `output_path` | Where the files are written — a stage path (`@my_db.my_schema.my_stage/dcm`) or a Snowflake Workspace path. |

When `'DATABASE'` is included (the default, since `NULL` means all types), the procedure also emits a
`DEFINE DATABASE <db_name>` statement — including its comment, if any — into `schemas.sql`, ahead of the
schema definitions.

**Prerequisites:** a role that can see the objects (the procedure runs with your privileges), write
access to `output_path`, and a running warehouse.

---

### How to use it

**1. Create the procedure** — run the `CREATE OR REPLACE PROCEDURE` statement in this file once.

**2. Generate the definition files:**
```sql
CALL DDL_TO_DCM_DEFINITIONS(
    'MY_DATABASE',                              -- database to migrate
    NULL,                                       -- all schemas (or e.g. ['RAW','SERVE'])
    NULL,                                       -- all object types (or e.g. ['TABLE','VIEW'])
    '@MY_DATABASE.PUBLIC.DCM_STAGE/migration'   -- target folder
);
```

**3. Review the result table** — each object reports a status:

| STATUS | Meaning |
|---|---|
| `SAVED` | Definition file written. |
| `INFO` | Advisory (e.g. an attachment was skipped, or a property was preserved). |
| `UNSUPPORTED` | Not migrated — see the message for why. |
| `ERROR` | Couldn't be read; message in `FILE_PATH`. |
| `SUMMARY` | Roll-up counts at the top. |

Handle any `ERROR` / `UNSUPPORTED` rows, then:

**4. Adopt into a DCM project** — point a project's manifest at the generated folder and run:
```bash
snow dcm plan   <project>   # existing objects should show ONLY "set PROJECT"
snow dcm deploy <project>   # brings them under DCM management
```

A clean plan (every object = adoption with no other changes) confirms the definitions match your live
objects exactly.

---

### Output layout

One file per object type per schema, always (there is no per-object-file mode):

```
<output_path>/<DB>/schemas.sql                  # DEFINE DATABASE + DEFINE SCHEMA statements
<output_path>/<DB>/<SCHEMA>/tables.sql
<output_path>/<DB>/<SCHEMA>/views.sql
...
```

Folders/files by type: `tables`, `views`, `dynamic_tables`, `tasks`, `functions`, `procedures`,
`sequences`, `file_formats`, `alerts`, `tags`, `policies`, `pipes`, `stages`.

---

## GRANTS_TO_DCM_DEFINITIONS

`GRANTS_TO_DCM_DEFINITIONS` is the companion procedure for roles and grants — the piece
`DDL_TO_DCM_DEFINITIONS` explicitly leaves out. Point it at an ACCOUNT, DATABASE, or SCHEMA scope and
it produces DCM `DEFINE ROLE` / `DEFINE DATABASE ROLE` statements plus the `GRANT` statements needed
to reproduce the caller's role setup.

Only roles **owned by the calling role**, and only grants **made by the calling role**
(`granted_by = CURRENT_ROLE()`), are emitted. Grants made by another role are reported as
`UNSUPPORTED` (with the granter named) rather than silently skipped or emitted incorrectly.

### What gets migrated

- **Roles owned by the caller**, scoped to the chosen level:
  - `ACCOUNT` scope → account roles (`DEFINE ROLE`)
  - `DATABASE` scope → database roles in that database (`DEFINE DATABASE ROLE`)
  - `SCHEMA` scope → rolls up to the parent database (database roles aren't schema-scoped)
- **Grants made by the caller** within the chosen scope: privilege grants on objects, schemas,
  databases, or the account, plus role-membership grants (`GRANT ROLE ... TO ...`,
  `GRANT DATABASE ROLE ... TO ...`). See [How grants are organized](#how-grants-are-organized) for
  where each ends up.
- **Future grants**, when `consolidate_inherited = TRUE` (see below) — otherwise reported as
  `UNSUPPORTED` with a note to enable that option.

### Reported instead of migrated

- **`OWNERSHIP` grants** — DCM manages ownership separately; these are skipped and reported.
- **Account-level privilege grants** (`GRANT ... ON ACCOUNT`) — ignored and reported.
- **Grants made by another role** (`granted_by != CURRENT_ROLE()`) — reported as `UNSUPPORTED`,
  naming the granting role, so you know to re-run as that role if you want them captured.
- **Grantees the procedure can't target** (anything other than `ROLE`, `DATABASE ROLE`, `USER`, or
  `SHARE`) — reported as `UNSUPPORTED`.
- **Future grants without `consolidate_inherited`** — reported as `UNSUPPORTED` with a note to enable
  the option.

### Parameters

```sql
GRANTS_TO_DCM_DEFINITIONS(
    scope_type            STRING,           -- 'ACCOUNT' | 'DATABASE' | 'SCHEMA'
    scope_name            STRING,           -- NULL for ACCOUNT; 'MY_DB' or 'MY_DB.MY_SCHEMA' otherwise
    output_path           STRING,           -- target stage / workspace folder
    consolidate_inherited BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (SCHEMA, OBJECT_TYPE, OBJECT_NAME, STATUS, FILE_PATH)
```

| Parameter | Description |
|---|---|
| `scope_type` | `ACCOUNT`, `DATABASE`, or `SCHEMA`. Determines which roles are in scope and how grants are collected. |
| `scope_name` | `NULL` for `ACCOUNT` scope; the database name for `DATABASE` scope; `DB.SCHEMA` for `SCHEMA` scope. |
| `output_path` | Where `roles.sql` and `grants.sql` are written — a stage path or a Snowflake Workspace path. |
| `consolidate_inherited` | See [Consolidating future grants into `INHERITED`](#consolidating-future-grants-into-inherited) below. |

**Prerequisites:** run as the role whose roles/grants you want captured (roles and grants are filtered
to that role), write access to `output_path`, and a running warehouse.

### How to use it

**1. Create the procedure** — run the `CREATE OR REPLACE PROCEDURE` statement in
`GRANTS_TO_DCM_sproc.sql` once.

**2. Generate the definition files:**
```sql
CALL GRANTS_TO_DCM_DEFINITIONS('ACCOUNT',  NULL,              '@DB.SC.STG/run', FALSE);
CALL GRANTS_TO_DCM_DEFINITIONS('DATABASE', 'MY_DB',           '@DB.SC.STG/run', TRUE);
CALL GRANTS_TO_DCM_DEFINITIONS('SCHEMA',   'MY_DB.MY_SCHEMA', '@DB.SC.STG/run', TRUE);
```

**3. Review the result table** — same `STATUS` conventions as `DDL_TO_DCM_DEFINITIONS`
(`SAVED` / `UNSUPPORTED` / `ERROR` / `SUMMARY`), plus `INHERITED GRANT` rows when consolidation
kicks in.

**4. Adopt into a DCM project** — same as the DDL procedure: point the project's manifest at the
generated `roles.sql` / `grants.sql` / `role_grants.sql` files, then `snow dcm plan` / `snow dcm deploy`.

### How grants are organized

Unlike `roles.sql` (one file per scope), grants are split **by container**, derived from each grant's
own object name rather than from `scope_type` — so the layout is identical no matter which scope
produced it, and it mirrors `DDL_TO_DCM_DEFINITIONS`'s own folder structure:

```
<output_path>/_account/roles.sql              # ACCOUNT scope only
<output_path>/_account/role_grants.sql        # GRANT ROLE ... TO ...
<output_path>/_account/grants.sql             # grants on containerless objects (WAREHOUSE, etc.)
<output_path>/<DB>/roles.sql                  # DATABASE scope only
<output_path>/<DB>/role_grants.sql            # GRANT DATABASE ROLE ... TO ...
<output_path>/<DB>/grants.sql                 # grants ON DATABASE <DB> itself
<output_path>/<DB>/<SCHEMA>/grants.sql        # grants ON SCHEMA <DB>.<SCHEMA> + grants on objects inside it
```

A file is written only if it has content — e.g. `roles.sql` is skipped for a scope with no
caller-owned roles, and a schema with no caller grants gets no `grants.sql` at all. One consequence:
an `ACCOUNT`-scope call can produce `<DB>/...` files for *any* database an owned role happens to have
grants on, not just one — each grant is filed under the object it actually belongs to.

**Caveat:** because a file is only written when non-empty, a container whose grants later drop to
zero (e.g. everything revoked) leaves a stale file behind on re-run rather than being cleaned up —
review `UNSUPPORTED`/`SAVED` counts against your expectations if you're relying on this for drift
detection over time.

### Consolidating future grants into `INHERITED`

With `consolidate_inherited = TRUE`, the final step checks whether the caller granted a privilege on
**every current object of a type** in a container (schema or database) **and** a matching `FUTURE`
grant exists (same privilege, grantee, container). When both hold, the per-object grants and the
future grant are replaced by a single:

```sql
GRANT INHERITED <privilege> ON ALL <type> IN <container> TO <grantee>;
```

This requires `FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED'` to deploy. Coverage that's only partial
(a future grant without matching per-object grants on every current object) is left as `UNSUPPORTED`
rather than collapsed incorrectly. Some object types are never eligible for consolidation — `SHARE`,
`INTEGRATION`, `APPLICATION`, `APPLICATION_PACKAGE`, and `ORGANIZATION` — and `OWNERSHIP` future grants
are always reported rather than converted.
