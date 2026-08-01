# DDL&nbsp;→&nbsp;DCM Migration Procedures

Two companion stored procedures that bring existing Snowflake objects under
[Database Change Management (DCM)](https://docs.snowflake.com/en/user-guide/database-change-management)
without recreating or modifying them:

- **`DDL_TO_DCM_DEFINITIONS`** migrates object structure (tables, views, tasks, and so on).
- **`GRANTS_TO_DCM_DEFINITIONS`** migrates roles and grants.

> ℹ️ **In Preview**: These procedures are in preview and still evolving. Review the generated definitions and validate with PLAN before deploying to production.

Run them separately. Their outputs land in different files (`roles.sql` / `grants.sql` versus the object
definition files) so they can be adopted independently.

These procedures cover the **file generation step only**. They do not create a DCM project, and they do not
run ANALYZE, PLAN, or DEPLOY. If you have Cortex Code available, the [`dcm-migrate` skill](../Cortex_Code_Skill/)
wraps the same generation logic in the full workflow, including the review-and-refine loop against PLAN output
described in [Reviewing the plan](#reviewing-the-plan) below.

## What you get

Definition files that describe your existing objects, so DCM can **adopt** them: it recognizes the object you
already have and attaches it to your project rather than rebuilding it. Your live objects keep working exactly
as they are, and from that point on you manage them declaratively and under version control.

The procedures also handle the parts of a migration that usually go wrong:

- **Nothing silently changes.** Retention, comments, clustering, change tracking and similar properties are
  carried across, so adoption does not quietly reset a table setting or drop a comment.
- **Running tasks and alerts stay running.** Objects that are started today are emitted with `STARTED`, so they
  are still started after deploy instead of coming up suspended.
- **Secrets are never written out.** Objects carrying inline credentials are reported instead of emitted, so no
  keys or tokens land in your definition files.
- **One bad object does not block the rest.** Anything that cannot be migrated is reported with a reason, rather
  than failing the whole run.

Expect most objects to adopt with no changes. Some will need fine-tuning before the plan is clean, which is
normal for a migration and covered in [Reviewing the plan](#reviewing-the-plan).

## DDL_TO_DCM_DEFINITIONS

### What gets migrated

| Category | Types |
|---|---|
| Structure | Database, Schemas, Tables, Views, Dynamic Tables |
| Programmatic | Tasks, Functions, Procedures (including overloads) |
| Ingestion | Pipes, Streams |
| Utility | Sequences, File Formats, Alerts, Tags |
| Governance | Masking Policies, Authentication Policies |
| Storage | Internal Stages; External Stages backed by a storage integration |

Every type can be selected individually via `object_type_allow_list`, see [Parameters](#parameters).

### Reported instead of migrated

These appear in the result table as `UNSUPPORTED` or `INFO`, so you can act on them. They are never emitted in
a way that would be unsafe or fail to deploy:

- **Grants, roles, and tag/policy attachments.** Out of scope for this procedure, which covers object structure.
  Policy and tag *objects* are migrated, their associations are not. Roles and grants are covered by
  `GRANTS_TO_DCM_DEFINITIONS` below.
- **External stages with embedded credentials.** Flagged rather than emitted, to keep secrets out of definition
  files. Use a storage integration instead, which is fully supported.
- **Data Metric Functions** and **Semantic Views.** Flagged as unsupported.
- **Temporary stages.** Skipped.

### Good to know

- Database and schema names must be ordinary unquoted identifiers. Anything else is rejected up front as an
  `ERROR` row before any SQL runs.
- A stage's inline file format and copy options cannot be read back from Snowflake, so stages are migrated
  without them. Re-add those by hand if you depend on them.
- A stream's definition is assembled from `SHOW STREAMS` metadata (source object, `APPEND_ONLY` /
  `INSERT_ONLY`, comment) rather than from `GET_DDL`, because `GET_DDL('STREAM', ...)` renders the source of a
  stream on a dynamic table as a single quoted identifier containing dots (`"DB.SCHEMA.NAME"`), which does not
  resolve.
- `SHOW_INITIAL_ROWS` cannot be read back from Snowflake. `SHOW STREAMS`, `DESCRIBE STREAM`,
  `DESCRIBE AS RESOURCE`, and `GET_DDL` all omit it, but `CREATE OR ALTER` still enforces it. A stream created
  with `SHOW_INITIAL_ROWS = TRUE` therefore fails the first plan with
  `Property 'SHOW_INITIAL_ROWS' cannot be changed in CREATE OR ALTER`. Add `SHOW_INITIAL_ROWS = TRUE` to that
  stream's definition by hand and re-plan.
- Of a stream's properties, only the comment can be altered in place. The source object, `APPEND_ONLY`, and
  `INSERT_ONLY` are immutable, so a definition that diverges from the live stream on any of those fails the plan
  rather than silently replacing the stream. A clean adoption preserves the stream's offset.

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
| `db_name` | Database whose objects are migrated. Must be an ordinary unquoted identifier. |
| `schema_allow_list` | An `ARRAY` of schema names to limit the scan to (e.g. `['RAW','SERVE']`), or `NULL` for **all** schemas. `INFORMATION_SCHEMA` is always excluded. |
| `object_type_allow_list` | An `ARRAY` of object type names to limit the migration to (e.g. `['TABLE','VIEW']`), or `NULL`/empty for **all** types. Case-, space-, and underscore-insensitive (`'file format'` = `'FILE_FORMAT'`). Includes `'SCHEMA'` and `'DATABASE'` as valid tokens. An unrecognized name is reported as an `ERROR` row rather than failing the call. |
| `output_path` | Where the files are written: a stage path (`@my_db.my_schema.my_stage/dcm`) or a Snowflake Workspace path. |

When `'DATABASE'` is included (the default, since `NULL` means all types), the procedure also emits a
`DEFINE DATABASE <db_name>` statement, including its comment, into `schemas.sql` ahead of the schema
definitions. A DCM project cannot define its own parent database or schema, so exclude those types if the
project lives inside the database you are migrating.

**Prerequisites:** a role that can see the objects (the procedure runs with your privileges), write access to
`output_path`, and a running warehouse.

### How to use it

**1. Create the procedure.** Run the `CREATE OR REPLACE PROCEDURE` statement in `DDL_to_DCM_sproc.sql` once.

**2. Generate the definition files:**
```sql
CALL DDL_TO_DCM_DEFINITIONS(
    'MY_DATABASE',                              -- database to migrate
    NULL,                                       -- all schemas (or e.g. ['RAW','SERVE'])
    NULL,                                       -- all object types (or e.g. ['TABLE','VIEW'])
    '@MY_DATABASE.PUBLIC.DCM_STAGE/migration'   -- target folder
);
```

**3. Review the result table.** Each object reports a status:

| STATUS | Meaning |
|---|---|
| `SAVED` | Definition file written. |
| `INFO` | Advisory (e.g. an attachment was skipped, or a property was preserved). |
| `UNSUPPORTED` | Not migrated, see the message for why. |
| `ERROR` | Could not be read; message in `FILE_PATH`. |
| `WARNING` | A metadata lookup failed; the corresponding filter or check is degraded for this run. |
| `SUMMARY` | Roll-up counts at the top. |

Handle any `ERROR` and `UNSUPPORTED` rows, then:

**4. Adopt into a DCM project.** Point a project's manifest at the generated folder and run:
```bash
snow dcm plan   <project>
snow dcm deploy <project>
```

Review the plan before deploying, see [Reviewing the plan](#reviewing-the-plan).

### Output layout

One file per object type per schema (there is no per-object-file mode):

```
<output_path>/<DB>/schemas.sql                  # DEFINE DATABASE + DEFINE SCHEMA statements
<output_path>/<DB>/<SCHEMA>/tables.sql
<output_path>/<DB>/<SCHEMA>/views.sql
...
```

Folders and files by type: `tables`, `views`, `dynamic_tables`, `tasks`, `functions`, `procedures`,
`sequences`, `file_formats`, `alerts`, `tags`, `policies`, `pipes`, `streams`, `stages`.

## GRANTS_TO_DCM_DEFINITIONS

The companion procedure for roles and grants, the piece `DDL_TO_DCM_DEFINITIONS` deliberately leaves out. Point
it at an `ACCOUNT`, `DATABASE`, or `SCHEMA` scope and it produces `DEFINE ROLE` / `DEFINE DATABASE ROLE`
statements plus the `GRANT` statements needed to reproduce the caller's role setup.

Only roles **owned by the calling role**, and only grants **made by the calling role**
(`granted_by = CURRENT_ROLE()`), are emitted. Grants made by another role are reported as `UNSUPPORTED` with the
granter named, rather than silently skipped or emitted incorrectly.

### What gets migrated

- **Roles owned by the caller**, scoped to the chosen level:
  - `ACCOUNT` scope emits account roles (`DEFINE ROLE`)
  - `DATABASE` scope emits database roles in that database (`DEFINE DATABASE ROLE`)
  - `SCHEMA` scope emits no roles, since database roles are not schema-scoped
- **Grants made by the caller** within the chosen scope: privilege grants on objects, schemas, or databases,
  plus role-membership grants (`GRANT ROLE ... TO ...`, `GRANT DATABASE ROLE ... TO ...`).
- **Future grants**, when `consolidate_inherited = TRUE`, otherwise reported as `UNSUPPORTED`.

### Reported instead of migrated

- **`OWNERSHIP` grants.** DCM manages ownership separately, so these are skipped and reported.
- **Account-level privilege grants** (`GRANT ... ON ACCOUNT`). Ignored and reported.
- **Grants made by another role.** Reported with the granting role named, so you know to re-run as that role.
- **Grantees the procedure cannot target** (anything other than `ROLE`, `DATABASE ROLE`, `USER`, or `SHARE`).
- **Future grants without `consolidate_inherited`.** Reported with a note to enable the option.

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
| `output_path` | Where `roles.sql`, `grants.sql`, and `role_grants.sql` are written. Use the same path as the DDL procedure so the layouts merge. |
| `consolidate_inherited` | See [Consolidating future grants](#consolidating-future-grants-into-inherited) below. |

**Prerequisites:** run as the role whose roles and grants you want captured (both are filtered to that role),
write access to `output_path`, and a running warehouse.

### How to use it

**1. Create the procedure.** Run the `CREATE OR REPLACE PROCEDURE` statement in `GRANTS_TO_DCM_sproc.sql` once.

**2. Generate the definition files:**
```sql
CALL GRANTS_TO_DCM_DEFINITIONS('ACCOUNT',  NULL,              '@DB.SC.STG/run', FALSE);
CALL GRANTS_TO_DCM_DEFINITIONS('DATABASE', 'MY_DB',           '@DB.SC.STG/run', TRUE);
CALL GRANTS_TO_DCM_DEFINITIONS('SCHEMA',   'MY_DB.MY_SCHEMA', '@DB.SC.STG/run', TRUE);
```

**3. Review the result table.** Same status conventions as `DDL_TO_DCM_DEFINITIONS`, plus `INHERITED GRANT` rows
when consolidation applies. `UNSUPPORTED` rows are aggregated into one row per category with a count, instead of
one row per object.

**4. Adopt into a DCM project.** Same as the DDL procedure: point the project's manifest at the generated
`roles.sql` / `grants.sql` / `role_grants.sql` files, then plan and deploy.

### How grants are organized

Unlike `roles.sql` (one file per scope), grants are split **by container**, derived from each grant's own object
name rather than from `scope_type`. The layout is therefore identical no matter which scope produced it, and it
mirrors the DDL procedure's folder structure:

```
<output_path>/_account/roles.sql              # ACCOUNT scope only
<output_path>/_account/role_grants.sql        # GRANT ROLE ... TO ...
<output_path>/_account/grants.sql             # grants on containerless objects (WAREHOUSE, etc.)
<output_path>/<DB>/roles.sql                  # DATABASE scope only
<output_path>/<DB>/role_grants.sql            # GRANT DATABASE ROLE ... TO ...
<output_path>/<DB>/grants.sql                 # grants ON DATABASE <DB> itself
<output_path>/<DB>/<SCHEMA>/grants.sql        # grants ON SCHEMA + grants on objects inside it
```

A file is written only if it has content. One consequence: an `ACCOUNT`-scope call can produce `<DB>/...` files
for *any* database an owned role happens to have grants on, not just one, because each grant is filed under the
object it actually belongs to.

**Caveat:** because a file is only written when non-empty, a container whose grants later drop to zero (for
example everything revoked) leaves a stale file behind on re-run rather than being cleaned up. Delete stale
files manually, and review the `SAVED` and `UNSUPPORTED` counts against your expectations when re-running over
time.

### Consolidating future grants into `INHERITED`

With `consolidate_inherited = TRUE`, the final step checks whether the caller granted a privilege on **every
current object of a type** in a container (schema or database) **and** a matching `FUTURE` grant exists (same
privilege, grantee, container). When both hold, the per-object grants and the future grant are replaced by a
single statement:

```sql
GRANT INHERITED <privilege> ON ALL <type> IN <container> TO <grantee>;
```

This requires `FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED'` to deploy. Coverage that is only partial (a future
grant without matching per-object grants on every current object) is left as `UNSUPPORTED` rather than collapsed
incorrectly. Some object types are never eligible for consolidation (`SHARE`, `INTEGRATION`, `APPLICATION`,
`APPLICATION_PACKAGE`, and `ORGANIZATION`), and `OWNERSHIP` future grants are always reported rather than
converted.

## Reviewing the plan

A successful adoption shows no changes to your objects beyond the entry that records the DCM project
association. `GRANT` operations are also expected, since they only add access.

Most objects reach that state directly. Some edge cases need fine-tuning first, and a `CREATE`, `ALTER`, or
`DROP` in the plan means the definition does not yet match the live object. The common cases:

- **A view shows a one-time difference.** Views written with a trailing semicolon, or referencing objects
  without their full name, differ harmlessly on the first plan and settle after the first deploy.
- **A stage wants an `ALTER`.** Inline file format and copy options are not readable back from Snowflake, so a
  stage configured with non-default values needs those clauses added by hand.
- **A stream fails the plan on `SHOW_INITIAL_ROWS`.** Add `SHOW_INITIAL_ROWS = TRUE` to that stream's definition
  and re-plan.
- **A table shows property or column differences.** Usually column ordering or default value formatting. Compare
  against `SELECT GET_DDL('TABLE', '<fqn>', TRUE)` and adjust the definition to match.
- **ANALYZE reports syntax errors.** Check for constructs DCM does not accept, such as CTEs referenced in
  correlated subqueries (replace with joins), or cross-database references that are not fully qualified.
- **Tags or masking policies are missing after adoption.** Attachment clauses are stripped on purpose. The
  attachments remain on the live objects, they are just not managed by the project.
- **Expected grants are missing.** Only grants made by the role you ran as are captured. Check the aggregated
  `UNSUPPORTED` rows for `grant by another role` and re-run as that role.

To resolve a difference, treat the plan output as the truth about the live object and adjust the definition to
match it. Then re-plan and repeat until only the project association remains. The
[`dcm-migrate` Cortex Code skill](../Cortex_Code_Skill/) automates this loop, including reading the plan output
and proposing the corresponding definition edits.
