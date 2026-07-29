<!-- Human documentation only. Not part of the skill workflow. Agents: refer to SKILL.md instead. -->

# Cortex Skill: `dcm-migrate` - Bulk-Import Existing Snowflake Objects into a DCM Project

A [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) skill that migrates an existing Snowflake database (or selected schemas) into a [DCM Project](https://docs.snowflake.com/en/developer-guide/snowflake-cli/dcm/overview). The skill handles the full workflow: scanning the source database, generating DCM `DEFINE` definitions, validating with PLAN, and adopting with DEPLOY.

> **⚠️ Experimental**: This skill is experimental and intended for testing only. It is **not** part of the official Snowflake product and carries no SLA or support guarantees. Review all generated definitions and validate with PLAN before deploying to production.

For environments where Cortex Code is not available, two standalone stored procedures cover the file generation step: `DDL_TO_DCM_DEFINITIONS` for object structure and `GRANTS_TO_DCM_DEFINITIONS` for roles and grants. Both live in [`../python_procedures/`](../python_procedures/) alongside their own documentation. The remaining steps (project setup, PLAN, DEPLOY) must then be run manually.


## What It Does

The skill takes a source database and a target DCM project, then:

1. Detects your environment (local CLI or Workspaces) and adapts accordingly
2. Asks for the source database, target project, and any schema, object-type, or role filters
3. Scans the database and generates `DEFINE` statements for all supported objects
4. Optionally captures roles and grants at account, database, or schema scope
5. Integrates the definitions into a new or existing DCM project
6. Runs ANALYZE to catch syntax issues, and fixes them
7. Runs PLAN and validates zero changes (definitions match live objects exactly)
8. Runs DEPLOY to adopt the objects into DCM management
9. Optionally analyzes definitions for Jinja templating opportunities (multi-environment support)

The skill pauses at key checkpoints for your review before proceeding.

### Supported Object Types

| Category | Types |
|---|---|
| Structure | Database, Schemas, Tables, Views, Dynamic Tables |
| Programmatic | Tasks, Functions, Procedures (including overloads) |
| Ingestion | Pipes |
| Utility | Sequences, File Formats, Alerts, Tags |
| Governance | Masking Policies, Authentication Policies |
| Storage | Internal Stages; External Stages backed by a storage integration |

Stages are generated from `SHOW STAGES` metadata (URL and storage integration for external stages, directory-table flag, comment). Inline file format and copy options cannot be read back from Snowflake, so stages configured with non-default values may show `ALTER STAGE` drift during PLAN and need to be hand-tuned.

Currently-running tasks and alerts are emitted with `STARTED`, so they keep running after adoption instead of coming up suspended.

Marked `UNSUPPORTED` in the output:
- Semantic views
- Data metric functions (the `TABLE`-argument column name is not exposed by `GET_DDL` or `DESCRIBE`, so the DDL cannot be regenerated)
- External stages with inline credentials (secrets are never written into definition files)
- External stages without a storage integration

Silently skipped: streams and temporary stages.

Tag and policy *attachment* clauses on tables and views (`WITH TAG`, `WITH MASKING POLICY`, `WITH ROW ACCESS POLICY`) are stripped and reported as `INFO`, because DCM does not support setting them via `CREATE OR ALTER`. The attachments stay on the live objects, they are simply not managed by the project. The tag and policy *objects* themselves are migrated.


### Grants

Roles and grants are handled separately from object structure, by `grants_to_dcm.py` (or `GRANTS_TO_DCM_DEFINITIONS`). Point it at an `ACCOUNT`, `DATABASE`, or `SCHEMA` scope and it writes `DEFINE ROLE` / `DEFINE DATABASE ROLE` statements plus the `GRANT` statements needed to reproduce the caller's setup.

Only roles **owned by the calling role** and only grants **made by the calling role** (`granted_by = CURRENT_ROLE()`) are emitted. Grants made by another role are reported as `UNSUPPORTED` naming the granter, so you know to re-run as that role. `OWNERSHIP` grants, account-level privilege grants, and unsupported grantee types are reported rather than emitted. Future grants are reported unless consolidation is enabled, in which case full coverage collapses into `GRANT INHERITED ... ON ALL ... IN ...` (which requires `FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED'` to deploy).

### Prerequisites

- Cortex Code with the `dcm-migrate` skill installed (place the skill directory under `.cortex/skills/`)
- A Snowflake connection with a role that can see and `GET_DDL` every object you want migrated (see [Role visibility](#role-visibility-what-the-migration-can-actually-see) below)
- For new projects: `CREATE DCM PROJECT` privilege in the target schema

### Role visibility (what the migration can actually see)

Ownership in Snowflake is not transitive down the object hierarchy. A role that owns the source *database* does **not** automatically see schemas or objects inside the database that were created by (or transferred to) other roles. `SHOW OBJECTS` and `GET_DDL` both check per-object privileges.

Use a role that falls into one of the following categories:

1. `ACCOUNTADMIN`
2. A role with the global `MANAGE GRANTS` privilege (by default only `SECURITYADMIN`)
3. A role that owns the source database **and** owns every schema and object inside it (common in greenfield setups where one role created everything)
4. A role with explicit privileges granted on every target object

If none of these apply, use the `--role` filter (script) or switch to the target role (procedure) to migrate only objects owned by that role, and repeat the migration with each relevant role to cover the full database.

Reference: [Overview of Access Control](https://docs.snowflake.com/en/user-guide/security-access-control-overview), [SHOW OBJECTS](https://docs.snowflake.com/en/sql-reference/sql/show-objects).

### Getting Started

Tell the agent what you want to migrate. For example:

> "Migrate the ANALYTICS_DB database into a new DCM project"

> "Import the RAW and SERVE schemas from PROD_DB into my existing DCM project"

The agent will guide you through the rest.

---


## Alternative: Stored Procedures for manual execution

If you do not have Cortex Code, two stored procedures in [`../python_procedures/`](../python_procedures/) handle the file generation step on their own:

- **`DDL_TO_DCM_DEFINITIONS`** — object structure. Scans a database, converts `CREATE` statements to `DEFINE` syntax, expands bare object references to fully qualified names, and writes the resulting `.sql` files to a stage or workspace path.
- **`GRANTS_TO_DCM_DEFINITIONS`** — roles and grants, at account, database, or schema scope.

The procedures only generate definition files. They do not create a DCM project, run ANALYZE, PLAN, or DEPLOY. See [Manual Steps After File Generation](#manual-steps-after-file-generation) below for the remaining workflow.

### Setup

Run the contents of `DDL_to_DCM_sproc.sql` and `GRANTS_TO_DCM_sproc.sql` in any Snowflake worksheet to create the procedures. Both use `EXECUTE AS CALLER`, so they run with your current role's privileges.

### DDL_TO_DCM_DEFINITIONS

```sql
CALL DDL_TO_DCM_DEFINITIONS(
    'MY_DATABASE',                -- database name
    NULL,                         -- schema allow-list (NULL = all schemas, or ARRAY e.g. ['RAW', 'SERVE'])
    NULL,                         -- object-type allow-list (NULL = all types, or ARRAY e.g. ['TABLE', 'VIEW'])
    'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live/DCM_Migration'  -- output path (stage or workspace)
);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `db_name` | STRING | Source database to scan |
| `schema_allow_list` | ARRAY | Schemas to include, or `NULL` for all (INFORMATION_SCHEMA is always excluded) |
| `object_type_allow_list` | ARRAY | Object types to include, or `NULL`/empty for all. Case-, space-, and underscore-insensitive (`'file format'` = `'FILE_FORMAT'`). Includes `'SCHEMA'` and `'DATABASE'` as valid tokens. An unrecognized name is reported as an `ERROR` row rather than failing the call. |
| `output_path` | STRING | Target path for generated files. Can be a named stage (`@my_stage/folder`) or a workspace path (`snow://workspace/...`) |

Output is always one file per object type per schema; there is no per-object file mode.

### GRANTS_TO_DCM_DEFINITIONS

```sql
CALL GRANTS_TO_DCM_DEFINITIONS('ACCOUNT',  NULL,              '@DB.SC.STG/run', FALSE);
CALL GRANTS_TO_DCM_DEFINITIONS('DATABASE', 'MY_DB',           '@DB.SC.STG/run', TRUE);
CALL GRANTS_TO_DCM_DEFINITIONS('SCHEMA',   'MY_DB.MY_SCHEMA', '@DB.SC.STG/run', TRUE);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `scope_type` | STRING | `ACCOUNT`, `DATABASE`, or `SCHEMA`. Determines which roles are in scope and how grants are collected. |
| `scope_name` | STRING | `NULL` for `ACCOUNT`; the database name for `DATABASE`; `DB.SCHEMA` for `SCHEMA`. |
| `output_path` | STRING | Where `roles.sql`, `grants.sql`, and `role_grants.sql` are written. |
| `consolidate_inherited` | BOOLEAN | Collapse full per-object plus future coverage into `GRANT INHERITED ... ON ALL ... IN ...`. Defaults to `FALSE`. Requires `FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED'` to deploy. |

Run it as the role whose roles and grants you want captured; both are filtered to that role.

### Output

Both procedures return a result table with columns: `SCHEMA`, `OBJECT_TYPE`, `OBJECT_NAME`, `STATUS`, `FILE_PATH`.

Status values:
- `SAVED` — definition file written successfully
- `INFO` — advisory: a property was gap-filled from `DESCRIBE AS RESOURCE`, or a tag/policy attachment clause was stripped
- `UNSUPPORTED` — intentionally excluded. Reason in `FILE_PATH` (e.g. `semantic views`, `external stage with inline credentials (use a storage integration)`, `OWNERSHIP (skipped)`).
- `ERROR` — DDL retrieval failed for that object, or a per-schema SHOW command failed (permissions issue)
- `WARNING` — a metadata lookup failed; the corresponding filter or collision check is degraded for this run

Both prepend `SUMMARY` rows with a `TOTAL` count and per-status counts, followed by the detail rows sorted ERROR → UNSUPPORTED → SAVED. `GRANTS_TO_DCM_DEFINITIONS` aggregates `UNSUPPORTED` rows into one row per category with a count, instead of one row per object.

### Example

Migrate only the `RAW` and `SERVE` schemas, then capture the caller's grants for the same database:

```sql
CALL DDL_TO_DCM_DEFINITIONS(
    'ANALYTICS_DB',
    ['RAW', 'SERVE'],
    NULL,
    'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live/analytics_migration'
);

CALL GRANTS_TO_DCM_DEFINITIONS(
    'DATABASE',
    'ANALYTICS_DB',
    'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live/analytics_migration',
    FALSE
);
```

This produces files organized as (only populated object types appear):

```
<output_path>/ANALYTICS_DB/
├── schemas.sql                 (DEFINE DATABASE + DEFINE SCHEMA statements)
├── roles.sql                   (database roles owned by the caller)
├── role_grants.sql             (GRANT DATABASE ROLE ... TO ...)
├── grants.sql                  (grants ON DATABASE ANALYTICS_DB itself)
├── RAW/
│   ├── grants.sql              (schema + object grants for RAW)
│   ├── tables.sql
│   ├── views.sql
│   ├── dynamic_tables.sql
│   ├── tasks.sql
│   ├── functions.sql
│   ├── procedures.sql
│   ├── sequences.sql
│   ├── file_formats.sql
│   ├── stages.sql
│   ├── pipes.sql
│   ├── policies.sql            (masking + authentication policies)
│   ├── tags.sql
│   └── alerts.sql
└── SERVE/
    ├── grants.sql
    ├── tables.sql
    ├── views.sql
    └── functions.sql
```

An `ACCOUNT`-scope grants call also writes an `_account/` folder holding `roles.sql`, `role_grants.sql`, and grants on containerless objects such as warehouses.

### Role Filtering

The stored procedures do not have a built-in role filter. If you need to limit the migration to objects owned by a specific role, switch to that role before calling:

```sql
USE ROLE ANALYTICS_ROLE;
CALL DDL_TO_DCM_DEFINITIONS('ANALYTICS_DB', NULL, NULL, '@my_stage/migration');
```

This way, the procedure only has access to objects the role can see, and `GET_DDL` calls for unowned objects will be logged as errors rather than silently producing incorrect definitions. The skill's `ddl_to_dcm.py` additionally supports `--role` to filter by the `owner` column without switching roles.


## Manual Steps After File Generation

If you used the stored procedures (or need to complete the workflow manually for any reason), the remaining steps are:

### 1. Create or locate a DCM project

If you do not already have a DCM project, create a `manifest.yml` at the project root:

```yaml
manifest_version: 2
type: DCM_PROJECT
default_target: 'DEV'

targets:
  DEV:
    project_name: 'MY_DB.MY_SCHEMA.MY_PROJECT'
    project_owner: MY_ROLE
```

Place the generated definition files under `sources/definitions/` relative to the manifest. Then create the project object in Snowflake:

```bash
snow dcm create -c <connection> --from <project_dir>
```

### 2. Run ANALYZE to check syntax

```bash
snow dcm raw-analyze -c <connection> --target DEV --from <project_dir>
```

Fix any reported errors in the definition files and re-run until it passes.

### 3. Run PLAN to validate zero changes

```bash
snow dcm plan -c <connection> --target DEV --save-output --from <project_dir>
```

The plan should show **zero changes** for all objects being adopted, apart from the `ALTER` that sets the DCM Project association on each entity. `GRANT` operations are acceptable (they are additive). If any other `CREATE`, `ALTER`, or `DROP` operations appear, the definitions do not match the live objects and need adjustment.

A view written with a trailing semicolon or unqualified references may show a harmless one-time difference on the first plan that clears itself after the first deploy.

### 4. Run DEPLOY to adopt

```bash
snow dcm deploy -c <connection> --target DEV --alias "migrate ANALYTICS_DB" --from <project_dir>
```

This adopts the existing objects into DCM management without modifying them.

### 5. Verify

```bash
snow dcm list-deployments -c <connection> --from <project_dir>
```

All migrated objects are now managed by the DCM project.


## Troubleshooting

**"Cannot access database"**: The active role lacks `USAGE` on the source database. Grant `USAGE` or switch to a role that has it.

**Many ERROR rows for individual objects**: The role likely does not have `SELECT`/`USAGE` on those specific objects. Switch to a role that owns the target objects, or grant the necessary privileges.

**PLAN shows ALTER operations**: The definition does not exactly match the live object. Common causes are column ordering differences, default value formatting, or missing object properties. Compare the definition with `SELECT GET_DDL('TABLE', '<fqn>', TRUE)` and adjust to match.

**PLAN shows ALTER for stages**: A stage's inline file format and copy options cannot be read back from Snowflake, so they are not emitted. Edit the generated `.sql` file to add the missing clauses, then re-run PLAN until it shows zero changes.

**ANALYZE reports syntax errors**: Check for unsupported DDL constructs like correlated subqueries with CTEs (replace with JOINs), or cross-database references that are not fully qualified.

**BACKFILL FROM warnings**: A bare name in a `BACKFILL FROM` clause could not be expanded to a fully qualified name, typically because the referenced object was not found during the scan. Manually qualify the reference or remove the clause if the source object no longer exists.

**Tags or masking policies missing after adoption**: Attachment clauses are stripped on purpose (reported as `INFO`) because DCM does not support setting them via `CREATE OR ALTER`. The attachments remain on the live objects; they are just not managed by the project. Re-apply them manually if the project needs to own them.

**Expected grants are missing**: Only grants where `granted_by = CURRENT_ROLE()` are emitted. Check the aggregated `UNSUPPORTED` rows for `grant by another role` and re-run as that role.

**Stale grant files after a re-run**: A `grants.sql` is written only when it has content, so a container whose grants have since been revoked keeps its previous file. Delete stale files manually.
