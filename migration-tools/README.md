# Cortex Skill: `dcm-migrate` - Bulk-Import Existing Snowflake Objects into a DCM Project

A [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) skill that migrates an existing Snowflake database (or selected schemas) into a [DCM Project](https://docs.snowflake.com/en/developer-guide/snowflake-cli/dcm/overview). The skill handles the full workflow: scanning the source database, generating DCM `DEFINE` definitions, validating with PLAN, and adopting with DEPLOY.

> **⚠️ Experimental**: This skill is experimental and intended for testing only. It is **not** part of the official Snowflake product and carries no SLA or support guarantees. Review all generated definitions and validate with PLAN before deploying to production.

For environments where Cortex Code is not available, a standalone stored procedure (`DDL_TO_DCM_DEFINITIONS`) is included as a fallback that covers the file generation step. The remaining steps (project setup, PLAN, DEPLOY) must then be run manually.


## What It Does

The skill takes a source database and a target DCM project, then:

1. Detects your environment (local CLI or Workspaces) and adapts accordingly
2. Asks for the source database, target project, and any schema or role filters
3. Scans the database and generates `DEFINE` statements for all supported objects
4. Integrates the definitions into a new or existing DCM project
5. Runs ANALYZE to catch syntax issues, and fixes them
6. Runs PLAN and validates zero changes (definitions match live objects exactly)
7. Runs DEPLOY to adopt the objects into DCM management
8. Optionally analyzes definitions for Jinja templating opportunities (multi-environment support)

The skill pauses at key checkpoints for your review before proceeding.

### Supported Object Types

- Schemas
- Tables
- Views
- Dynamic tables
- Tasks
- SQL functions
- SQL procedures
- Sequences
- File formats
- Alerts
- Tags
- Internal stages (minimal `DEFINE STAGE` generated from `SHOW STAGES` metadata (directory-table flag + comment). File format and copy options are not captured, so stages configured with non-default values may show `ALTER STAGE` drift during PLAN and need to be hand-tuned.)

Marked `UNSUPPORTED` in the output:
- Semantic views
- Data metric functions
- Non-SQL functions/procedures (Python, Java, JavaScript, Scala)
- External stages
- Ownership grants
- Future grants


### Grants

The migration also extracts grants at the database, schema, and object level into `grants.sql` files (one at the database root, one per schema). OWNERSHIP grants and future grants are reported as `UNSUPPORTED` for awareness but not written to the files — OWNERSHIP is implicit from object creation, and future grants do not apply to objects that already exist.

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

If none of these apply, use the `--role` filter (script) or switch to the target role (sproc) to migrate only objects owned by that role, and repeat the migration with each relevant role to cover the full database.

Reference: [Overview of Access Control](https://docs.snowflake.com/en/user-guide/security-access-control-overview), [SHOW OBJECTS](https://docs.snowflake.com/en/sql-reference/sql/show-objects).

### Getting Started

Tell the agent what you want to migrate. For example:

> "Migrate the ANALYTICS_DB database into a new DCM project"

> "Import the RAW and SERVE schemas from PROD_DB into my existing DCM project"

The agent will guide you through the rest.

---


## Alternative: Stored Procedure (`DDL_TO_DCM_DEFINITIONS`) for manual execution

If you do not have Cortex Code, the included stored procedure can handle the file generation step on its own. It scans a database, converts `CREATE` statements to `DEFINE` syntax, expands bare object references to fully qualified names, and writes the resulting `.sql` files to a stage or workspace path.

The procedure only generates definition files. It does not create a DCM project, run ANALYZE, PLAN, or DEPLOY. See [Manual Steps After File Generation](#manual-steps-after-file-generation) below for the remaining workflow.

### Setup

Run the contents of `DDL_to_DCM_sproc.sql` in any Snowflake worksheet to create the procedure. It uses `EXECUTE AS CALLER`, so it runs with your current role's privileges.

### Usage

```sql
CALL DDL_TO_DCM_DEFINITIONS(
    'MY_DATABASE',                -- database name
    NULL,                         -- schema list (NULL = all schemas, or ARRAY e.g. ['RAW', 'SERVE'])
    NULL,                         -- object types (NULL = all supported, or ARRAY e.g. ['VIEW', 'DYNAMIC TABLE', 'SCHEMA', 'GRANT'])
    TRUE,                         -- group files by type (TRUE = one file per type, FALSE = one file per object)
    'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live/DCM_Migration'  -- output path (stage or workspace)
);
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `db_name` | STRING | Source database to scan |
| `schema_list` | ARRAY | Schemas to include, or `NULL` for all (INFORMATION_SCHEMA is always excluded) |
| `object_types` | ARRAY | Object types to include, or `NULL` for all supported types. Accepted values (case-insensitive, spaces or underscores): `TABLE`, `VIEW`, `DYNAMIC TABLE`, `TASK`, `FUNCTION`, `PROCEDURE`, `SEQUENCE`, `FILE FORMAT`, `ALERT`, `TAG`, `STAGE`, `SCHEMA`, `GRANT`. Any unknown value aborts the call with an error before any scanning starts. |
| `group_files_by_type` | BOOLEAN | `TRUE`: one `.sql` file per object type per schema. `FALSE`: one file per object. |
| `output_path` | STRING | Target path for generated files. Can be a named stage (`@my_stage/folder`) or a workspace path (`snow://workspace/...`) |

### Output

Returns a result table with columns: `SCHEMA`, `OBJECT_TYPE`, `OBJECT_NAME`, `STATUS`, `FILE_PATH`.

Status values:
- `SAVED` — definition file (or `grants.sql`) written successfully
- `UNSUPPORTED` — intentionally excluded. Reason in `FILE_PATH` (e.g. `semantic views`, `language=PYTHON`, `OWNERSHIP grant`, `future grant (SELECT)`).
- `ERROR` — DDL retrieval failed for that object, or a per-schema SHOW command failed (permissions issue)
- `WARNING` — a filtering lookup failed (semantic-view or `INFORMATION_SCHEMA` language lookup); filter is degraded for this run

The procedure prepends `SUMMARY` rows to the result with a `TOTAL` count and per-status counts, followed by the detail rows sorted ERROR → UNSUPPORTED → SAVED.

### Example

Migrate only the `RAW` and `SERVE` schemas, writing grouped files to a workspace:

```sql
CALL DDL_TO_DCM_DEFINITIONS(
    'ANALYTICS_DB',
    ['RAW', 'SERVE'],
    NULL,
    TRUE,
    'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live/analytics_migration'
);
```

This produces files organized as (only populated object types appear):

```
<output_path>/ANALYTICS_DB/
├── schemas.sql
├── grants.sql                 (database-level grants, if any)
├── RAW/
│   ├── grants.sql             (schema + object grants for RAW)
│   ├── tables.sql
│   ├── views.sql
│   ├── dynamic_tables.sql
│   ├── tasks.sql
│   ├── functions.sql
│   ├── procedures.sql
│   ├── sequences.sql
│   ├── file_formats.sql
│   ├── stages.sql
│   ├── tags.sql
│   └── alerts.sql
└── SERVE/
    ├── grants.sql
    ├── tables.sql
    ├── views.sql
    └── functions.sql
```

### Role Filtering

The stored procedure does not have a built-in role filter. If you need to limit the migration to objects owned by a specific role, switch to that role before calling the procedure:

```sql
USE ROLE ANALYTICS_ROLE;
CALL DDL_TO_DCM_DEFINITIONS('ANALYTICS_DB', NULL, NULL, TRUE, '@my_stage/migration');
```

This way, the procedure only has access to objects the role can see, and `GET_DDL` calls for unowned objects will be logged as errors rather than silently producing incorrect definitions.


## Manual Steps After File Generation

If you used the stored procedure (or need to complete the workflow manually for any reason), the remaining steps are:

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

The plan should show **zero changes** for all objects being adopted. `GRANT` operations are acceptable (they are additive). If any `CREATE`, `ALTER`, or `DROP` operations appear, the definitions do not match the live objects and need adjustment.

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

**PLAN shows ALTER for internal stages**: The generated `DEFINE STAGE` only captures the directory-table flag and comment from `SHOW STAGES`. If the stage has a non-default file format or copy options, edit the generated `.sql` file to add the missing clauses, then re-run PLAN until it shows zero changes.

**ANALYZE reports syntax errors**: Check for unsupported DDL constructs like correlated subqueries with CTEs (replace with JOINs), or cross-database references that are not fully qualified.

**BACKFILL FROM warnings**: A bare name in a `BACKFILL FROM` clause could not be expanded to a fully qualified name, typically because the referenced object was not found during the scan. Manually qualify the reference or remove the clause if the source object no longer exists.
