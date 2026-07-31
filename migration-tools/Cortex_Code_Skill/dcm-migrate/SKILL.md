---
name: dcm-migrate
description: "Bulk-migrate existing Snowflake objects into a new or existing DCM project using the ddl_to_dcm.py and grants_to_dcm.py scripts. Converts DDL to DEFINE syntax, captures roles and grants, validates with PLAN (zero-change check), adopts via DEPLOY, and analyzes definitions for Jinja templating opportunities. Use when: migrating a database to DCM, bulk-importing objects, adopting existing infrastructure, converting DDL to DCM definitions, migrating roles and grants. Triggers: migrate to DCM, import database, adopt objects, bulk import, DDL to DCM, convert database to DCM, migrate database, migrate grants, migrate roles."
---

# DCM Migrate

Bulk-migrate existing Snowflake database objects into a new or existing DCM project. Runs `ddl_to_dcm.py` (object structure) and optionally `grants_to_dcm.py` (roles and grants) via `uv` to generate definition files, then validates with `snow dcm plan` and adopts with `snow dcm deploy`.

## When to Use

- Migrating an entire database (or selected schemas) into DCM management
- Bulk-importing many existing objects at once (vs. the manual one-by-one adoption in the bundled DCM skill)

## When NOT to Use

- Adopting 1-3 individual objects — use the bundled DCM skill's IMPORT_EXISTING workflow instead
- Creating a new project from scratch with no existing objects — use the bundled DCM skill's CREATE workflow

## Prerequisites

- `snow` CLI 3.16+, `uv` installed
- Active Snowflake connection with a role that can `GET_DDL` every target object (see **Role visibility** below). For new projects: `CREATE DCM PROJECT` privilege in the target schema.

**Role visibility:**

Ownership in Snowflake is not transitive down the object hierarchy. A role that owns the source database does **not** automatically see schemas or objects inside the database that were created by (or transferred to) other roles. `SHOW OBJECTS` and `GET_DDL` both check per-object privileges.

Use a role that falls into one of the following categories:

1. `ACCOUNTADMIN`
2. A role with the global `MANAGE GRANTS` privilege (by default only `SECURITYADMIN`)
3. A role that owns the source database **and** owns every schema and object inside it (common in greenfield setups where one role created everything)
4. A role with explicit privileges granted on every target object

If none of these apply, use the `--role` filter and run the migration once per owning role to cover the full database. Reference: [Overview of Access Control](https://docs.snowflake.com/en/user-guide/security-access-control-overview).

## Tools

### Script: ddl_to_dcm.py

Scans a database, retrieves DDL for all objects, converts `CREATE` to `DEFINE`, expands references to fully qualified names, and writes `.sql` definition files to a local directory. One file per object type per schema, always. Roles and grants are out of scope; use `grants_to_dcm.py` for those.

Covered object types:

| Category | Types |
|---|---|
| Structure | Database, Schemas, Tables, Views, Dynamic Tables |
| Programmatic | Tasks, Functions, Procedures (including overloads) |
| Ingestion | Pipes, Streams |
| Utility | Sequences, File Formats, Alerts, Tags |
| Governance | Masking Policies, Authentication Policies |
| Storage | Internal Stages; External Stages backed by a storage integration |

Reported as UNSUPPORTED rather than emitted:
- **Semantic views** (not yet supported by DCM)
- **Data metric functions** (the `TABLE`-argument column name is not exposed by `GET_DDL` or `DESCRIBE`, so the DDL cannot be regenerated)
- **External stages with inline credentials** (secrets must never be written into definition files; use a storage integration instead)
- **External stages without a storage integration** (not reconstructable)

Silently skipped (no output row): temporary stages.

Tag and policy *attachment* clauses (`WITH TAG`, `WITH MASKING POLICY`, `WITH ROW ACCESS POLICY`) are stripped from table and view DDL and reported as `INFO`, because DCM does not support setting them via `CREATE OR ALTER`. The tag and policy *objects* themselves are migrated.

Stages are emitted from `SHOW STAGES` metadata (URL and storage integration for external stages, directory flag, comment). Inline file format and copy options cannot be read back from Snowflake, so PLAN may show `ALTER STAGE` drift for stages with non-default settings.

Currently-running tasks and alerts are emitted with `STARTED` so they stay running after adoption instead of coming up suspended.

**Usage:**
```bash
SNOWFLAKE_CONNECTION_NAME=<connection> uv run --project <SKILL_DIR> \
  python <SKILL_DIR>/scripts/ddl_to_dcm.py \
  --db-name <DB_NAME> \
  [--schema-list SCHEMA1 SCHEMA2 ...] \
  [--object-types TYPE1 TYPE2 ...] \
  --output-path <PROJECT_DIR>/sources/definitions \
  [--role <ROLE_NAME>]
```

**Arguments:**
- `--db-name` (required): Source database name
- `--output-path` (required): Local directory for generated definition files
- `--schema-list` (optional): Space-separated schema allow-list; omit for all schemas
- `--object-types` (optional): Space-separated object-type allow-list. Accepted values (case-insensitive, spaces or underscores interchangeable): `DATABASE`, `SCHEMA`, `TABLE`, `VIEW`, `DYNAMIC TABLE`, `TASK`, `FUNCTION`, `PROCEDURE`, `SEQUENCE`, `FILE FORMAT`, `ALERT`, `TAG`, `MASKING POLICY`, `AUTHENTICATION POLICY`, `PIPE`, `STREAM`, `STAGE`. Omit for all supported types. An unrecognized value is reported as an `ERROR` row and the run continues with the remaining types.
- `--connection` (optional): Snowflake connection name override. Normally use the `SNOWFLAKE_CONNECTION_NAME` env var instead.
- `--role` (optional): Only migrate objects owned by this role, filtered by the `owner` column across every discovery command. Recommended for non-ACCOUNTADMIN users to avoid permission errors on unowned objects.

**Output:** JSON array to stdout with `{schema, object_type, object_name, status, file_path}` per row, prefixed by `SUMMARY` rows carrying a `TOTAL` and per-status counts. Detail rows are sorted `ERROR` → `UNSUPPORTED` → `SAVED` → everything else. Summary to stderr. Statuses:

| Status | Meaning |
|---|---|
| `SAVED` | Definition written to the file named in `file_path` |
| `INFO` | Advisory: a property was gap-filled, or an attachment clause was stripped |
| `UNSUPPORTED` | Not migrated; reason in `file_path` |
| `ERROR` | Could not be read; message in `file_path` |
| `WARNING` | A metadata lookup failed and a filter is degraded for this run |

When `--role` is used, the stderr summary also reports how many objects matched the role.

### Script: grants_to_dcm.py

Generates `DEFINE ROLE` / `DEFINE DATABASE ROLE` statements plus the `GRANT` statements needed to reproduce the caller's role setup. Only roles **owned by the calling role** and only grants **made by the calling role** (`granted_by = CURRENT_ROLE()`) are emitted. Grants made by another role are reported as `UNSUPPORTED` naming the granter, so the user knows to re-run as that role.

Reported rather than emitted:
- **OWNERSHIP grants** — DCM manages ownership separately
- **Account-level privilege grants** (`GRANT ... ON ACCOUNT`) — ignored
- **Grants made by another role**
- **Grantees other than** `ROLE`, `DATABASE ROLE`, `USER`, `SHARE`
- **Future grants**, unless `--consolidate-inherited` is passed

**Usage:**
```bash
SNOWFLAKE_CONNECTION_NAME=<connection> uv run --project <SKILL_DIR> \
  python <SKILL_DIR>/scripts/grants_to_dcm.py \
  --scope-type ACCOUNT|DATABASE|SCHEMA \
  [--scope-name <DB> | <DB>.<SCHEMA>] \
  --output-path <PROJECT_DIR>/sources/definitions \
  [--consolidate-inherited]
```

**Arguments:**
- `--scope-type` (required): `ACCOUNT`, `DATABASE`, or `SCHEMA`. Determines which roles are in scope and how grants are collected. `ACCOUNT` scope emits account roles, `DATABASE` scope emits database roles in that database, `SCHEMA` scope emits no roles (database roles are not schema-scoped).
- `--scope-name`: omit for `ACCOUNT`; the database name for `DATABASE`; `DB.SCHEMA` for `SCHEMA`.
- `--output-path` (required): Local directory for generated files. Use the same directory as `ddl_to_dcm.py` so the layouts merge.
- `--consolidate-inherited` (optional): When the caller granted a privilege on **every current object** of a type in a container **and** a matching `FUTURE` grant exists, replace both with a single `GRANT INHERITED <privilege> ON ALL <type> IN <container> TO <grantee>`. Partial coverage is left as `UNSUPPORTED` rather than collapsed incorrectly. Requires `FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED'` to deploy.
- `--connection` (optional): Snowflake connection name override.

**Output layout:** grants are split by container, derived from each grant's own object name rather than from `--scope-type`, so the layout is identical no matter which scope produced it and mirrors the `ddl_to_dcm.py` folder structure:

```
<output-path>/_account/roles.sql          # ACCOUNT scope only
<output-path>/_account/role_grants.sql    # GRANT ROLE ... TO ...
<output-path>/_account/grants.sql         # grants on containerless objects (WAREHOUSE, etc.)
<output-path>/<DB>/roles.sql              # DATABASE scope only
<output-path>/<DB>/role_grants.sql        # GRANT DATABASE ROLE ... TO ...
<output-path>/<DB>/grants.sql             # grants ON DATABASE <DB> itself
<output-path>/<DB>/<SCHEMA>/grants.sql    # grants ON SCHEMA + grants on objects inside it
```

A file is written only when it has content. One consequence: an `ACCOUNT`-scope run can produce `<DB>/...` files for any database an owned role happens to have grants on, not just one.

**Caveat:** because a file is only written when non-empty, a container whose grants later drop to zero leaves a stale file behind on re-run rather than being cleaned up. Check the `SAVED` and `UNSUPPORTED` counts against expectations when re-running over time.

The caller role is the role of the active Snowflake connection. To capture grants made by a different role, re-run with a connection that uses that role.

### CLI Commands

- **`snow dcm ...`** — DCM lifecycle commands (create, raw-analyze, plan, deploy). Always pass `-c <connection>`.
## Workflow

```
Step 1: Gather Context
  ↓
Step 2: Resolve Target (new or existing project?)
  ├─→ New project → Create project + manifest + directory
  └─→ Existing project → Locate manifest, download sources if needed
  ↓
  ⚠️ STOP: Approve target configuration
  ↓
Step 3: Generate Definitions (run ddl_to_dcm.py via uv)
  ↓
  ⚠️ STOP: Review generation results
  ↓
Step 3b: Generate Roles and Grants (optional, run grants_to_dcm.py via uv)
  ↓
  ⚠️ STOP: Review grant results
  ↓
Step 4: Integrate into Project (handle unsupported objects)
  ↓
Step 5: Run ANALYZE → fix errors
  ↓
Step 6: Run PLAN → validate zero changes
  ↓
  ⚠️ STOP: Present plan results, iterate if mismatches
  ↓
Step 7: Run DEPLOY to adopt
  ↓
  ⚠️ STOP: Confirm deployment success
  ↓
Step 8: Jinja Templating Analysis (optional)
  ↓
  ⚠️ STOP: Present templating proposals
```

### Step 1: Gather Context

**Role detection (mandatory first step):**

1. Run `SELECT CURRENT_ROLE()` and present the role
2. If the role is **not** ACCOUNTADMIN and does **not** hold `MANAGE GRANTS`, warn that database ownership does not cascade to child objects. Recommend **owned-only** migration or switching to ACCOUNTADMIN.
3. Ask the user: **owned-only** (recommended) or **all objects**? This determines whether `--role` is passed in Step 3.

Collect from the user:

1. **Source database** (required)
2. **Schema allow-list** (optional; omit for all schemas)
3. **Object-type allow-list** (optional; omit for all supported types). Accepted values: `DATABASE`, `SCHEMA`, `TABLE`, `VIEW`, `DYNAMIC TABLE`, `TASK`, `FUNCTION`, `PROCEDURE`, `SEQUENCE`, `FILE FORMAT`, `ALERT`, `TAG`, `MASKING POLICY`, `AUTHENTICATION POLICY`, `PIPE`, `STREAM`, `STAGE`.
4. **Roles and grants** — should they be migrated too? If yes, ask for the scope (`ACCOUNT`, `DATABASE`, or `SCHEMA`) and whether to consolidate future grants into `GRANT INHERITED`. This drives Step 3b.
5. **Target DCM project** — new or existing?
6. **Connection** — which Snowflake connection to use

**Path handling:** Files must be on the local filesystem — the DCM CLI requires it.

### Step 2: Resolve Target

**If new project:**

- Ask for the project identifier (`DB.SCHEMA.PROJECT_NAME`)
- The project's parent DB and schema CANNOT be defined inside the project itself
- Ask if multi-environment templating is needed
- **Fetch the current account identifier** from the active session before writing the manifest. Run:
  ```sql
  SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCOUNT_IDENTIFIER
  ```
  Use the returned value as `account_identifier` for the target matching the current connection (typically `DEV`). For additional targets that point to other accounts (e.g. `PROD`), leave a placeholder like `<PROD_ORG>-<PROD_ACCOUNT>` and tell the user to fill it in.
- Create local directory structure:
  ```
  <project_dir>/
  ├── manifest.yml
  └── sources/
      └── definitions/
  ```
- Create `manifest.yml` using this template:

  **Minimal manifest (no templating):**
  ```yaml
  manifest_version: 2
  type: DCM_PROJECT
  default_target: 'DEV'

  targets:
    DEV:
      account_identifier: '<ACCOUNT_IDENTIFIER>'   # from CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME()
      project_name: 'DB_NAME.SCHEMA_NAME.PROJECT_NAME'
      project_owner: DCM_DEVELOPER
  ```

  **With multi-environment templating:**
  ```yaml
  manifest_version: 2
  type: DCM_PROJECT
  default_target: 'DEV'

  targets:
    DEV:
      account_identifier: '<ACCOUNT_IDENTIFIER>'   # current session
      project_name: 'DB_NAME.SCHEMA_NAME.PROJECT_NAME_DEV'
      project_owner: DCM_DEVELOPER
      templating_config: 'DEV'
    PROD:
      account_identifier: '<PROD_ORG>-<PROD_ACCOUNT>'   # replace with the PROD account identifier
      project_name: 'DB_NAME.SCHEMA_NAME.PROJECT_NAME'
      project_owner: DCM_PROD_DEPLOYER
      templating_config: 'PROD'

  templating:
    defaults:
      db: "DEV_DB_NAME"
      wh_size: "XSMALL"
    configurations:
      DEV:
        db: "DEV_DB_NAME"
        wh_size: "XSMALL"
      PROD:
        db: "PROD_DB_NAME"
        wh_size: "LARGE"
  ```

— DCM auto-discovers all `.sql` files in `sources/definitions/`
- Run `snow dcm create` to register the project object in Snowflake

**If existing project:**

- Ask the user to specify the target path for the new definition files
- Locate `manifest.yml` and ask the user if a new target should be created for testing or which existing target should be used

**⚠️ MANDATORY STOPPING POINT**: Present the resolved target configuration (project identifier, connection, output directory) for user approval before proceeding.

### Step 3: Generate Definitions

First verify uv is available:

```bash
uv --version
```

If this fails, install uv before continuing:

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
# or: brew install uv
```

Run `ddl_to_dcm.py` using the command from the **Tools** section above, always passing `--output-path <project_dir>/sources/definitions`. Add `--schema-list` if only specific schemas should be migrated, and `--object-types` if only specific object types should be migrated. If the user chose **owned-only** in Step 1, add `--role <ROLE_NAME>` (using the role from `SELECT CURRENT_ROLE()`). Parse the JSON output from stdout.

**⚠️ MANDATORY STOPPING POINT**: Present results. Highlight ERROR, WARNING, UNSUPPORTED, INFO, and SAVED rows with counts. Call out `INFO` rows for stripped tag/policy attachments, since those associations will not be adopted. For BACKFILL warnings, ask whether to FQN-expand, leave as-is, or remove the clause. Confirm before proceeding.

### Step 3b: Generate Roles and Grants (Optional)

Skip this step if the user declined grant migration in Step 1.

Run `grants_to_dcm.py` using the command from the **Tools** section, passing the same `--output-path` as Step 3 so the two layouts merge. Use the scope the user chose, and add `--consolidate-inherited` only if they asked for it.

Before running, confirm the active connection's role is the role whose grants should be captured, because only grants where `granted_by = CURRENT_ROLE()` are emitted. If grants were made by several roles, the script must be run once per role.

**⚠️ MANDATORY STOPPING POINT**: Present the grant results. Report the number of role definitions, the number of grant statements, and every aggregated `UNSUPPORTED` category with its count. If `UNSUPPORTED` includes `grant by another role`, tell the user which role and ask whether to re-run as that role. If `--consolidate-inherited` produced `GRANT INHERITED` statements, remind the user that deploying them requires `FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED'`.

### Step 4: Integrate into Project

Review the generated definitions for objects that DCM does not support with DEFINE:

| Object Type | Support | Action |
|-------------|---------|--------|
| Database, Schemas | DEFINE | Kept in `schemas.sql` |
| Tables, Views, Dynamic Tables | DEFINE | Keep in `sources/definitions/` |
| Tasks | DEFINE | Keep in `sources/definitions/` |
| Functions, Procedures (including overloads) | DEFINE | Keep in `sources/definitions/` |
| Sequences, File Formats, Alerts, Tags | DEFINE | Keep in `sources/definitions/` |
| Masking Policies, Authentication Policies | DEFINE | Keep in `sources/definitions/`; the policy objects are migrated, their attachments are not |
| Pipes | DEFINE | Keep in `sources/definitions/` |
| Internal Stages | DEFINE | Keep in `sources/definitions/`; PLAN may show ALTER if non-default file format or copy options are configured — hand-tune if needed |
| External Stages with a storage integration | DEFINE | Keep in `sources/definitions/`; URL and storage integration are captured |
| External Stages with inline credentials | SKIP (reported) | Reported as UNSUPPORTED so secrets stay out of definition files; convert to a storage integration or manage outside DCM |
| External Stages without a storage integration | SKIP (reported) | Reported as UNSUPPORTED; manage outside DCM |
| Tag / policy attachments on tables and views | STRIPPED (reported as INFO) | Not supported via CREATE OR ALTER; re-apply manually after deploy if needed |
| Streams | DEFINE | Keep in `sources/definitions/`; assembled from `SHOW STREAMS` metadata, not `GET_DDL` |
| Semantic Views | SKIP (reported) | Recreate manually after deploy; the migration skips them |
| Data Metric Functions | SKIP (reported) | The TABLE-argument column name is not exposed by GET_DDL or DESCRIBE; recreate manually after deploy |
| Integrations, Network Rules | SKIP (reported) | Move to `pre_deploy.sql` |

**Concrete checks to perform:**

1. **Scan for unsupported objects:** Search definition files for `URL =` in stage definitions, `DEFINE INTEGRATION`, `DEFINE NETWORK RULE`. Move any matches to `pre_deploy.sql` or `post_deploy.sql` at the project root.

2. **Scan for Jinja conflicts:** Search definition files for literal `{{` or `}}` that are NOT Jinja template variables (e.g., SQL string manipulation like `'{{' || var || '}}'`). Wrap affected DEFINE blocks in `{% raw %}...{% endraw %}` to prevent Jinja parse errors during ANALYZE.

3. **Verify FQN completeness:** Search for any remaining bare object references that should be fully qualified. Look for `FROM <bare_name>`, `JOIN <bare_name>`, `INTO <bare_name>` where `<bare_name>` doesn't contain a `.`.

**If merging into an existing project:** Check for naming conflicts with existing definitions. Present any conflicts to the user for resolution.

### Step 5: Run ANALYZE

```bash
snow dcm raw-analyze -c <connection> --target <target> --from <project_dir>
```

Read and parse the output.

#### Common issues to fix

- **Missing FQN references** — the script expands same-schema references but may miss cross-schema references
- **Syntax issues from complex DDL** — some GET_DDL output may contain constructs that need manual adjustment
- **CTE in correlated subqueries** — DEFINE does not support CTEs referenced in correlated subqueries (replace with LEFT JOINs)

Fix errors in the definition files and re-run analyze until it passes cleanly.

### Step 6: Run PLAN

```bash
snow dcm plan -c <connection> --target <target> --save-output --from <project_dir>
```

Read `<project_dir>/out/plan/plan_result.json` and parse the operations.

#### Plan validation

The plan will not be a complete no-op. Each entity will show an `ALTER` that sets the DCM Project association (Project: `<project_name>`). This is expected and correct — it is how DCM records ownership of the object. Beyond these project-assignment ALTERs, the plan MUST show **zero changes** for existing objects. `GRANT` operations are also acceptable (additive). Any other `CREATE`, `ALTER`, or `DROP` operations indicate definition mismatches that need to be resolved.

Two expected exceptions:

- **Tag and policy associations are deliberately not adopted.** The migration strips `WITH TAG`, `WITH MASKING POLICY`, and `WITH ROW ACCESS POLICY` clauses because DCM does not support setting them via `CREATE OR ALTER`. The attachments remain on the live objects and are simply not managed by the project.
- **A view written with a trailing semicolon or unqualified references may show a harmless one-time difference** on the first plan. It clears itself after the first deploy.

#### Resolving mismatches by reverse-diffing the PLAN output

When PLAN reports an `ALTER` (or the detailed diff under a `CREATE OR REPLACE`), treat the plan output as the source of truth for what the live object actually has, then patch the DEFINE file to match. The plan direction is "DEFINE -> live", so **flip it** when updating the file:

- If PLAN says a property will change **from X to Y**, the current live value is X and the DEFINE currently resolves to Y. Update the DEFINE so it produces X.
- Example: PLAN shows `CHANGE_TRACKING: TRUE -> FALSE`. The live table has `CHANGE_TRACKING = TRUE`; the DEFINE is missing it (so it resolves to the default FALSE). Add `CHANGE_TRACKING = TRUE` to the `DEFINE TABLE` block.
- Example: PLAN shows `DATA_METRIC_SCHEDULE: 'TRIGGER_ON_CHANGES' -> '60 MINUTE'`. Add `DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES'` to the DEFINE.
- Example: PLAN shows a column dropped. The live table has the column; the DEFINE is missing it. Add the column (with the exact type from `GET_DDL`).
- Example: PLAN shows a CLUSTER BY being dropped. Add `CLUSTER BY (<keys>)` to the DEFINE.

Work through the diffs one object at a time. After each batch of fixes, re-run PLAN. Repeat until PLAN shows zero changes for all adopted objects. Only GRANT operations should remain.

If a diff cannot be resolved by DEFINE (e.g., the property is not supported in DEFINE syntax), surface it to the user and move the object to `pre_deploy.sql` / `post_deploy.sql` or exclude it from adoption.

**⚠️ MANDATORY STOPPING POINT**: Present the plan summary to the user:
- List of objects that will be adopted (zero changes)
- Any remaining mismatches
- Any new GRANT operations that will be applied

Get explicit approval before deploying.

### Step 7: Run DEPLOY

```bash
snow dcm deploy -c <connection> --target <target> --alias "migrate <source_db>" --from <project_dir>
```

After deploy, verify with `snow dcm list-deployments -c <connection> --from <project_dir>`.

**⚠️ MANDATORY STOPPING POINT**: Confirm deployment success to the user. Report:
- Deployment alias and timestamp
- Number of objects now under DCM management
- Any warnings from the deployment

### Step 8: Jinja Templating Analysis (Optional)

After adoption is complete, analyze the definitions for Jinja templating opportunities. This step is purely advisory — no changes are applied without approval.

**Load** `references/jinja_analysis.md` for detailed detection patterns.

Two analysis modes:

**A) User-requested parameterization** — The user specifies what to parameterize (e.g., database name, warehouse name, environment suffix):
1. Scan definitions for literal occurrences of the specified values
2. Propose `{{ variable }}` replacements with before/after examples
3. Propose `manifest.yml` additions (defaults + per-target configurations)

**B) Auto-detected patterns** — Scan without specific user direction:
1. **Literal value frequency** — Find values that appear in 3+ definitions (database names, warehouse references, role names)
2. **Structural repetition** — Find DEFINE blocks with identical structure but different names (macro candidates)
3. **Environment-specific values** — Find hardcoded sizes, retention periods, or environment names

Present findings as a categorized report:

**⚠️ MANDATORY STOPPING POINT**: Present the templating proposal. Do NOT apply changes until the user approves specific items.

If approved, make the changes and re-run PLAN + DEPLOY to validate the templated definitions produce the same result.

## Error Handling

**"Cannot access database":** Verify USAGE privilege and database name spelling (case-sensitive).

**GET_DDL errors:** Logged as ERROR rows. Common cause: insufficient privileges. Fix by granting access or using `--role` to filter to owned objects.

**WARNING rows:** A metadata lookup failed (semantic views, schemas, the database comment, or `INFORMATION_SCHEMA.COLUMNS`) and the corresponding filter or collision check is degraded for this run. Fix by granting `USAGE` on the database and `SELECT` on the relevant INFORMATION_SCHEMA views.

**INFO rows:** Not errors. They record a property gap-filled from `DESCRIBE AS RESOURCE` (for example `DATA_RETENTION_TIME_IN_DAYS` or `DATA_METRIC_SCHEDULE`) or an attachment clause stripped from a table or view. Review the stripped-attachment rows, since those associations will not be managed by the project.

**PLAN shows ALTER:** Usually column ordering or default value formatting differences. Compare line-by-line with `SELECT GET_DDL('TABLE', '<fqn>', TRUE)` (the `TRUE` parameter is required).

**PLAN shows ALTER STAGE:** A stage's inline file format or copy options cannot be read back from Snowflake, so they are not emitted. Add the missing clauses to the generated `.sql` file by hand and re-run PLAN.

**PLAN fails with `Property 'SHOW_INITIAL_ROWS' cannot be changed in CREATE OR ALTER`:** `SHOW_INITIAL_ROWS` cannot be read back from Snowflake (neither `SHOW STREAMS`, `DESCRIBE STREAM`, `DESCRIBE AS RESOURCE`, nor `GET_DDL` expose it) but `CREATE OR ALTER` still enforces it. Add `SHOW_INITIAL_ROWS = TRUE` to that stream's definition by hand and re-plan. Only a stream's comment is alterable in place; its source object, `APPEND_ONLY`, and `INSERT_ONLY` are immutable.

**ANALYZE syntax errors:** Check for unsupported DDL constructs (CTEs in correlated subqueries), or cross-database references missing FQN qualification.

**Unexpected grant statements missing:** `grants_to_dcm.py` only emits grants where `granted_by = CURRENT_ROLE()`. Check the aggregated `UNSUPPORTED` rows for `grant by another role` and re-run with a connection using that role.

**Stale grant files on re-run:** A `grants.sql` is written only when it has content, so a container whose grants have since been revoked keeps its old file. Delete the stale file manually.

## Stopping Points

- ✋ **Step 2** — Approve target configuration (project identifier, connection, output directory) before generating definitions
- ✋ **Step 3** — Review generation results (SAVED / ERROR / UNSUPPORTED / INFO counts, BACKFILL warnings) before integrating
- ✋ **Step 3b** — Review grant results (role definitions, grant counts, UNSUPPORTED categories) before integrating
- ✋ **Step 6** — Approve plan results (zero-change validation) before deploying
- ✋ **Step 7** — Confirm deployment success
- ✋ **Step 8** — Approve templating proposals before applying any changes

## Output

A DCM project managing the migrated database with:
- Definition files in `sources/definitions/` matching the existing object state
- Optional `roles.sql`, `grants.sql`, and `role_grants.sql` reproducing the caller's role setup
- Zero-change PLAN proves definitions match reality (except for new Project association)
- Successful DEPLOY adoption
- Optional: Jinja-templated definitions with variables and macros for multi-environment use
