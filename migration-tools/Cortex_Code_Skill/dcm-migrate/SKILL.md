---
name: dcm-migrate
description: "Bulk-migrate existing Snowflake objects into a new or existing DCM project using the ddl_to_dcm.py script. Converts DDL to DEFINE syntax, validates with PLAN (zero-change check), adopts via DEPLOY, and analyzes definitions for Jinja templating opportunities. Use when: migrating a database to DCM, bulk-importing objects, adopting existing infrastructure, converting DDL to DCM definitions. Triggers: migrate to DCM, import database, adopt objects, bulk import, DDL to DCM, convert database to DCM, migrate database."
---

# DCM Migrate

Bulk-migrate existing Snowflake database objects into a new or existing DCM project. Supports two execution modes:

- **Local mode** (CLI + uv available): Runs `ddl_to_dcm.py` via Snowpark, then validates with `snow dcm plan` and adopts with `snow dcm deploy`.
- **Workspaces mode** (Snowflake Workspaces / Snowsight IDE): Performs DDL→DEFINE conversion inline via SQL, writes files to the workspace filesystem, then prompts the user to run PLAN and DEPLOY through the Workspaces UI.

The agent auto-detects the environment at the start of the workflow.

## When to Use

- Migrating an entire database (or selected schemas) into DCM management
- Bulk-importing many existing objects at once (vs. the manual one-by-one adoption in the bundled DCM skill)

## When NOT to Use

- Adopting 1-3 individual objects — use the bundled DCM skill's IMPORT_EXISTING workflow instead
- Creating a new project from scratch with no existing objects — use the bundled DCM skill's CREATE workflow

## Prerequisites

- **Local mode:** `snow` CLI 3.16+, `uv` installed
- **Workspaces mode:** No CLI tools required; PLAN/DEPLOY run via the Workspaces UI
- **Both modes:** Active Snowflake connection with a role that can `GET_DDL` every target object (see **Role visibility** below). For new projects: `CREATE DCM PROJECT` privilege in the target schema.

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

Scans a database, retrieves DDL for all objects (tables, views, dynamic tables, tasks, SQL functions, SQL procedures, sequences, file formats, alerts, tags, internal stages), converts `CREATE` to `DEFINE`, expands references to fully qualified names, and writes `.sql` definition files to a local directory. Also extracts grants at the database, schema, and object level and writes `grants.sql` files for reference.

Automatically skips (reported as UNSUPPORTED):
- **Semantic views** (not yet supported by DCM)
- **Non-SQL functions and procedures** (Python, Java, JavaScript, Scala)
- **External stages** (have a URL — manage outside DCM)
- **OWNERSHIP grants** (implicit from object creation)
- **Future grants** (do not apply to migrated objects)

Silently skipped (no output row): streams, temporary internal stages.

Internal stages (without URL) are converted to `DEFINE STAGE` from `SHOW STAGES` metadata (directory flag + comment only). File format and copy options are not captured — PLAN may show `ALTER STAGE` drift for stages with non-default settings.

When an unsupported row appears in the output, the reason is given in the `file_path` column (e.g. `semantic views`, `language=PYTHON`, `OWNERSHIP grant`, `future grant (SELECT)`).

**Usage:**
```bash
SNOWFLAKE_CONNECTION_NAME=<connection> uv run --project <SKILL_DIR> \
  python <SKILL_DIR>/scripts/ddl_to_dcm.py \
  --database <DB_NAME> \
  --output <PROJECT_DIR>/sources/definitions \
  [--schemas SCHEMA1 SCHEMA2 ...] \
  [--group-by-type] \
  [--role <ROLE_NAME>]
```

**Arguments:**
- `--database` (required): Source database name
- `--output` (required): Local directory for generated definition files
- `--schemas` (optional): Space-separated schema allow-list; omit for all schemas
- `--group-by-type` (optional): Write one file per object type per schema (recommended for large databases)
- `--connection` (optional): Snowflake connection name override. Normally use the `SNOWFLAKE_CONNECTION_NAME` env var instead.
- `--role` (optional): Only migrate objects owned by this role. Filters schemas, tables, views, dynamic tables, tasks, functions, procedures, sequences, file formats, alerts, tags, and internal stages by the `owner` column. Recommended for non-ACCOUNTADMIN users to avoid permission errors on unowned objects.

**Output:** JSON array to stdout with `{schema, object_type, object_name, status, file_path}` per object. Summary to stderr. Statuses: `SAVED`, `ERROR`, `UNSUPPORTED` (with reason in `file_path`), `WARNING` (degraded filter). When `--role` is used, also prints matched object count.

### CLI Commands

- **`snow dcm ...`** — DCM lifecycle commands (create, raw-analyze, plan, deploy). Always pass `-c <connection>`.

## Workflow

```
Step 0: Environment Detection (auto)
  ├─→ Local mode (uv + snow CLI available)
  └─→ Workspaces mode (Snowsight IDE)
  ↓
Step 1: Gather Context
  ↓
Step 2: Resolve Target (new or existing project?)
  ├─→ New project → Create project + manifest + directory
  └─→ Existing project → Locate manifest, download sources if needed
  ↓
  ⚠️ STOP: Approve target configuration
  ↓
Step 3: Generate Definitions
  ├─→ Local: run ddl_to_dcm.py via uv
  └─→ Workspaces: inline SQL conversion by the agent
  ↓
  ⚠️ STOP: Review generation results
  ↓
Step 4: Integrate into Project (handle unsupported objects)
  ↓
Step 5: Run ANALYZE → fix errors
  ├─→ Local: snow dcm raw-analyze CLI
  └─→ Workspaces: prompt user to run via UI
  ↓
Step 6: Run PLAN → validate zero changes
  ├─→ Local: snow dcm plan CLI
  └─→ Workspaces: prompt user to run via UI
  ↓
  ⚠️ STOP: Present plan results, iterate if mismatches
  ↓
Step 7: Run DEPLOY to adopt
  ├─→ Local: snow dcm deploy CLI
  └─→ Workspaces: prompt user to run via UI
  ↓
  ⚠️ STOP: Confirm deployment success
  ↓
Step 8: Jinja Templating Analysis (optional)
  ↓
  ⚠️ STOP: Present templating proposals
```

### Step 0: Environment Detection

Before gathering user input, detect the execution environment:

1. Check if `uv` is available: run `which uv`
2. Check if `snow` CLI is available: run `which snow`

**If both are available → Local mode.** Proceed with the script-based workflow.

**If either is missing → Workspaces mode.** Inform the user:
> "Detected Workspaces environment (no local CLI tools). I'll generate definitions inline using SQL and you'll run PLAN/DEPLOY through the Workspaces UI."

Store the detected mode and branch at Steps 3, 5, 6, and 7.

### Step 1: Gather Context

**Role detection (mandatory first step):**

1. Run `SELECT CURRENT_ROLE()` and present the role
2. If the role is **not** ACCOUNTADMIN and does **not** hold `MANAGE GRANTS`, warn that database ownership does not cascade to child objects. Recommend **owned-only** migration or switching to ACCOUNTADMIN.
3. Ask the user: **owned-only** (recommended) or **all objects**? This determines whether `--role` is passed in Step 3.

Collect from the user:

1. **Source database** (required)
2. **Schema allow-list** (optional; omit for all schemas)
3. **Target DCM project** — new or existing?
4. **Connection** — which Snowflake connection to use
5. **Group by type** — one file per type per schema (recommended) or one file per object

**Path handling:** Files must be on the local filesystem (DCM CLI requires it). If the user specifies a workspace/stage path, explain this and offer to sync after migration via `snow stage copy`.

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
- Create `manifest.yml` using this template (substitute `<ACCOUNT_IDENTIFIER>` with the value fetched above):

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

  **CRITICAL:** The only valid top-level keys are `manifest_version`, `type`, `default_target`, `targets`, and `templating`. Any other keys (like `include_definitions`, `project`) will cause schema validation errors.

- Place definitions in `sources/definitions/` — manifest v2 auto-discovers all `.sql` files in this directory
- Run `snow dcm create` to register the project object in Snowflake

**If existing project:**

- Locate `manifest.yml` and confirm the target
- If no local sources exist, download them using `snow dcm list-deployments` + `snow stage copy`

**⚠️ MANDATORY STOPPING POINT**: Present the resolved target configuration (project identifier, connection, output directory) for user approval before proceeding.

### Step 3: Generate Definitions

#### Local mode

Run `ddl_to_dcm.py` using the command from the **Tools** section above, with `--output <project_dir>/sources/definitions --group-by-type`. Add `--schemas` if only specific schemas should be migrated. If the user chose **owned-only** in Step 1, add `--role <ROLE_NAME>` (using the role from `SELECT CURRENT_ROLE()`). Parse the JSON output from stdout.

#### Workspaces mode

Replicate the logic of `ddl_to_dcm.py` inline using SQL queries and agent file writes:

1. **Discover objects:** `SHOW OBJECTS IN DATABASE`, `SHOW SCHEMAS IN DATABASE`, `SHOW SEMANTIC VIEWS IN DATABASE` (to exclude semantic views). Per schema: `SHOW TASKS`, `SHOW USER FUNCTIONS`, `SHOW USER PROCEDURES`, `SHOW SEQUENCES`, `SHOW FILE FORMATS`, `SHOW ALERTS`. If owned-only, filter by the `owner` column. Query `INFORMATION_SCHEMA.PROCEDURES`/`FUNCTIONS` to skip non-SQL callables.

2. **Convert DDL:** For each object, run `GET_DDL`, replace `CREATE` with `DEFINE`, uppercase the type keyword, expand same-schema bare references to FQNs. For functions/procedures, use the signature form for `GET_DDL` and expand the bare name to a quoted FQN.

3. **Extract grants:** `SHOW GRANTS ON DATABASE/SCHEMA/each object` and `SHOW FUTURE GRANTS IN DATABASE/SCHEMA`. Write non-OWNERSHIP grants to `grants.sql` files. Report OWNERSHIP and future grants as UNSUPPORTED.

4. **Write files** using the directory structure from the Tools section. Scan for `BACKFILL FROM <bare_name>` warnings.

**⚠️ MANDATORY STOPPING POINT**: Present results. Highlight ERROR, WARNING, UNSUPPORTED, and SAVED rows with counts. For BACKFILL warnings, ask whether to FQN-expand, leave as-is, or remove the clause. Confirm before proceeding.

### Step 4: Integrate into Project

Review the generated definitions for objects that DCM does not support with DEFINE:

| Object Type | Support | Action |
|-------------|---------|--------|
| Tables, Views, Dynamic Tables | DEFINE | Keep in `sources/definitions/` |
| Tasks | DEFINE | Keep in `sources/definitions/` |
| Functions, Procedures (SQL only) | DEFINE | Keep in `sources/definitions/` |
| Sequences, File Formats, Alerts, Tags | DEFINE | Keep in `sources/definitions/` |
| Internal Stages (no URL) | DEFINE | Keep in `sources/definitions/`; PLAN may show ALTER if non-default file format or copy options are configured — hand-tune if needed |
| External Stages (with URL) | Unsupported | Reported as UNSUPPORTED; manage outside DCM |
| Streams | Not yet handled | Silently skipped by the migration |
| Semantic Views | Unsupported | Recreate manually after deploy; the migration skips them |
| Non-SQL Functions/Procedures (Python, Java, etc.) | Unsupported | Recreate manually after deploy; the migration skips them |
| Integrations, Network Rules | Unsupported | Move to `pre_deploy.sql` |

**Concrete checks to perform:**

1. **Scan for unsupported objects:** Search definition files for `URL =` in stage definitions, `DEFINE STREAM`, `DEFINE INTEGRATION`, `DEFINE NETWORK RULE`. Move any matches to `pre_deploy.sql` or `post_deploy.sql` at the project root.

2. **Scan for Jinja conflicts:** Search definition files for literal `{{` or `}}` that are NOT Jinja template variables (e.g., SQL string manipulation like `'{{' || var || '}}'`). Wrap affected DEFINE blocks in `{% raw %}...{% endraw %}` to prevent Jinja parse errors during ANALYZE.

3. **Verify FQN completeness:** Search for any remaining bare object references that should be fully qualified. Look for `FROM <bare_name>`, `JOIN <bare_name>`, `INTO <bare_name>` where `<bare_name>` doesn't contain a `.`.

**If merging into an existing project:** Check for naming conflicts with existing definitions. Present any conflicts to the user for resolution.

### Step 5: Run ANALYZE

#### Local mode

```bash
snow dcm raw-analyze -c <connection> --target <target> --from <project_dir>
```

Read and parse the output.

#### Workspaces mode

Prompt the user to run **Analyze** via the Workspaces sidebar and share the results.

#### Common issues to fix

- **Missing FQN references** — the script expands same-schema references but may miss cross-schema references
- **Syntax issues from complex DDL** — some GET_DDL output may contain constructs that need manual adjustment
- **CTE in correlated subqueries** — DEFINE does not support CTEs referenced in correlated subqueries (replace with LEFT JOINs)

Fix errors in the definition files and re-run analyze until it passes cleanly.

### Step 6: Run PLAN

#### Local mode

```bash
snow dcm plan -c <connection> --target <target> --save-output --from <project_dir>
```

Read `<project_dir>/out/plan/plan_result.json` and parse the operations.

#### Workspaces mode

Prompt the user to run **Plan** via the Workspaces sidebar and share the results.

#### Plan validation

The plan MUST show **zero changes** for existing objects. `GRANT` operations are acceptable (additive). `CREATE`, `ALTER`, or `DROP` operations indicate definition mismatches.

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

#### Local mode

```bash
snow dcm deploy -c <connection> --target <target> --alias "migrate <source_db>" --from <project_dir>
```

After deploy, verify with `snow dcm list-deployments -c <connection> --from <project_dir>`.

#### Workspaces mode

Prompt the user to run **Deploy** (alias: `migrate <source_db>`) via the Workspaces sidebar and confirm success.

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

**Environment detection fails:** If `which uv` or `which snow` returns non-zero, assume Workspaces mode. If the user explicitly says they are in Workspaces, switch regardless.

**"Cannot access database" (local mode):** Verify USAGE privilege and database name spelling (case-sensitive).

**GET_DDL errors:** Logged as ERROR rows. Common cause: insufficient privileges. Fix by granting access or using `--role` to filter to owned objects.

**WARNING rows:** A filtering lookup failed (semantic views or INFORMATION_SCHEMA language). Non-SQL callables or semantic views may not be correctly filtered. Fix by granting `USAGE` on the database and `SELECT` on the relevant INFORMATION_SCHEMA views.

**PLAN shows ALTER:** Usually column ordering or default value formatting differences. Compare line-by-line with `SELECT GET_DDL('TABLE', '<fqn>', TRUE)` (the `TRUE` parameter is required).

**ANALYZE syntax errors:** Check for unsupported DDL constructs (CTEs in correlated subqueries), or cross-database references missing FQN qualification.

## Output

A DCM project managing the migrated database with:
- Definition files in `sources/definitions/` matching the existing object state
- Zero-change PLAN validation (definitions match reality)
- Successful DEPLOY adoption
- Optional: Jinja-templated definitions with variables and macros for multi-environment use
