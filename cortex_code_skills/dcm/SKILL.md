---
name: dcm
description: "Use for **ALL** requests that mention: create, build, set up, debug, fix, troubleshoot, optimize, improve, evaluate, or analyze a DCM project. This is the **REQUIRED** entry point - even if the request seems simple. DO NOT attempt to create DCM projects manually or search for DCM documentation - always invoke this skill first. This skill guides users through creating, auditing, evaluating, and debugging workflows for DCM (Database Change Management) projects. Triggers: DCM, DCM project, Database Change Management, snow dcm, manifest.yml with DEFINE, infrastructure-as-code, three-tier role pattern, database roles, DEFINE TABLE, DEFINE SCHEMA."
---

# DCM (Database Change Management) Skill

## When to Use

Use this skill when a user wants to:

- Create a new DCM project from scratch
- Modify an existing DCM project (with or without local source code)
- Define or modify Snowflake infrastructure (databases, schemas, tables, views, dynamic tables, tasks, warehouses, roles, grants)
- Set up data quality expectations and data metric functions
- Understand dependencies or lineage between objects in a DCM project
- Deploy changes to Snowflake infrastructure

## Prerequisites

- Active Snowflake connection (`-c <connection>` required for all DCM commands)
- Appropriate Snowflake privileges for the operations being performed
- For new projects: `CREATE DATABASE` or `CREATE SCHEMA` privileges as needed
- For deployments: privileges to create/alter/drop the objects defined in the project
- Snowflake CLI (`snow`) version 3.16 or later (recommended)

## ⚠️ MANDATORY INITIALIZATION

Before any DCM workflow, you MUST complete Steps 0, 1, and 2 as **sequential gates**. Each step MUST complete (including any required user response) before the next step begins. DO NOT batch these steps with other tool calls. Each gate requires its own turn in the conversation.

### Step 0: Check Snowflake CLI Version ✋ BLOCKING

Run `snow --version`.

**If version >= 3.16** → Proceed to Step 1.

**If version < 3.16** → Output the warning below, then STOP. Do not proceed to Step 1. Do not load any files. Do not call any tools. Your entire response for this turn must be ONLY the warning message. Wait for the user to respond.

> "⚠️ Your Snowflake CLI version is X.Y.Z. DCM works best with version 3.16 or later — some features may not work as expected on older versions.
> 
> To upgrade, run:
> ```
> pip install snowflake-cli --upgrade
> ```
> 
> You can continue with your current version, upgrade manually, or I can run the upgrade for you."

Do not run the upgrade unless the user explicitly asks you to. If they choose to continue, proceed to Step 1.

**🛑 END OF TURN. Output ONLY the warning above. Do not call any other tools. Do not read any files. Wait for user response.**

**If `snow` is not found** → Inform the user they need to install the Snowflake CLI before using DCM, then STOP.

### Step 1: Load Syntax Overview ✋ BLOCKING

**⚠️ Gate check: Only proceed here if Step 0 completed with version >= 3.16, or the user explicitly chose to continue with an older version.**

**Load** the syntax overview to understand DCM core principles:

1. **Load**: [reference/syntax_overview.md](reference/syntax_overview.md) - DCM syntax principles, supported entities, and primitive loading guide

**Additional references — load as needed per sub-skill guidance:**
- [reference/project_structure.md](reference/project_structure.md) - Manifest and project structure (load when creating/modifying manifests)
- [reference/cli_reference.md](reference/cli_reference.md) - CLI command details (load when running DCM commands)
- `reference/primitives/*.md` - Per-object-type syntax and examples (load only the primitives needed for the task — see the loading guide in syntax_overview.md)

**DO NOT PROCEED until you have loaded the syntax overview.**

### Step 2: Gather Required Information ✋ BLOCKING

For ALL DCM operations, you MUST collect:

1. **Target DCM Project Identifier** (fully qualified: `DATABASE.SCHEMA.PROJECT_NAME`)

   - This is the Snowflake object where the project is registered
   - Required for all `snow dcm` commands (except `list` that should be used to aid the user in selecting a project)
   - **⚠️ CRITICAL**: A DCM project CANNOT define its parent database or schema. If the project identifier is `MY_DB.MY_SCHEMA.MY_PROJECT`, you cannot use `DEFINE DATABASE MY_DB` or `DEFINE SCHEMA MY_DB.MY_SCHEMA` - these containers must already exist. You can only define objects *inside* the project's schema.

2. **Snowflake Connection** (`--connection` or `-c`)

   - The named connection to use for all operations
   - Ask user if not provided (or use default connection if not specified)

3. **Target Name** (if the project uses targets)
   - Check `manifest.yml` for available targets (DEV, PROD, etc.)
   - The `--target` flag selects a target from the manifest, which bundles the project identifier with a templating configuration
   - If omitted, the `default_target` from the manifest is used

**DO NOT PROCEED until you have confirmed these details with the user.**

## Intent Detection

When a user makes a request, detect their intent and follow the appropriate workflow.

**⚠️ MANDATORY SUB-SKILL LOADING**: When an intent below maps to a sub-skill file (marked with ✋ MUST Load), you **MUST** load that sub-skill file before doing any work. The inline workflow summaries later in this document are overviews only — they are **NOT sufficient** to complete the task correctly. The sub-skills contain critical details, examples, and guardrails that prevent common errors. **DO NOT** skip loading the sub-skill and attempt to follow only the inline workflow.

### CREATE Intent - User wants to create a new DCM project

**Trigger phrases**: "create project", "new project", "set up DCM", "start from scratch", "build infrastructure"

**→ ✋ MUST Load**: [create-project/SKILL.md](create-project/SKILL.md) — DO NOT write any files or run commands until this sub-skill is loaded.

### MODIFY_LOCAL Intent - User wants to modify an existing project with local source code

**Trigger phrases**: "modify", "update", "change", "add table", "edit definitions" (when source files are available locally)

**→ ✋ MUST Load**: [modify-project/SKILL.md](modify-project/SKILL.md) — DO NOT modify definitions until this sub-skill is loaded.

### DOWNLOAD_AND_MODIFY Intent - User wants to work with an existing deployed project (no local code)

**Trigger phrases**: "download project", "get sources", "work with existing project", "modify deployed project"

**→ ✋ MUST Load**: [modify-project/SKILL.md](modify-project/SKILL.md) (includes download workflow) — DO NOT download or modify until this sub-skill is loaded.

### ANALYZE Intent - User wants to understand dependencies or check for errors

**Trigger phrases**: "analyze", "check dependencies", "lineage", "what depends on", "validate"

**→ Follow**: [Analyze Project Workflow](#workflow-4-analyze-project)

### IMPORT_EXISTING Intent - User wants to import/adopt existing Snowflake objects into DCM

**Trigger phrases**: "import existing", "adopt objects", "bring into DCM", "convert to DCM", "add existing table"

**→ Follow**: [Adopting Existing Objects Workflow](#workflow-5-adopting-existing-objects)

### ROLE_GRANT_GUIDELINES Intent - User needs guidance on roles/grants in DCM

**Trigger phrases**: "dcm role", "dcm grant", "roles in dcm", "grants in dcm project", "dcm permission model", "dcm warehouse grant error", "define roles in dcm"

**→ ✋ MUST Load**: [roles-and-grants/SKILL.md](roles-and-grants/SKILL.md) — DO NOT give grant advice until this sub-skill is loaded.

### DEPLOY Intent - User wants to deploy changes

**Trigger phrases**: "deploy", "apply changes", "push to Snowflake"

**→ ✋ MUST Load**: [deploy-project/SKILL.md](deploy-project/SKILL.md) — DO NOT run plan or deploy commands until this sub-skill is loaded.

## Core Workflows

### Workflow 1: Create New Project

This workflow is fully documented in [create-project/SKILL.md](create-project/SKILL.md).
You **MUST** load that sub-skill before writing any files or running commands.

### Workflow 2: Modify Existing Project

This workflow is fully documented in [modify-project/SKILL.md](modify-project/SKILL.md).
You **MUST** load that sub-skill before modifying any definitions.

### Workflow 3: Download and Modify Existing Project

This workflow is fully documented in [modify-project/SKILL.md](modify-project/SKILL.md) (includes the download workflow).
You **MUST** load that sub-skill before downloading or modifying any project.

### Workflow 4: Analyze Project

```
User wants to understand dependencies or validate
    ↓
1. Run analyze:
   snow dcm raw-analyze <identifier> -c <connection> \
     --target <config> 
    ↓
2. ⚠️ CRITICAL: Read and parse command output
   - This step is MANDATORY, not optional
   - Check for errors at file and definition levels
   - Extract dependency information
   - Extract column-level lineage
    ↓
3. If errors exist:
   - Report them to the user
   - Fix issues in definition files
   - Rerun analyze
    ↓
4. Present findings to user:
   - List of objects defined
   - Dependencies between objects
   - Any errors or warnings
   - Column lineage (if requested)
```

### Workflow 5: Adopting Existing Objects

```
User wants to import existing Snowflake objects into DCM
    ↓
1. Identify the objects to adopt:
   - Ask user which objects to import
   - Get fully qualified names
    ↓
2. Get current DDL for each object:
   SELECT GET_DDL('TABLE', 'DB.SCHEMA.TABLE');
   SELECT GET_DDL('VIEW', 'DB.SCHEMA.VIEW');
   SELECT GET_DDL('STAGE', 'DB.SCHEMA.STAGE');
    ↓
2.5. ⚠️ MANDATORY: Categorize Objects by DCM Support
   - **Stages**: Check for URL parameter
     ✅ No URL (internal) → Use DEFINE STAGE
     ⚠️ Has URL (external) → Place in post_deploy.sql
   - **Grants**: Load roles-and-grants/SKILL.md
     ✅ Supported → include in definitions
     ⚠️ Workaround needed → warehouse grants need account role
     ❌ Unsupported → document in post_deployment_grants.sql
   - **Other Objects**: Tables, Views, Warehouses → DEFINE
   - **Unsupported Objects**: Streams, Alerts → post_deploy.sql; Integrations → pre_deploy.sql
   - Present categorized analysis to user
   - ⚠️ CHECKPOINT: Get explicit approval before proceeding
    ↓
3. Convert supported objects (CREATE to DEFINE):
   - Replace CREATE keyword with DEFINE for supported objects
   - Internal stages: CREATE STAGE → DEFINE STAGE
   - Preserve all properties exactly
   - Keep grants separate (handle per step 2.5 analysis)
   - External stages/streams/alerts go to companion scripts (not DEFINE)
    ↓
4. Add definitions to project files:
   - DEFINE statements → appropriate .sql files
   - Unsupported objects → pre_deploy.sql or post_deploy.sql (see unsupported_objects.md)
   - Unsupported grants → post_deployment_grants.sql
    ↓
5. Run analyze and READ command output:
   - Verify objects appear in definitions
    ↓
6. Run plan and READ out/plan/plan_result.json:
   - ⚠️ VERIFY: Plan should show ZERO changes for adopted objects
   - If plan shows CREATE/ALTER, the definition doesn't match
   - Adjust definition to match existing object exactly
    ↓
7. Repeat until plan shows no changes for adopted objects
```

**Key Point:** Successful adoption = plan shows NO operations for the adopted objects. They should appear in analyze but result in zero changes in plan.

### Workflow 6: Deploy Changes

This workflow is fully documented in [deploy-project/SKILL.md](deploy-project/SKILL.md).
You **MUST** load that sub-skill before running plan or deploy commands.

⚠️ **CRITICAL: NEVER deploy without running plan first and getting explicit user confirmation.**

## Workflow Decision Tree

```
Start Session
    ↓
MANDATORY: Load syntax_overview.md (primitives loaded on-demand by sub-skills)
    ↓
Gather: Project identifier, Connection, Configuration
    ↓
Detect User Intent
    ↓
    ├─→ CREATE → ✋ MUST Load create-project/SKILL.md BEFORE writing any files
    │   (Triggers: "create project", "new project", "set up DCM")
    │   ⚠️ If roles/grants/permissions mentioned:
    │      → ALSO MUST load roles-and-grants/SKILL.md
    │
    ├─→ MODIFY_LOCAL → ✋ MUST Load modify-project/SKILL.md BEFORE modifying
    │   (Triggers: "modify", "update", "add table" with local files)
    │
    ├─→ DOWNLOAD_AND_MODIFY → ✋ MUST Load modify-project/SKILL.md BEFORE downloading
    │   (Triggers: "download project", "get sources", "work with existing")
    │
    ├─→ IMPORT_EXISTING → Follow Adopting Existing Objects workflow
    │   (Triggers: "import existing", "adopt", "bring into DCM", "convert DDL")
    │   → Get DDL → ⚠️ Analyze grants first → Convert to DEFINE
    │   → ALWAYS load roles-and-grants/SKILL.md for grant analysis
    │   → Verify plan shows zero changes for adopted objects
    │
    ├─→ ROLE_GRANT_GUIDELINES → ✋ MUST Load roles-and-grants/SKILL.md
    │   (Triggers: "dcm role", "dcm grant", "roles in dcm", "dcm permission model")
    │   → Recommended patterns for roles and grants in DCM
    │
    ├─→ ANALYZE → Run analyze workflow
    │   (Triggers: "analyze", "check dependencies", "lineage")
    │   ⚠️ MUST read command output after running
    │
    └─→ DEPLOY → ✋ MUST Load deploy-project/SKILL.md BEFORE running plan/deploy
        (Triggers: "deploy", "apply changes")
            ↓
        ALWAYS: analyze → plan → READ OUTPUT FILES → user confirmation → deploy
            ↓
        If tests exist: offer to run tests
```

## Sub-Skills

| Sub-Skill                                          | Purpose                                     | When to Load                  |
| -------------------------------------------------- | ------------------------------------------- | ----------------------------- |
| [create-project/SKILL.md](create-project/SKILL.md) | Create new DCM project from scratch         | CREATE intent                 |
| [modify-project/SKILL.md](modify-project/SKILL.md) | Modify existing project (local or download) | MODIFY/DOWNLOAD/IMPORT intent |
| [deploy-project/SKILL.md](deploy-project/SKILL.md) | Safe deployment with confirmation           | DEPLOY intent                 |
| [roles-and-grants/SKILL.md](roles-and-grants/SKILL.md) | Best practices for roles/grants in DCM | Role patterns, grant errors, permission models |

**Note:** The IMPORT_EXISTING workflow (adopting existing objects) is documented in [modify-project/SKILL.md](modify-project/SKILL.md) and in [Workflow 5: Adopting Existing Objects](#workflow-5-adopting-existing-objects).

**Note:** For role and grant guidance (recommended patterns, handling warehouse constraints, unsupported grant types), load the **roles-and-grants** skill.

## Rules

### Running Scripts

When running scripts from this skill:

1. Use bash to run the download script:

   ```bash
   bash <skill-dir>/scripts/download_project.sh <project_name> \
     --connection <connection> \
     --target <target_folder>
   ```

2. Do not `cd` into the skill directory - run from the user's working directory.

### DCM Command Patterns

All DCM commands follow this pattern:

```bash
snow dcm <command> <identifier> -c <connection> [options]
```

**Common options:**

- `--target <name>`: Use specific target from manifest.yml (bundles project identifier + templating config)
- `--format json`: Get machine-readable output (for list commands)

### Definition Syntax Rules

1. **Use DEFINE, not CREATE** for named objects:

   ```sql
   DEFINE TABLE MY_DB.MY_SCHEMA.MY_TABLE (
       id NUMBER,
       name VARCHAR
   );
   ```

2. **Always use fully qualified names**:

   ```sql
   DEFINE TABLE database.schema.table_name (...);
   ```

3. **Grants use standard SQL syntax** (imperative, not DEFINE):

   ```sql
   GRANT SELECT ON TABLE MY_DB.MY_SCHEMA.MY_TABLE TO ROLE MY_ROLE;
   ```

4. **Data quality expectations** use ATTACH syntax:
   ```sql
   ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
       TO TABLE MY_DB.RAW.MY_TABLE
       ON (column_name)
       EXPECTATION NO_NULLS (value = 0);
   ```

### ⚠️ CRITICAL: Reading Output Files

**After running `plan`, you MUST read and parse the output JSON files:**

- `out/plan/plan_result.json` - after plan

**This is MANDATORY, not optional.** The agent must:

1. Read the JSON file
2. Parse the content
3. Check for errors or issues
4. Report findings to the user
5. Fix any issues before proceeding

**If plan output already exists** and user asks for a summary, **read the existing file** instead of rerunning unless explicitly requested.

### Safety Rules

1. **NEVER deploy without running plan first**
2. **NEVER deploy without explicit user confirmation**
3. **ALWAYS highlight DROP and data-affecting ALTER operations**
4. **ALWAYS suggest using --alias for deployments** to track deployment history
5. **ALWAYS read and parse output JSON files** after analyze/plan commands
6. **If you encounter `ATTACH PRE_HOOK` or `ATTACH POST_HOOK`** in any definition file, inform the user that DDL hooks are not supported in the current version of DCM. Offer to extract the hook contents into `pre_deploy.sql` / `post_deploy.sql` companion scripts at the project root. ⚠️ Warn that companion scripts do NOT support Jinja — any `{{ }}` variables must be replaced with literal values or shell variable substitution.

### When Creating Definitions

1. **Clarify requirements before writing code**:

   - Ask about object names
   - Confirm column names and types
   - Verify relationships between objects
   - Understand configuration needs (multi-environment?)

2. **Propose structure and get confirmation**:

   - Present proposed definitions to user
   - Wait for approval before writing files

3. **Use appropriate file organization** (all files go in `sources/definitions/`):
   - `infrastructure.sql`: Databases, schemas, warehouses, **internal stages**
   - `tables.sql` or `raw.sql`: Table definitions
   - `analytics.sql`: Dynamic tables, transformations
   - `serve.sql`: Views for consumption
   - `access.sql`: Roles, grants, permissions
   - `expectations.sql`: Data quality rules

## Common Use Cases

### Creating a Data Pipeline

1. Define source tables with CHANGE_TRACKING = TRUE
2. Define dynamic tables for transformations (or tasks for procedural ETL)
3. Define views for consumption
4. Define tasks for scheduled operations and orchestration
5. Define roles and grants for access control
6. Optionally add data quality expectations

### Adopting Existing Objects into DCM

When a user wants to "import" or "adopt" existing Snowflake objects:

1. **Get current DDL**: `SELECT GET_DDL('TABLE', 'fully.qualified.name')`
2. **Categorize the object**:
   - ✅ **Internal stages** (no URL) → Convert to `DEFINE STAGE`
   - ⚠️ **External stages** (with URL parameter) → Place in `post_deploy.sql`
   - ✅ **Tables, Views, Warehouses** → Convert to `DEFINE`
   - ⚠️ **Streams, Alerts** → Place in `post_deploy.sql`
   - ⚠️ **Integrations** → Place in `pre_deploy.sql`
3. **Convert CREATE to DEFINE** (for supported objects): Replace the keyword only
4. **Add to DCM project definitions**: Place in appropriate .sql file
5. **Run analyze**: Verify object appears in definitions
6. **Run plan and READ the output**:
   - ⚠️ Plan should show **ZERO changes** for adopted objects
   - If plan shows CREATE/ALTER, definition doesn't match exactly
   - Adjust definition until plan shows no changes

**Success criteria:** Adopted objects appear in analyze but result in zero operations in plan.

### Multi-Environment Setup

1. Define targets in manifest.yml (DEV, PROD) with corresponding `templating` configurations
2. Ensure each target on the same account has a unique `project_name` (e.g., `MY_PROJECT_DEV`, `MY_PROJECT_STG`, `MY_PROJECT_PROD`) -- targets with the same `project_name` on the same account will deploy over each other
3. Use Jinja variables in definitions: `{{env_suffix}}`, `{{wh_size}}`
4. Use `--target` flag to select the target (which resolves both project identifier and templating config)
5. Use `templating.defaults` for shared values and configurations for overrides
6. Use Jinja dictionaries for per-resource configuration (e.g., team-specific warehouse sizes, retention policies)

### Inspecting dbt Pipelines

When user asks to create DCM from dbt models:

1. Read dbt model files to understand transformations
2. Create corresponding dynamic table definitions
3. Preserve the DAG structure in DCM

## Error Handling

When commands fail, check:

1. **Connection issues**: Verify connection name is correct
2. **Permission errors**: Ensure user has required privileges
3. **Analysis errors**: Review errors in analyze output JSON
4. **Plan failures**: Check the `error` field in plan output

For debugging, suggest: `snow dcm <command> --debug`

## Related Documentation

- [DCM Syntax Overview](reference/syntax_overview.md) - Core principles and primitive loading guide
- [Project Structure Guide](reference/project_structure.md) - Manifest and project layout
- [CLI Command Reference](reference/cli_reference.md) - All `snow dcm` commands
- `reference/primitives/` - Per-object-type syntax and examples (loaded on-demand)
