---
name: create-project
description: "Create new DCM projects from scratch. Triggers: new project, create dcm project, set up dcm, start from scratch, build infrastructure"
---

# Create New DCM Project

## Overview

This sub-skill guides you through creating a new DCM project from scratch, including project registration in Snowflake, local file structure creation, and initial definition writing.

## Prerequisites

- Snowflake connection name
- Target database and schema for the project (must exist)
- Understanding of what infrastructure the user wants to define

## Step-by-Step Workflow

### Step 1: Gather Project Requirements

**Ask the user:**

1. **Project identifier**: What should the project be named?

   - Format: `DATABASE.SCHEMA.PROJECT_NAME`
   - Example: `MY_DB.PROJECTS.ANALYTICS_PIPELINE`
   - **⚠️ CRITICAL**: The DATABASE and SCHEMA in the identifier are the *parent containers* where the project lives. You CANNOT define these containers in the project itself. For example, if the project is `MY_DB.MY_SCHEMA.MY_PROJECT`, you cannot use `DEFINE DATABASE MY_DB` or `DEFINE SCHEMA MY_DB.MY_SCHEMA`. You can only define objects *inside* `MY_DB.MY_SCHEMA`.

The user can provide the response in a freeform manner, ask clarification if needed.

2. **What infrastructure to define?**

   - Databases and schemas
   - Tables (source tables, staging tables)
   - Dynamic tables (transformations)
   - Views (consumption layer)
   - Internal stages (for data loading/unloading)
   - External stages (S3, Azure, GCS)
   - Tasks (scheduled operations, ETL orchestration)
   - Warehouses
   - Roles and grants
   - Data quality expectations
   - etc.

The user might provide some of this information in the initial prompt, so ask clarification questions as needed.

**⚠️ Stage-Specific Guidance:**
   - Internal stages (no URL) → Use `DEFINE STAGE` in infrastructure.sql
   - External stages (S3/Azure/GCS with URL) → Place in `post_deploy.sql` (not supported by DEFINE)

2.5. **⚠️ If roles/grants are needed, clarify:**

   - **Role hierarchy pattern?** (e.g., ADMIN → DEVELOPER → ANALYST)
   - **Warehouse access?** Which roles need warehouse usage?
     - ⚠️ Warehouse grants CANNOT go to database roles (Snowflake constraint)
     - Will need an account role for warehouse access
   - **Any privileged grants?** (ON ACCOUNT, IMPORTED PRIVILEGES, etc.)
     - These are unsupported in DCM and need post-deployment manual application
   - **User assignments?** Which users get which roles?

   **→ Load** [../roles-and-grants/SKILL.md](../roles-and-grants/SKILL.md) for recommended patterns.

3. **Multi-environment support?**
   - Will this deploy to DEV/PROD separately? (requires separate targets in manifest)
   - What differs between environments? (database names, warehouse sizes, users)
   - Each target can point to a different DCM project identifier

### Step 2: Verify Prerequisites

Check that the target database and schema exist:

```sql
SHOW DATABASES LIKE '<database>';
SHOW SCHEMAS IN DATABASE <database> LIKE '<schema>';
```

If they don't exist, prompt the user:

> "The database/schema for your DCM project doesn't exist. Would you like me to create them first, or would you prefer to create them manually?"

### Step 3: Create DCM Project in Snowflake

Register the project:

```bash
snow dcm create <DATABASE.SCHEMA.PROJECT_NAME> -c <connection>
```

Expected output: Confirmation that the project was created.

### Step 4: Create Local Project Structure

Create this directory structure:

    project/
    ├── manifest.yml
    └── sources/
        ├── definitions/
        │   └── (definition .sql files here)
        └── macros/
            └── (optional global macro .sql files here)

For manifest.yml schema and target configuration, see [../reference/project_structure.md](../reference/project_structure.md).

### Step 5: Create manifest.yml

Use ONLY the fields shown in these examples. See [../reference/project_structure.md](../reference/project_structure.md) for the full v2 schema.

**Without templating (single environment):**

```yaml
manifest_version: 2
type: DCM_PROJECT
default_target: 'DEV'

targets:
  DEV:
    account_identifier: MY_ACCOUNT
    project_name: 'DATABASE.SCHEMA.PROJECT_NAME'
    project_owner: DCM_DEVELOPER
```

**With templating (multi-environment):**

```yaml
manifest_version: 2
type: DCM_PROJECT
default_target: 'DEV'

targets:
  DEV:
    account_identifier: DEV_ACCOUNT
    project_name: 'DATABASE.SCHEMA.PROJECT_NAME_DEV'
    project_owner: DCM_DEVELOPER
    templating_config: 'DEV'
  PROD:
    account_identifier: PROD_ACCOUNT
    project_name: 'DATABASE.SCHEMA.PROJECT_NAME'
    project_owner: DCM_PROD_DEPLOYER
    templating_config: 'PROD'

templating:
  defaults:
    wh_size: 'XSMALL'
    users:
      - 'DEV_USER'
  configurations:
    DEV:
      wh_size: 'XSMALL'
      users:
        - 'DEV_USER'
    PROD:
      wh_size: 'LARGE'
      users:
        - 'PROD_SERVICE_USER'
```

> **Best Practice:** Embed the project identifier in manifest targets rather than passing it as a CLI argument. The `--target` flag selects a target, which resolves both the project name and templating configuration.

> **CRITICAL**: When defining multiple targets on the same account, each target MUST have a unique `project_name` or they will deploy over each other. Use environment suffixes (e.g., `PROJECT_NAME_DEV`, `PROJECT_NAME_STG`) to differentiate.

> **Finding values for target fields:**
> - `account_identifier`: Run `SELECT CURRENT_ACCOUNT()` in the target Snowflake account
> - `project_owner`: The role that will own the DCM project object. Run `DESCRIBE DCM PROJECT <identifier>` on an existing project to see its owner, or choose the role you'll use for deployments

### Step 6: Clarify Object Definitions

Before writing definitions, **provide a proposed structure and get confirmation from the user**

### Safe Defaults for Access Control

When creating a project from scratch, keep access control minimal unless the user explicitly requests roles and grants:

- **If the user does NOT mention roles/grants/users:** Do NOT create an access.sql file. Omit roles and grants entirely.
- **If the user requests roles, grants, or access control:** You **MUST** load [../roles-and-grants/SKILL.md](../roles-and-grants/SKILL.md) before writing any access control definitions. That sub-skill covers grant syntax constraints, stage privilege types, warehouse grant workarounds, and unsupported patterns that will cause plan failures if used incorrectly.

### Step 7: Write Definition Files

Based on the objects identified in Step 1, **load the relevant references** before writing definitions.

**Object primitives** -- load based on what the user explicitly requested:

- **Databases/schemas**: Load `../reference/primitives/databases_and_schemas.md`
- **Tables**: Load `../reference/primitives/tables.md`
- **Views**: Load `../reference/primitives/views.md`
- **Dynamic tables**: Load `../reference/primitives/dynamic_tables.md`
- **Tasks**: Load `../reference/primitives/tasks.md`
- **Stages**: Load `../reference/primitives/stages.md`
- **Warehouses**: Load `../reference/primitives/warehouses.md`
- **Roles/grants**: Load `../reference/primitives/roles_and_grants.md` AND `../roles-and-grants/SKILL.md`
- **SQL functions**: Load `../reference/primitives/sql_functions.md`

**Mechanism references** -- load when ANY of the listed conditions apply:

| Reference | Load when the user... |
|-----------|-----------------------|
| `../reference/primitives/jinja_templating.md` | Requests multi-environment support (DEV/PROD); uses templated object names (suffixes, prefixes); needs team-based or user-based loops; wants conditional object creation; mentions macros or reusable patterns; requests parameterized configurations (warehouse sizes, retention policies per team) |
| `../reference/primitives/unsupported_objects.md` | Requests external stages (S3, Azure, GCS with URL); needs streams, alerts, or file formats; requires integrations (API, notification, external access, catalog, security); asks for semantic views, shares, or network policies; mentions any object type not in the primitives list above; OR existing files contain `ATTACH PRE_HOOK` / `POST_HOOK` |
| `../reference/primitives/data_quality.md` | Requests data quality checks, expectations, or monitoring; mentions null checks, uniqueness, freshness, or row counts; wants to attach metrics to tables, views, or dynamic tables; asks about DATA_METRIC_SCHEDULE or system DMFs; needs custom data metric functions |

Place all definition files in `sources/definitions/`. Recommended file grouping:

| File | Contents |
|------|----------|
| `infrastructure.sql` | Databases, schemas, warehouses, internal stages |
| `tables.sql` or `raw.sql` | Table definitions |
| `analytics.sql` | Dynamic tables, transformations |
| `serve.sql` | Views for consumption |
| `access.sql` | Roles, grants, permissions |
| `expectations.sql` | Data metric functions, quality rules |

### Step 8: Validate with Analyze

Run analyze to validate the project:

```bash
snow dcm raw-analyze <identifier> -c <connection> \
    --target <target>
```

#### ⚠️ CRITICAL: Read and Parse the Output

**You MUST read and parse command output.**

### Step 9: Fix Any Errors

Common issues and fixes:

| Error                  | Cause                     | Solution                                       |
| ---------------------- | ------------------------- | ---------------------------------------------- |
| "Unknown object"       | Missing dependency        | Define the referenced object or check spelling |
| "Syntax error"         | Invalid DCM syntax        | Check DEFINE syntax matches CREATE equivalent  |
| "Duplicate definition" | Same object defined twice | Remove duplicate or consolidate                |
| "Invalid identifier"   | Bad naming                | Use fully qualified names                      |

### Step 10: Next Steps

Once analysis passes without errors:

1. **Ask user**: "The project has been created and validated. Would you like to:"

   - Run a plan to see what will be created?
   - Preview any data in existing objects? Only ask if the project contains previewable objects.

2. **Load** the deploy-project sub-skill from [../deploy-project/SKILL.md](../deploy-project/SKILL.md) if user wants to deploy

## Tips

### Naming Conventions

- Use UPPERCASE for Snowflake object names (matches Snowflake default)
- Use `{{env}}` or similar variables for environment-specific naming
- Be consistent with prefixes/suffixes

### File Organization

Keep related objects together:

- All tables in one file, or
- Group by business domain (sales.sql, marketing.sql, etc.)

### Global Macros

If your project uses Jinja macros shared across multiple definition files, place them in `sources/macros/`:

    project/
    ├── manifest.yml
    └── sources/
        ├── definitions/
        │   └── *.sql
        └── macros/
            └── shared_macros.sql

Unlike macros defined inline in definition files (which are scoped to that file only), macros in `sources/macros/` are accessible from all definition files.

### Templating Variables

Common variables to define under `templating.defaults` and `templating.configurations`:

- `env_suffix`: Environment suffix for object names (`_DEV`, `""`)
- `wh_size`: Warehouse size
- `users`: List of users for grants
- `teams`: List of team/schema names
