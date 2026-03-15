---
name: modify-project
description: "Modify existing DCM projects. Triggers: modify dcm, update project, add table, edit definitions, download project sources, import existing objects"
---

# Modify Existing DCM Project

## Overview

This sub-skill guides you through modifying an existing DCM project, whether you have local source code or need to download sources from a deployed project.

## Determine Starting Point

### Scenario A: Local Source Code Available

If the user has source code locally (manifest.yml and sources/definitions/ folder):

- Proceed directly to [Understand Current State](#step-1-understand-current-state)

### Scenario B: No Local Source Code

If the user wants to work with a deployed project but doesn't have the source:

1. **List ALL available projects (use --database "" to see all):**

   ```bash
   snow dcm list -c <connection> --database ""
   ```

   ⚠️ Note: Without `--database ""`, only projects in the default database are shown.

2. **Describe the target project:**

   ```bash
   snow dcm describe <identifier> -c <connection>
   ```

3. **Download sources using the skill's script:**

   ```bash
   bash <skill-dir>/scripts/download_project.sh <project_name> \
       --connection <connection> \
       --target <local_folder>
   ```

4. Proceed to [Understand Current State](#step-1-understand-current-state)

## Modification Workflow

### Step 1: Understand Current State

**Read the manifest.yml:**

- What targets are defined?
- What templating configurations exist?
- Note any template variables used (under `templating.defaults` and `templating.configurations`)

**Read existing definition files:**

- Understand the current object structure
- Identify dependencies between objects
- Note any Jinja templating patterns

**Run analyze to get the full picture:**

```bash
snow dcm raw-analyze <identifier> -c <connection> \
    --target <target>
```

**From analyze output, understand:**

- All objects currently defined
- Dependencies between objects
- Column-level lineage
- Any existing data quality expectations

### Step 2: Clarify Requested Changes

**Ask the user specifically about:**

1. **What to ADD?**

   - New tables, views, dynamic tables?
   - New tasks for ETL or scheduled operations?
   - New roles or grants?
   - New data quality expectations?

2. **What to MODIFY?**

   - Column changes (add, rename, change type)?
   - Query/transformation changes?
   - Property changes (warehouse size, refresh schedule)?

3. **What to REMOVE?**
   - ⚠️ This will result in DROP operations
   - Confirm user understands data loss implications

**For each change, confirm:**

- Object names (fully qualified)
- Column definitions
- Specific values/logic
- Whether changes affect existing data

### Step 3: Propose Changes

**Present proposed modifications to user before making them:**

```
📝 Proposed Changes:

ADD:
- Table: MY_DB.RAW.NEW_TABLE
  Columns: ID (NUMBER), NAME (VARCHAR), CREATED_AT (TIMESTAMP_NTZ)

MODIFY:
- Table: MY_DB.RAW.EXISTING_TABLE
  + Add column: STATUS (VARCHAR(50))
  ~ Change column: AMOUNT from NUMBER to NUMBER(15,2)

REMOVE:
- View: MY_DB.SERVE.OLD_VIEW

Do you approve these changes?
```

### Step 4: Make Changes to Definition Files

Before writing or modifying definitions, **load the relevant references**:

**Object primitives** -- load based on what is being added or modified:

- **Databases/schemas**: Load `../reference/primitives/databases_and_schemas.md`
- **Tables**: Load `../reference/primitives/tables.md`
- **Views**: Load `../reference/primitives/views.md`
- **Dynamic tables**: Load `../reference/primitives/dynamic_tables.md`
- **Tasks**: Load `../reference/primitives/tasks.md`
- **Stages**: Load `../reference/primitives/stages.md`
- **Warehouses**: Load `../reference/primitives/warehouses.md`
- **Roles/grants**: Load `../reference/primitives/roles_and_grants.md`
- **SQL functions**: Load `../reference/primitives/sql_functions.md`

**Mechanism references** -- load when ANY of the listed conditions apply:

| Reference | Load when... |
|-----------|-------------|
| `../reference/primitives/jinja_templating.md` | Existing project uses Jinja templates; changes involve templated names or variables; adding multi-environment support; adding loops, conditionals, or macros |
| `../reference/primitives/unsupported_objects.md` | Adding external stages, streams, alerts, file formats, or integrations; existing files contain `ATTACH PRE_HOOK` / `POST_HOOK`; any object type not supported by DEFINE |
| `../reference/primitives/data_quality.md` | Adding or modifying data quality expectations; attaching DMFs to tables/views/DTs; changing DATA_METRIC_SCHEDULE on objects; creating custom data metric functions |

Load only the references relevant to the objects being modified.

**Adding new objects:**

- Add DEFINE statements to appropriate file
- Use consistent naming with existing objects
- Follow existing Jinja variable patterns

**Modifying existing objects:**

- Locate the object's DEFINE statement
- Update the definition
- Ensure fully qualified names are preserved

**Removing objects:**

- Remove the DEFINE statement entirely
- Check for dependent objects that might break

### Step 5: Validate with Analyze

```bash
snow dcm raw-analyze <identifier> -c <connection> \
    --target <target> 
```

#### ⚠️ CRITICAL: Read and Parse the Output 

**You MUST read and parse output that is returned by the command.**


**Fix any errors before proceeding.**

### Step 6: Run Plan to Preview Changes

```bash
snow dcm plan <identifier> -c <connection> \
    --target <target> \
    --save-output
```

#### ⚠️ CRITICAL: Read and Parse the Output

**You MUST read and parse `out/plan/plan_result.json`.**

For detailed instructions, see: [Parent SKILL.md - Critical: Reading Output Files](../SKILL.md#critical-reading-output-files)

**If plan output already exists** and user asks for a summary:

- Read the existing file instead of rerunning
- Only rerun if explicitly requested or definitions have changed

### Step 7: Present Plan Summary

**Format the plan output for the user:**

```
📊 Plan Summary for <identifier>

Target: <target_name>

✅ CREATE (X objects):
   - TABLE: MY_DB.RAW.NEW_TABLE
   - ROLE: MY_DB_READER

⚠️  ALTER (Y objects):
   - TABLE: MY_DB.RAW.EXISTING_TABLE
     + Add column: STATUS (VARCHAR)
     ~ Modify column: AMOUNT (NUMBER → NUMBER(15,2))

🚨 DROP (Z objects):
   - VIEW: MY_DB.SERVE.OLD_VIEW
   ⚠️ WARNING: This will permanently delete the view

📋 GRANT changes:
   - Grant SELECT on MY_DB.RAW.NEW_TABLE to MY_ROLE
```

**Highlight risky changes:**

- Column type changes (may fail if data incompatible)
- Column removals (data loss)
- Table/view drops (data loss)

### Step 8: Offer Preview

**Ask the user:**

> "Would you like to preview data in any of the affected objects before deployment?"

If yes, use:

```bash
snow dcm preview -c <connection> \
    --object <fully.qualified.object.name> \
    --limit 10
```

### Step 9: Proceed to Deploy

Once user has reviewed the plan:

1. **Load** the deploy-project sub-skill from [../deploy-project/SKILL.md](../deploy-project/SKILL.md) if user wants to deploy

2. Follow the deployment workflow with explicit confirmation

## Common Modification Scenarios

### Adding a New Task

1. **Load** `../reference/primitives/tasks.md` for complete syntax and examples
2. Add DEFINE TASK statement to tasks.sql or appropriate file
3. Add any grants for task execution permissions

**Note:** Tasks are automatically suspended/resumed during deployment if changes are needed.

### Adding a New Table

1. Add DEFINE TABLE statement to tables.sql or appropriate file
2. Add any grants for the new table to access.sql
3. Update dependent views/dynamic tables if needed

### Adding a Column to Existing Table

```sql
-- Before
DEFINE TABLE MY_DB.RAW.CUSTOMERS (
    CUSTOMER_ID NUMBER,
    NAME VARCHAR
);

-- After
DEFINE TABLE MY_DB.RAW.CUSTOMERS (
    CUSTOMER_ID NUMBER,
    NAME VARCHAR,
    EMAIL VARCHAR(255)  -- New column
);
```

### Changing a Dynamic Table Transformation

1. Update the SELECT statement in the dynamic table definition
2. Run analyze to verify the new query is valid
3. Plan will show the body change

### Adding Data Quality Expectations

1. Ensure the target table has `DATA_METRIC_SCHEDULE` set:

   ```sql
   DEFINE TABLE MY_DB.RAW.MY_TABLE (...)
   DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
   ```

2. Add ATTACH statements:
   ```sql
   ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
       TO TABLE MY_DB.RAW.MY_TABLE
       ON (IMPORTANT_COLUMN)
       EXPECTATION NO_NULLS (value = 0);
   ```

### Removing an Object

**⚠️ CAUTION: Removing a DEFINE statement will DROP the object**

1. Check for dependencies:

   ```bash
   # Use analyze output to find what depends on this object
   snow dcm raw-analyze <identifier> -c <connection>
   # Review dependencies in the output
   ```

2. Remove dependent objects first (or update them to not reference the removed object)

3. Remove the DEFINE statement

4. Plan will show DROP operation

### Detaching Objects from DCM (Without Dropping)

If you want to remove an object from DCM management without dropping it:

1. **Detach the object** using ALTER ... UNSET:

   ```sql
   ALTER TABLE MY_DB.MY_SCHEMA.MY_TABLE UNSET DCM PROJECT;
   ```

2. **Remove the DEFINE statement** from your project definition files before the next deployment (otherwise the object will be re-adopted)

3. The object continues to exist in Snowflake as an unmanaged object

This is useful when transferring object management between DCM projects.

### Adopting/Importing Existing Objects

When user wants to import existing Snowflake objects into the DCM project:

1. **Get current DDL**:

   ```sql
   SELECT GET_DDL('TABLE', 'MY_DB.MY_SCHEMA.MY_TABLE');
   SELECT GET_DDL('VIEW', 'MY_DB.MY_SCHEMA.MY_VIEW');
   SELECT GET_DDL('DYNAMIC_TABLE', 'MY_DB.MY_SCHEMA.MY_DT');
   SELECT GET_DDL('STAGE', 'MY_DB.MY_SCHEMA.MY_STAGE');
   ```

2. **⚠️ MANDATORY: Categorize objects BEFORE converting**:

   **A. Stages - Check for URL parameter:**
   
   ```sql
   DESC STAGE MY_DB.MY_SCHEMA.MY_STAGE;
   -- Look for "url" field in output
   ```
   
   | Stage Type | Has URL? | Action |
   |------------|----------|--------|
   | Internal | ❌ No URL | Convert to `DEFINE STAGE` → infrastructure.sql |
   | External | ✅ Has URL (S3/Azure/GCS) | Place in `post_deploy.sql` |

   **B. Grants - Analyze privilege patterns:**

   ```sql
   -- Get grants on the objects being adopted
   SHOW GRANTS ON TABLE MY_DB.MY_SCHEMA.MY_TABLE;
   SHOW GRANTS ON VIEW MY_DB.MY_SCHEMA.MY_VIEW;
   
   -- Get grants to roles if roles are being adopted
   SHOW GRANTS TO ROLE <role_name>;
   SHOW GRANTS TO DATABASE ROLE <db.role_name>;
   ```

   **Load** [../roles-and-grants/SKILL.md](../roles-and-grants/SKILL.md) and categorize grants:

   For the complete list of unsupported grant patterns, see: [Syntax Reference - Unsupported Grants](../reference/syntax_overview.md#what-dcm-does-not-support-for-grants)

   | Category | Example | Action |
   |----------|---------|--------|
   | ✅ Supported | `GRANT SELECT ON TABLE` | Include in access.sql |
   | ⚠️ Workaround | `GRANT USAGE ON WAREHOUSE TO DATABASE ROLE` | Use account role pattern |
   | ❌ Unsupported | `GRANT ... ON ACCOUNT`, `GRANT IMPORTED PRIVILEGES` | Document in post_deployment_grants.sql |

   **⚠️ CHECKPOINT**: Present categorization analysis to user and get approval before proceeding.

3. **Convert supported objects (CREATE to DEFINE)**:
   - **Internal stages**: `CREATE STAGE` → `DEFINE STAGE`
   - **Tables/Views/Warehouses**: `CREATE` → `DEFINE`
   - **External stages**: Place in `post_deploy.sql` (not supported by DEFINE)
   - **Grants**: Handle per analysis in step 2 (don't blindly convert)

4. **Add to project definition files**:
   - Supported objects (DEFINE) → appropriate .sql files (tables.sql, infrastructure.sql, etc.)
   - Internal stages → infrastructure.sql or stages.sql
   - External stages → `post_deploy.sql`
   - Supported grants → access.sql
   - Unsupported grants → post_deployment_grants.sql (manual execution)

5. **Run analyze and READ the output**: Verify object appears in definitions

6. **Run plan and READ the output**:

   - ⚠️ **CRITICAL**: Plan should show **ZERO changes** for adopted objects
   - If plan shows CREATE or ALTER, the definition doesn't match exactly
   - Compare DCM definition with actual DDL and adjust

7. **Iterate until plan shows no changes** for adopted objects

**Success = object appears in analyze but has zero operations in plan**

## Handling Dependencies

When modifying objects, be aware of the dependency chain:

```
Source Tables
    ↓
Dynamic Tables (transformations)
    ↓
Views (consumption)
    ↓
Grants
```

**Rule:** Modify from bottom up when removing, top down when adding.

## Checking Lineage

Use analyze output to understand column-level lineage:

```bash
snow dcm raw-analyze <identifier> -c <connection>
```

The output includes:

- `dependencies`: Object-level dependencies
- `columns[].lineage`: Column-level lineage for views and dynamic tables

This helps you understand:

- What will break if you remove a column
- Where data flows through your pipeline
