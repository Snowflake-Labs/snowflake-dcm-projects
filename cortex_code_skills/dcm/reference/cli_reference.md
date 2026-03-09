# DCM CLI Reference

This document provides a comprehensive reference for the `snow dcm` command-line interface used to manage DCM (Database Change Management) projects in Snowflake. This reference is designed to help AI agents understand and execute DCM operations on behalf of users.

## Overview

DCM is a declarative database change management system that enables:

- **Version-controlled infrastructure**: Define Snowflake objects as code
- **Static analysis**: Validate projects before deployment
- **Change planning**: Preview what changes will be made
- **Safe deployments**: Apply changes with full visibility

## Connection Requirement

**All DCM commands require a Snowflake connection.** Always specify the connection using the `-c` or `--connection` flag:

```bash
snow dcm <command> -c <connection_name> [options]
```

---

## Commands

### `snow dcm list`

Lists available DCM projects in the connected Snowflake account.

#### Syntax

```bash
snow dcm list -c <connection> [options]
```

#### Options

| Option       | Short | Description                                                                            |
| ------------ | ----- | -------------------------------------------------------------------------------------- |
| `--like`     | `-l`  | SQL LIKE pattern for filtering projects by name (e.g., `"my%"`)                        |
| `--in`       |       | Scope the listing using `--in <scope_type> <scope_name>` (e.g., `--in database MY_DB`) |
| `--database` |       | Database to list projects from. Use `--database ""` to list ALL projects               |

#### ⚠️ IMPORTANT: Default Database Behavior

**By default, `snow dcm list` only returns projects from the connection's default database.** This means you may not see all projects in the account.

**To list ALL projects across all databases, use:**

```bash
snow dcm list -c <connection> --database ""
```

**Agent Guidance:** When helping users discover projects, **always use `--database ""`** to ensure you show all available projects, not just those in the default database.

#### Examples

```bash
# List ALL DCM projects across all databases (RECOMMENDED DEFAULT)
snow dcm list -c myconnection --database ""

# List projects only in the default database
snow dcm list -c myconnection

# List projects matching a pattern
snow dcm list -c myconnection --database "" --like "ANALYTICS%"

# List projects in a specific database
snow dcm list -c myconnection --in database ANALYTICS_DB
```

---

### `snow dcm create`

Creates a new DCM project in Snowflake.

#### Syntax

```bash
snow dcm create <identifier> -c <connection> [options]
```

#### Arguments

| Argument     | Required | Description                                                                         |
| ------------ | -------- | ----------------------------------------------------------------------------------- |
| `identifier` | Yes      | FULLY QUALIFIED IDENTIFIER for the DCM project (e.g., `MY_DB.MY_SCHEMA.MY_PROJECT`) |

#### Examples

```bash
# Create a new DCM project
snow dcm create MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection

```

#### Important Notes

- Project identifiers must follow Snowflake naming conventions
- If the identifier contains special characters or needs to preserve case, use proper quoting:
  ```bash
  snow dcm create db.public.'"My-Special-Project"' -c myconnection
  ```
- The database and schema must already exist in the Snowflake account. if the database or schema does not exist, prompt the user to create the database or schema before creating the project.

---

### `snow dcm describe`

Provides detailed description of a DCM project. Useful to help the user understand the state of the project

#### Syntax

```bash
snow dcm describe <name> -c <connection>
```


#### Arguments

| Argument | Required | Description                       |
| -------- | -------- | --------------------------------- |
| `name`   | Yes      | The identifier of the DCM project |

#### Examples

```bash
snow dcm describe MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection
```

---

### `snow dcm raw-analyze`

Performs static analysis on a DCM project, including:

- List of all objects defined in the project
- Object dependencies and relationships
- Column-level lineage information
- Errors and warnings

#### Syntax

```bash
snow dcm raw-analyze <identifier> -c <connection> [options]
```


#### Arguments

| Argument     | Required | Description                       |
| ------------ | -------- | --------------------------------- |
| `identifier` | Yes      | The identifier of the DCM project |

#### Options

| Option            | Short | Description                                                                                   |
| ----------------- | ----- | --------------------------------------------------------------------------------------------- |
| `--target` |       | Target from `manifest.yml` to use (bundles project identifier + templating config)            |

#### Examples

```bash
# Analyze from current directory
snow dcm raw-analyze MY_PROJECT -c myconnection

# Analyze with a specific target
snow dcm raw-analyze MY_PROJECT -c myconnection --target DEV
```

#### ⚠️ CRITICAL: Reading and Parsing Analyze Output

**After running `analyze`, you MUST read and parse the output that is returned by the command.**

This is NOT optional. The agent MUST:

1. **Read the output**: the output that is returned by the command.
2. **Parse the JSON** and check:
   - Are there any errors at the file level (`errors` array)?
   - Are there any errors at the definition level (each definition's `errors` array)?
   - Were all expected objects found in the `definitions` array?
3. **If errors exist**: Report them to the user and fix the issues before proceeding
4. **If no errors**: Confirm the analysis passed and list the objects found

**Example verification:**

```
After running analyze, I read the output that is returned by the command:
- Found 5 definitions: 2 tables, 2 dynamic tables, 1 view
- No errors detected at file or definition level
- Dependencies resolved successfully
```

#### Agent Guidance for Analyze

1. **Always read and parse the output JSON**: This is mandatory, not optional
2. **Check for errors first**: Look at the `errors` arrays at both file and definition levels
3. **Explore dependencies**: Use the dependency information to understand object relationships
4. **Investigate lineage**: For data quality and impact analysis, examine column-level lineage
5. **Encourage exploration**: Suggest users examine the rendered SQL to understand transformations
6. Since analyze output contains rendered jinja, it will also contain a definitions folder in the output that may cause confusion. The agent should remove the output folder before running the analyze command.

---

### `snow dcm plan`

Generates a detailed plan showing what changes will be made to Snowflake infrastructure. This is a **preview-only** operation that validates without executing.

#### Syntax

```bash
snow dcm plan <identifier> -c <connection> [options]
```


#### Arguments

| Argument     | Required | Description                       |
| ------------ | -------- | --------------------------------- |
| `identifier` | Yes      | The identifier of the DCM project |

#### Options

| Option            | Short | Description                                                                         |
| ----------------- | ----- | ----------------------------------------------------------------------------------- |
| `--target` |       | Target from `manifest.yml` to use (bundles project identifier + templating config)  |
| `--save-output`   |       | Save the plan output to a file |

> **Note:** The output path is always `out/` relative to the project directory. Add `out/` to your `.gitignore` to avoid pushing local output files to git.

#### Examples

```bash
# Plan from current directory
snow dcm plan MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection --save-output

# Plan with a specific target
snow dcm plan MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection --target PROD --save-output
```

> **Always use `--save-output`** when running `snow dcm plan`. Without it, `out/plan/plan_result.json` is not written and the agent cannot read or verify the plan results.

#### Understanding Plan Output

The plan output contains a structured representation of proposed changes:

**Plan Status:**

- `SUCCESS`: Plan generated successfully
- `PLAN_FAILED`: Plan generation failed (includes `error` field with details)

**Applied Configuration:**

- `templateVariables`: Record of variables used
- `configurationName`: Name of the configuration applied (optional)

**DDL Change Log (on success):**
Contains an array of `operations`, each being one of:

**Object Operations (CREATE, ALTER, DROP):**

- `operationType`: `CREATE`, `ALTER`, or `DROP`
- `objectDomain`: Type of object (e.g., `TABLE`, `VIEW`, `SCHEMA`)
- `objectName`: Name of the object
- `objectIdentifier`: Fully qualified identifier
- `details`: Object-specific details including:
  - `properties`: Key-value pairs showing changes (with `changeFrom`/`changeTo` for modifications)
  - `columns`: Column changes with operation types
  - `body`: Body changes (e.g., view definitions)

**Grant Operations:**

- `operationType`: `CREATE`, `ALTER`, or `DROP`
- `association`: `GRANT`
- `subject`: The object being granted (can be existing or future grants)
- `target`: The grantee

**DMF Attachment Operations:**

- `operationType`: `CREATE`, `ALTER`, or `DROP`
- `association`: `DMF_ATTACHMENT`
- `subject`: Data metric function being attached
- `target`: Target object and columns
- `details`: Expectation configurations

#### ⚠️ CRITICAL: Reading and Parsing Plan Output

**After running `plan`, you MUST read and parse `out/plan/plan_result.json` to verify the results.**

This is NOT optional. The agent MUST:

1. **Read the file**: `out/plan/plan_result.json`
2. **Parse the JSON** and check:
   - What is the `status`? (`SUCCESS` or `PLAN_FAILED`)
   - If `PLAN_FAILED`, what is the `error` message?
   - If `SUCCESS`, parse the `operations` array to understand all changes
3. **Categorize operations**: Count CREATE, ALTER, DROP operations
4. **Present a clear summary** to the user before any deployment

**If plan output already exists** and user asks for a summary or to proceed with deployment:

- **Do NOT rerun plan** - instead, read the existing `out/plan/plan_result.json`
- Only rerun plan if user explicitly requests it or if definitions have changed

#### Agent Guidance for Plan

1. **Always read and parse the output JSON**: This is mandatory, not optional
2. **Reuse existing plan output**: If `out/plan/plan_result.json` exists and is current, read it instead of rerunning
3. **Always run plan before deploy**: Never skip the planning step
4. **Highlight destructive changes**: Pay special attention to `DROP` and `ALTER` operations
5. **Summarize changes clearly**: Group changes by type (creates, alters, drops) and importance
6. **Watch for data-affecting changes**:
   - Column type changes
   - Column drops
   - Table drops
   - View body changes that might affect downstream consumers
7. **Present a human-readable summary**: Format changes in an easy-to-understand way, e.g.:
   ```
   📊 Plan Summary:
   ✅ CREATE: 3 objects (2 tables, 1 view)
   ⚠️  ALTER: 1 object (1 table - column type change)
   🚨 DROP: 1 object (1 table)
   ```
8. Since plan output contains rendered jinja, it will also contain a definitions folder in the output that may cause confusion. The agent should remove the output folder before running the plan command.

---

### `snow dcm preview`

Returns sample rows from tables, views, or dynamic tables defined in the DCM project. Useful for validating data before or after deployment.

#### Syntax

```bash
snow dcm preview <identifier> -c <connection> --object <fqn> [options]
```

#### Arguments

| Argument     | Required | Description                       |
| ------------ | -------- | --------------------------------- |
| `identifier` | Yes      | The identifier of the DCM project |

#### Options

| Option            | Short | Required | Description                                                                      |
| ----------------- | ----- | -------- | -------------------------------------------------------------------------------- |
| `--object`        |       | **Yes**  | Fully qualified name of the object to preview (e.g., `MY_DB.MY_SCHEMA.MY_TABLE`) |
| `--target` |       | No       | Target to use (from manifest.yml)                                                |
| `--limit`         |       | No       | Maximum number of rows to return                                                 |

#### Examples

```bash
# Preview a table with default settings
snow dcm preview MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection --object MY_DB.PUBLIC.CUSTOMERS

# Preview with row limit
snow dcm preview MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection --object MY_DB.PUBLIC.ORDERS --limit 10

# Preview with specific target
snow dcm preview MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection --target DEV --object MY_DB.PUBLIC.MY_VIEW --limit 5
```

#### Agent Guidance for Preview

1. **Use for data validation**: After deploy, preview objects to confirm data is correct
2. **Respect row limits**: Use `--limit` to avoid overwhelming output
3. **Object must be fully qualified**: Always use `<database>.<schema>.<object>` format
4. **Encourage exploration**: Suggest previewing key tables/views to understand the project's data

---

### `snow dcm deploy`

Applies changes defined in the DCM project to Snowflake. This command executes the actual DDL statements against the live infrastructure.

#### Syntax

```bash
snow dcm deploy <identifier> -c <connection> [options]
```


#### Arguments

| Argument     | Required | Description                       |
| ------------ | -------- | --------------------------------- |
| `identifier` | Yes      | The identifier of the DCM project |

#### Options

| Option            | Short | Description                                        |
| ----------------- | ----- | -------------------------------------------------- |
| `--target` |       | Target from `manifest.yml` to use (bundles project identifier + templating config) |
| `--alias`         |       | Alias for the deployment (for tracking/management) |

#### Examples

```bash
# Deploy from current directory
snow dcm deploy MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection

# Deploy with a specific target
snow dcm deploy MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection --target PROD

# Deploy with an alias for tracking
snow dcm deploy MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection --alias "release-v1.2.0"
```

#### ⚠️ CRITICAL WARNINGS

> **DEPLOY IS A DESTRUCTIVE OPERATION**
>
> This command makes changes to live Snowflake infrastructure. Changes may include:
>
> - Creating new objects
> - Altering existing objects (including column types, view definitions)
> - **DROPPING objects and all their data**

**Agent Guidelines:**

1. **ALWAYS run `plan` first**: Before every deploy, execute a plan command and review the output
2. **REQUIRE user confirmation**: Always prompt the user to confirm before executing deploy
3. **Highlight destructive changes**: If the plan shows DROP or significant ALTER operations, emphasize these risks
4. ** Alias is encouraged **: the agent should encourage the user to use an alias for the deployment to help track the deployment.

**Confirmation Template:**

```
⚠️ You are about to deploy changes to Snowflake. This operation will (use details from plan output):
- CREATE: [list of objects]
- ALTER: [list of objects]
- DROP: [list of objects]

This will affect the following database: [database_name]
Using connection: [connection_name]

Are you sure you want to proceed? (yes/no)
```

---

### `snow dcm list-deployments`

Lists all deployments for a given DCM project.

#### Syntax

```bash
snow dcm list-deployments <identifier> -c <connection>
```


#### Arguments

| Argument     | Required | Description                       |
| ------------ | -------- | --------------------------------- |
| `identifier` | Yes      | The identifier of the DCM project |

#### Examples

```bash
snow dcm list-deployments MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection
```

---

### `snow dcm drop-deployment`

Drops (removes) a specific deployment from a DCM project.

#### Syntax

```bash
snow dcm drop-deployment <identifier> <deployment_name> -c <connection> [options]
```

#### Arguments

| Argument          | Required | Description                             |
| ----------------- | -------- | --------------------------------------- |
| `identifier`      | Yes      | The identifier of the DCM project       |
| `deployment_name` | Yes      | Name or alias of the deployment to drop |

#### Options

| Option        | Description                                |
| ------------- | ------------------------------------------ |
| `--if-exists` | Do nothing if the deployment doesn't exist |

#### Examples

```bash
# Drop a deployment
snow dcm drop-deployment MY_PROJECT 'DEPLOYMENT$1' -c myconnection

# Drop only if it exists
snow dcm drop-deployment MY_PROJECT my_deployment -c myconnection --if-exists
```

#### Important Notes

- For deployment names containing `$`, use single quotes to prevent shell expansion

---

### `snow dcm drop`

Drops (deletes) a DCM project.

#### Syntax

```bash
snow dcm drop <name> -c <connection>
```

#### Arguments

| Argument | Required | Description                               |
| -------- | -------- | ----------------------------------------- |
| `name`   | Yes      | The identifier of the DCM project to drop |

#### Examples

```bash
snow dcm drop MY_PROJECT -c myconnection
```

#### ⚠️ Warning

This is a destructive operation that removes the DCM project metadata. Consider the implications before running.

---

### `snow dcm refresh`

Manually refreshes dynamic tables defined in the DCM project.

#### Syntax

```bash
snow dcm refresh <identifier> -c <connection>
```


#### Arguments

| Argument     | Required | Description                       |
| ------------ | -------- | --------------------------------- |
| `identifier` | Yes      | The identifier of the DCM project |

#### Examples

```bash
snow dcm refresh MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection
```

#### Use Cases

- Force a refresh of dynamic tables outside their normal schedule
- Validate data after upstream changes
- Testing and development scenarios

---

### `snow dcm test`

Runs expectation tests on tables, views, and dynamic tables defined in the DCM project. Tests validate data quality rules defined in the project.

#### Syntax

```bash
snow dcm test <identifier> -c <connection> [options]
```


#### Arguments

| Argument     | Required | Description                       |
| ------------ | -------- | --------------------------------- |
| `identifier` | Yes      | The identifier of the DCM project |

#### Examples

```bash
# Run tests with default output
snow dcm test MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection
```

#### Use Cases

- CI/CD pipelines for data validation
- Post-deployment verification
- Ongoing data quality monitoring

---

## Typical Workflow

A typical DCM workflow follows these steps:

```
┌───────────────────────────────────────────────────────────────┐
│  1. LIST - Discover existing projects                         │
│     snow dcm list -c myconnection                             │
│                                                               │
│     ↳ For existing projects, use LIST-DEPLOYMENTS to see     │
│       deployment history and understand project state         │
└───────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│  2. CREATE - Create a new project (if needed)                 │
│     snow dcm create MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection│
└───────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│  3. ANALYZE - Validate the project structure                  │
│     snow dcm raw-analyze MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection│
│                                                               │
│     ↳ Review errors and warnings                              │
│     ↳ Explore dependencies and lineage                        │
└───────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│  4. PREVIEW - Validate data before deployment                  │
│     snow dcm preview MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection│
│     --object MY_DB.MY_SCHEMA.MY_TABLE --limit 10              │
└───────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│  5. PLAN - Preview what changes will be made                  │
│     snow dcm plan MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection  │
│     --target DEV --save-output                                │
│                                                               │
│     ↳ Review CREATE/ALTER/DROP operations                     │
│     ↳ Pay attention to destructive changes                    │
└───────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│  6. PREVIEW - Validate data after deployment                  │
│     snow dcm preview MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection│
│     --object MY_DB.MY_SCHEMA.MY_TABLE --limit 10              │
└───────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│  7. DEPLOY - Apply changes (requires confirmation!)           │
│     snow dcm deploy MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection│
│     --target DEV --alias "v1.0.0"                      │
│                                                               │
│     ⚠️ ALWAYS get user confirmation first                     │
└───────────────────────────────────────────────────────────────┘

                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│  8. REFRESH - Refresh dynamic tables for latest data          │
│     snow dcm refresh MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection│
│                                                               │
│     ↳ Ensures TEST runs against the most current data         │
└───────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│  9. TEST - Run data quality tests                             │
│     snow dcm test MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection  │
└───────────────────────────────────────────────────────────────┘
```

**Exploring Existing Projects:** When working with an existing DCM project, use `list-deployments` to understand the project's deployment history. This helps you see what targets have been deployed, when they were applied, and what aliases were used—useful context before making changes.

---

## Common Options Across Commands

These options are available on most commands:

| Option               | Description                             |
| -------------------- | --------------------------------------- |
| `-c`, `--connection` | **Required**: Snowflake connection name |
| `--target`    | Target from `manifest.yml` (bundles project identifier + templating config) |
| `--save-output`      | Save plan output artifacts to `out/` (plan command only) |

---

---

## Adopting Existing Objects

DCM can "adopt" existing Snowflake objects into a project. This is useful when:

- Converting manually-created infrastructure to DCM-managed
- Importing existing tables, views, or other objects into version control
- Taking ownership of objects created outside DCM

### How Adoption Works

When you define an object in DCM that already exists in Snowflake with the **exact same definition**, DCM recognizes it as an existing object and does not attempt to recreate it. The plan will show **no changes** for that object.

### Adoption Workflow

1. **Get the current DDL** for the existing object:

   ```sql
   SELECT GET_DDL('TABLE', 'MY_DB.MY_SCHEMA.MY_TABLE');
   SELECT GET_DDL('VIEW', 'MY_DB.MY_SCHEMA.MY_VIEW');
   SELECT GET_DDL('DYNAMIC_TABLE', 'MY_DB.MY_SCHEMA.MY_DT');
   ```

2. **Convert CREATE to DEFINE**: Replace `CREATE` keyword with `DEFINE`

3. **Add to DCM project**: Place the definition in the appropriate `.sql` file

4. **Run analyze**: Verify the definition is valid

5. **Run plan**: The plan should show **NO CHANGES** for adopted objects

   - If plan shows changes, the definition doesn't match exactly
   - Adjust the DCM definition to match the existing object

### Verification

**A successful adoption means:**

- `analyze` shows the object in its definitions list
- `plan` shows **zero operations** for that object (no CREATE, ALTER, or DROP)

**If plan shows changes:**

- Compare the DCM definition with the actual object DDL
- Common mismatches: column order, data types, properties (CHANGE_TRACKING, etc.)
- Adjust the DCM definition until plan shows no changes

### Example

```sql
-- Existing table in Snowflake (get with GET_DDL)
CREATE TABLE MY_DB.RAW.CUSTOMERS (
    CUSTOMER_ID NUMBER(38,0) NOT NULL,
    NAME VARCHAR(255),
    EMAIL VARCHAR(255)
);

-- DCM definition to adopt it (in sources/definitions/tables.sql)
DEFINE TABLE MY_DB.RAW.CUSTOMERS (
    CUSTOMER_ID NUMBER(38,0) NOT NULL,
    NAME VARCHAR(255),
    EMAIL VARCHAR(255)
);
```

After running `plan`, this should show zero changes for MY_DB.RAW.CUSTOMERS, indicating successful adoption.

---

## Error Handling

When commands fail, check:

1. **Connection issues**: Verify the connection name and credentials
2. **Permission errors**: Ensure the user/role has required privileges
3. **Analysis errors**: Review the analyze output for validation issues
4. **Plan failures**: Check the `error` field in the plan output

For verbose debugging, add `--debug` to any command.
