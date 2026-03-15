# DCM CLI Reference

Comprehensive reference for the `snow dcm` CLI used to manage DCM (Database Change Management) projects in Snowflake.

## Overview

DCM is a declarative database change management system that enables version-controlled Snowflake infrastructure, static analysis, change planning, and safe deployments.

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

#### IMPORTANT: Default Database Behavior

**By default, `snow dcm list` only returns projects from the connection's default database.** To list ALL projects across all databases:

```bash
snow dcm list -c <connection> --database ""
```

**Agent Guidance:** When helping users discover projects, **always use `--database ""`** to ensure you show all available projects.

#### Examples

```bash
# List ALL DCM projects across all databases (RECOMMENDED DEFAULT)
snow dcm list -c myconnection --database ""

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
snow dcm create MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection
```

#### Important Notes

- Project identifiers must follow Snowflake naming conventions
- If the identifier contains special characters or needs to preserve case, use proper quoting:
  ```bash
  snow dcm create db.public.'"My-Special-Project"' -c myconnection
  ```
- The database and schema must already exist. Prompt the user to create them if they don't.

---

### `snow dcm describe`

Provides detailed description of a DCM project, useful for understanding project state.

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

Performs static analysis on a DCM project, returning object definitions, dependencies, column-level lineage, and errors/warnings.

#### Syntax

```bash
snow dcm raw-analyze <identifier> -c <connection> [options]
```

#### Arguments

| Argument     | Required | Description                       |
| ------------ | -------- | --------------------------------- |
| `identifier` | Yes      | The identifier of the DCM project |

#### Options

| Option     | Description                                                                        |
| ---------- | ---------------------------------------------------------------------------------- |
| `--target` | Target from `manifest.yml` to use (bundles project identifier + templating config) |

#### Examples

```bash
# Analyze from current directory
snow dcm raw-analyze MY_PROJECT -c myconnection

# Analyze with a specific target
snow dcm raw-analyze MY_PROJECT -c myconnection --target DEV
```

#### CRITICAL: Reading and Parsing Analyze Output

**After running `analyze`, you MUST read and parse the command output.**

1. **Parse the JSON** and check for errors at the file level (`errors` array) and at the definition level (each definition's `errors` array)
2. **If errors exist**: Report them to the user and fix before proceeding
3. **If no errors**: Confirm analysis passed and list the objects found

#### Agent Guidance

1. **Always read and parse the output JSON** -- this is mandatory
2. **Check for errors first** at both file and definition levels
3. **Use dependency info** to understand object relationships and lineage

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

| Option          | Description                                                                        |
| --------------- | ---------------------------------------------------------------------------------- |
| `--target`      | Target from `manifest.yml` to use (bundles project identifier + templating config) |
| `--save-output` | Save the plan output to a file                                                     |

> **Always use `--save-output`** when running `snow dcm plan`. Without it, `out/plan/plan_result.json` is not written and the agent cannot read or verify the plan results. Add `out/` to `.gitignore`.

#### Examples

```bash
# Plan from current directory
snow dcm plan MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection --save-output

# Plan with a specific target
snow dcm plan MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection --target PROD --save-output
```

#### Understanding Plan Output

**Plan Status:** `SUCCESS` (plan generated) or `PLAN_FAILED` (includes `error` field).

**DDL Change Log (on success)** contains an array of `operations`:

**Object Operations (CREATE, ALTER, DROP):**

- `operationType`: `CREATE`, `ALTER`, or `DROP`
- `objectDomain`: Type of object (e.g., `TABLE`, `VIEW`, `SCHEMA`)
- `objectName` / `objectIdentifier`: Name and fully qualified identifier
- `details`: Properties (with `changeFrom`/`changeTo` for modifications), column changes, body changes

**Grant Operations:**

- `association`: `GRANT`
- `subject`: The object being granted (existing or future grants)
- `target`: The grantee

**DMF Attachment Operations:**

- `association`: `DMF_ATTACHMENT`
- `subject`: Data metric function being attached
- `target`: Target object and columns

#### CRITICAL: Reading and Parsing Plan Output

**After running `plan`, you MUST read and parse `out/plan/plan_result.json`.**

1. **Read** `out/plan/plan_result.json` and check `status`
2. **If `PLAN_FAILED`**: report the `error` message
3. **If `SUCCESS`**: parse the `operations` array, categorize by CREATE/ALTER/DROP, and present a summary
4. **If plan output already exists** and definitions haven't changed, read the existing file instead of rerunning

#### Agent Guidance

1. **Always run plan before deploy** and read/parse the output JSON
2. **Highlight destructive changes**: Pay special attention to `DROP` and `ALTER` operations
3. **Summarize changes clearly**: Group by type and present a human-readable summary
4. **Watch for data-affecting changes**: column type changes, column drops, table drops, view body changes

---

### `snow dcm preview`

Returns sample rows from tables, views, or dynamic tables defined in the DCM project.

#### Syntax

```bash
snow dcm preview <identifier> -c <connection> --object <fqn> [options]
```

#### Arguments

| Argument     | Required | Description                       |
| ------------ | -------- | --------------------------------- |
| `identifier` | Yes      | The identifier of the DCM project |

#### Options

| Option     | Required | Description                                                                      |
| ---------- | -------- | -------------------------------------------------------------------------------- |
| `--object` | **Yes**  | Fully qualified name of the object to preview (e.g., `MY_DB.MY_SCHEMA.MY_TABLE`) |
| `--target` | No       | Target to use (from manifest.yml)                                                |
| `--limit`  | No       | Maximum number of rows to return                                                 |

#### Examples

```bash
# Preview a table
snow dcm preview MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection --object MY_DB.PUBLIC.CUSTOMERS

# Preview with row limit
snow dcm preview MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection --object MY_DB.PUBLIC.ORDERS --limit 10
```

#### Agent Guidance

1. **Use for data validation** after deploy to confirm data is correct
2. **Use `--limit`** to avoid overwhelming output
3. **Object must be fully qualified**: always use `<database>.<schema>.<object>` format

---

### `snow dcm deploy`

Applies changes defined in the DCM project to Snowflake. Executes actual DDL statements against live infrastructure.

#### Syntax

```bash
snow dcm deploy <identifier> -c <connection> [options]
```

#### Arguments

| Argument     | Required | Description                       |
| ------------ | -------- | --------------------------------- |
| `identifier` | Yes      | The identifier of the DCM project |

#### Options

| Option     | Description                                                                        |
| ---------- | ---------------------------------------------------------------------------------- |
| `--target` | Target from `manifest.yml` to use (bundles project identifier + templating config) |
| `--alias`  | Alias for the deployment (for tracking/management)                                 |

#### Examples

```bash
# Deploy from current directory
snow dcm deploy MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection

# Deploy with a specific target and alias
snow dcm deploy MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection --target PROD --alias "release-v1.2.0"
```

#### CRITICAL WARNINGS

> **DEPLOY IS A DESTRUCTIVE OPERATION**
>
> This command makes changes to live Snowflake infrastructure including creating, altering, and **dropping objects and their data**.

**Agent Guidelines:**

1. **ALWAYS run `plan` first** and review the output before every deploy
2. **REQUIRE user confirmation** before executing deploy
3. **Highlight destructive changes** if plan shows DROP or significant ALTER operations
4. **Encourage using `--alias`** to help track deployments

**Confirmation Template:**

```
You are about to deploy changes to Snowflake. This operation will:
- CREATE: [list of objects]
- ALTER: [list of objects]
- DROP: [list of objects]

Database: [database_name] | Connection: [connection_name]

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
# Drop a deployment (use single quotes for names containing $)
snow dcm drop-deployment MY_PROJECT 'DEPLOYMENT$1' -c myconnection

# Drop only if it exists
snow dcm drop-deployment MY_PROJECT my_deployment -c myconnection --if-exists
```

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

#### Warning

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

Runs expectation tests on tables, views, and dynamic tables defined in the DCM project.

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
snow dcm test MY_DB.MY_SCHEMA.MY_PROJECT -c myconnection
```

#### Use Cases

- CI/CD pipelines for data validation
- Post-deployment verification
- Ongoing data quality monitoring

---

## Workflow

See the parent SKILL.md for the recommended workflow sequence.

---

## Common Options Across Commands

| Option               | Description                                                                 |
| -------------------- | --------------------------------------------------------------------------- |
| `-c`, `--connection` | **Required**: Snowflake connection name                                     |
| `--target`           | Target from `manifest.yml` (bundles project identifier + templating config) |
| `--save-output`      | Save plan output artifacts to `out/` (plan command only)                    |

---

## Error Handling

When commands fail, check:

1. **Connection issues**: Verify the connection name and credentials
2. **Permission errors**: Ensure the user/role has required privileges
3. **Analysis errors**: Review the analyze output for validation issues
4. **Plan failures**: Check the `error` field in the plan output

For verbose debugging, add `--debug` to any command.
