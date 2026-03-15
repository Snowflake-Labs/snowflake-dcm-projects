---
name: deploy-project
description: "Safe deployment workflow for DCM projects. Triggers: deploy dcm, apply changes, push to snowflake, run deployment"
---

# Deploy DCM Project

## Overview

This sub-skill provides a safe, structured workflow for deploying DCM project changes to Snowflake. Deployment is a critical operation that can create, modify, or delete database objects.

## ⚠️ CRITICAL SAFETY RULES

1. **NEVER deploy without running plan first**
2. **NEVER deploy without explicit user confirmation**
3. **ALWAYS highlight destructive changes (DROP, data-affecting ALTER)**
4. **ALWAYS recommend using deployment aliases**

## Prerequisites

Before deploying, ensure:

- [ ] Analyze has been run successfully (no errors)
- [ ] Plan has been run successfully (no PLAN_FAILED status)
- [ ] User has reviewed the plan summary
- [ ] User has given explicit confirmation to proceed

## Deployment Workflow

### Step 0: Pre-Flight Checks

**A. Scan for legacy DDL hooks:**

Before proceeding, scan definition files for `ATTACH PRE_HOOK` or `ATTACH POST_HOOK`. If found, inform the user that DDL hooks are not supported in the current version of DCM and offer to extract the hook contents into `pre_deploy.sql` / `post_deploy.sql` companion scripts. ⚠️ Warn that companion scripts do NOT support Jinja — any `{{ }}` variables must be replaced with literal values or shell variable substitution. Do not proceed with plan until hooks are resolved.

**B. Detect companion scripts:**

Check the project root for these files:
- `pre_deploy.sql` — must run before `snow dcm plan` (creates objects the planner validates against)
- `post_deploy.sql` — must run after `snow dcm deploy`
- `post_deployment_grants.sql` — must run after deploy (existing behavior)

If any are found, present a single consolidated summary:

> "I found companion scripts for this project:
> - `pre_deploy.sql` — will run before plan (⚠️ may require elevated roles like ACCOUNTADMIN)
> - `post_deploy.sql` — will run after deploy
> - `post_deployment_grants.sql` — will run after deploy
>
> Shall I run these at the appropriate times during the deployment workflow?"

Get a single approval. If `pre_deploy.sql` exists, proceed to run it before Step 1.

**Running `pre_deploy.sql`:**

```bash
snow sql -f pre_deploy.sql -c <connection> --role ACCOUNTADMIN
```

⚠️ Objects in `pre_deploy.sql` often require elevated roles (ACCOUNTADMIN for integrations, SECURITYADMIN for network policies). Warn the user about role requirements.

### Step 1: Verify Analyze Passed

If analyze hasn't been run recently:

```bash
snow dcm raw-analyze <identifier> -c <connection> \
    --target <target> \
```

#### ⚠️ CRITICAL: Read and Parse the Output

**You MUST read and parse command output.**

**Do NOT proceed if there are errors.**

### Step 2: Run Plan (or Use Existing Output)

**Check if plan output already exists:**

- If `out/plan/plan_result.json` exists and is current, **read it instead of rerunning**
- Only rerun plan if explicitly requested by user or if definitions have changed

If plan needs to be run:

```bash
snow dcm plan <identifier> -c <connection> \
    --target <target> \
    --save-output
```

#### ⚠️ CRITICAL: Read and Parse the Output

**You MUST read and parse `out/plan/plan_result.json`.**

For detailed instructions, see: [Parent SKILL.md - Critical: Reading Output Files](../SKILL.md#critical-reading-output-files)

**Do NOT proceed to deployment without parsing the plan output.**

### Step 3: Present Plan Summary

Parse the plan output and present a clear summary:

```
╔══════════════════════════════════════════════════════════════╗
║                    📊 DEPLOYMENT PLAN                        ║
╠══════════════════════════════════════════════════════════════╣
║ Project:       <DATABASE.SCHEMA.PROJECT_NAME>                ║
║ Target:        <target_name>                                  ║
║ Connection:    <connection_name>                             ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║ ✅ CREATE: X objects                                         ║
║    • TABLE: MY_DB.RAW.NEW_TABLE                              ║
║    • DYNAMIC_TABLE: MY_DB.ANALYTICS.SUMMARY                  ║
║    • TASK: MY_DB.ETL.TSK_DAILY_LOAD                          ║
║    • ROLE: MY_DB_READER                                      ║
║                                                              ║
║ ⚠️  ALTER: Y objects                                          ║
║    • TABLE: MY_DB.RAW.CUSTOMERS                              ║
║      └─ Add column: EMAIL (VARCHAR)                          ║
║      └─ Change column: AMOUNT (NUMBER → NUMBER(15,2))        ║
║    • TASK: MY_DB.ETL.TSK_PROCESS_ORDERS                      ║
║      └─ Task will be auto-suspended, modified, and resumed   ║
║                                                              ║
║ 🚨 DROP: Z objects                                           ║
║    • VIEW: MY_DB.SERVE.OLD_REPORT                            ║
║      ⚠️  WARNING: This object and its data will be deleted   ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
```

**Note on Tasks:** When tasks are modified during deployment, DCM automatically suspends/resumes them - no manual intervention needed.

### Step 4: Highlight Risky Changes

**For each of these scenarios, add explicit warnings:**

| Change Type        | Risk Level | Warning Message                                           |
| ------------------ | ---------- | --------------------------------------------------------- |
| DROP TABLE         | 🚨 HIGH    | "All data in this table will be permanently deleted"      |
| DROP VIEW          | ⚠️ MEDIUM  | "This view will be removed; dependent queries will fail"  |
| DROP DYNAMIC TABLE | 🚨 HIGH    | "All computed data will be lost"                          |
| DROP TASK          | ⚠️ MEDIUM  | "Scheduled operations will stop; task will be deleted"    |
| ALTER TASK         | ℹ️ INFO    | "Task will be auto-suspended, modified, and auto-resumed" |
| Column DROP        | 🚨 HIGH    | "Data in this column will be permanently lost"            |
| Column TYPE change | ⚠️ MEDIUM  | "May fail if existing data is incompatible with new type" |
| GRANT revocation   | ⚠️ MEDIUM  | "Users/roles may lose access"                             |

### Step 5: Offer Preview (Optional)

**Ask the user:**

> "Before deployment, would you like to preview data in any of the objects that will be modified or dropped?"

If yes:

```bash
snow dcm preview <identifier> -c <connection> \
    --object <fully.qualified.object.name> \
    --target <target> \
    --limit 10
```

This is especially useful for:

- Verifying data before DROP operations
- Checking current state before ALTER operations

### Step 6: Request Explicit Confirmation

**Present a clear confirmation prompt:**

```
╔══════════════════════════════════════════════════════════════╗
║              ⚠️  DEPLOYMENT CONFIRMATION                     ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║ You are about to deploy changes to LIVE Snowflake            ║
║ infrastructure.                                              ║
║                                                              ║
║ • Creates: X objects                                         ║
║ • Alters:  Y objects                                         ║
║ • Drops:   Z objects                                         ║
║                                                              ║
║ Target Database: <database>                                  ║
║ Using Connection: <connection>                               ║
║                                                              ║
║ This action CANNOT be automatically undone.                  ║
║                                                              ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║ Type "yes" to confirm deployment, or "no" to cancel.         ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
```

**DO NOT PROCEED unless user explicitly confirms with "yes".**

### Step 7: Execute Deployment

**Suggest an alias for tracking:**

> "Would you like to add a deployment alias for tracking? (e.g., 'v1.0', 'add-email-column', 'initial-setup')"

```bash
snow dcm deploy <identifier> -c <connection> \
    --target <target> \
    --alias "<user-provided-alias-or-timestamp>"
```

### Step 8: Verify Deployment

After successful deployment:

1. **Confirm success** - Report the deployment completed

2. **Check for tests** - From the analyze output, determine if the project has data quality tests:

   ```
   Do any definitions have DATA_METRIC_SCHEDULE or ATTACH DATA METRIC FUNCTION?
   ```

3. **Offer to run tests:**
   > "This project has data quality tests defined. Would you like to run them to verify the deployment?"

### Step 8.1: Run Post-Deploy Script (If Any)

If `post_deploy.sql` exists and user approved companion scripts in Step 0:

```bash
snow sql -f post_deploy.sql -c <connection>
```

⚠️ If the script requires a specific role (e.g., objects referencing integrations created by ACCOUNTADMIN), add `--role <ROLE>` or ensure the script includes `USE ROLE` statements.

This creates objects that depend on DEFINE'd entities (streams, alerts, file formats, external stages). These scripts are safe to re-run if they use `CREATE IF NOT EXISTS` or `CREATE OR REPLACE`.

### Step 8.2: Apply Unsupported Grants (If Any)

Check if the project has a `post_deployment_grants.sql` file (grants that DCM cannot apply):

If file exists, present to user:

> "The following grants could not be applied by DCM and require manual execution:
> - [list grants from post_deployment_grants.sql]
> 
> These typically include: `GRANT ... ON ACCOUNT`, `GRANT IMPORTED PRIVILEGES`, warehouse grants to database roles, etc.
> 
> Would you like me to execute these grants now? (Requires appropriate privileges)"

**⚠️ CHECKPOINT**: Get explicit approval before running post-deployment grants.

### Step 9: Run Tests (If Requested)

If the project has tests and user wants to run them:

1. **Refresh dynamic tables first** (ensures fresh data):

   ```bash
   snow dcm refresh <identifier> -c <connection>
   ```

2. **Run tests:**

   ```bash
   snow dcm test <identifier> -c <connection> 
   ```

3. **Report test results** - Summarize pass/fail status

## Post-Deployment

### Viewing Deployment History

```bash
snow dcm list-deployments <identifier> -c <connection>
```

### Rolling Back

DCM doesn't have automatic rollback. To "undo" a deployment:

1. Modify definition files to previous state
2. Run plan to see what will change
3. Deploy the reverted definitions

**Recommendation:** Keep definition files in version control (git) for easy rollback.

## Error Handling

### Plan Fails

If plan returns `PLAN_FAILED`:

- Check the `error` field in the output
- Common causes:
  - Invalid SQL syntax
  - Missing dependencies
  - Permission issues
  - Object state conflicts

### Deployment Fails

If deploy fails:

- Note the error message
- Check Snowflake query history for failed DDL
- Some objects may have been created/modified before failure
- Run plan again to see remaining changes

### Partial Deployment

If deployment partially succeeds:

- Some objects were created/modified
- Run plan to see what's left
- Fix any errors and re-deploy

## Quick Reference

### Commands Used

| Command                     | Purpose                       |
| --------------------------- | ----------------------------- |
| `snow dcm raw-analyze`          | Validate project, find errors |
| `snow dcm plan`             | Preview what will change      |
| `snow dcm preview`          | View sample data from objects |
| `snow dcm deploy`           | Apply changes to Snowflake    |
| `snow dcm refresh`          | Refresh dynamic tables        |
| `snow dcm test`             | Run data quality tests        |
| `snow dcm list-deployments` | View deployment history       |

### Always Include

- `-c <connection>`: Connection name
- `--target <target>`: Target name from manifest.yml (bundles project identifier + templating config)
- `--save-output`: [important!] For plan command only

### Recommended

- `--alias "<name>"`: For deploy, to track deployment history
