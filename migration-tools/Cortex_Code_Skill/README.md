<!-- Human documentation only. Not part of the skill workflow. Agents: refer to SKILL.md instead. -->

# Cortex Skill: `dcm-migrate` - Bring existing Snowflake objects under DCM management

A [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) skill that takes a database you already have and hands back a working [DCM Project](https://docs.snowflake.com/en/developer-guide/snowflake-cli/dcm/overview) that manages it, without recreating or modifying any of your objects.

> ℹ️ **In Preview**: This skill is in preview and still evolving. Review the generated definitions and validate with PLAN before deploying to production.

## What you get

On the first PLAN, every existing object appears as a clean **adoption**: DCM recognizes the object you already have and attaches it to your project. No rebuilds, no property drift, no downtime. Your live objects keep working exactly as they are, and from that point on you manage them declaratively and under version control.

The skill also handles the parts of a migration that usually go wrong:

- **Nothing silently changes.** Retention, comments, clustering, change tracking and similar properties are carried across, so adoption does not quietly reset a table setting or drop a comment.
- **Running tasks and alerts stay running.** Objects that are started today are still started after deploy, instead of coming up suspended.
- **Secrets are never written out.** Objects carrying inline credentials are reported instead of emitted, so no keys or tokens land in your definition files.
- **One bad object does not block the rest.** Anything that cannot be migrated is reported with a reason, rather than failing the whole run.

## What it covers

| Category | Types |
|---|---|
| Structure | Database, Schemas, Tables, Views, Dynamic Tables |
| Programmatic | Tasks, Functions, Procedures (including overloads) |
| Ingestion | Pipes, Streams |
| Utility | Sequences, File Formats, Alerts, Tags |
| Governance | Masking Policies, Authentication Policies |
| Access | Roles, Database Roles, Grants |
| Storage | Internal Stages; External Stages backed by a storage integration |

Reported instead of migrated, each with the reason shown in the result:

- Semantic views
- Data metric functions
- External stages with inline credentials, or with no storage integration

Temporary stages are skipped.

Tag and policy **objects** are migrated. Their **attachments** to tables and views are not, because DCM cannot set those yet. The attachments stay on your live objects, they are simply not managed by the project. Re-apply them yourself if you want the project to own them.

For roles and grants, the skill captures the roles you own and the grants you made. Grants issued by a different role are reported by name, so you know to re-run as that role if you want them included.

## What the skill asks you

The skill needs five decisions from you, and it asks before doing any work:

1. **Which database**, and optionally which schemas or object types to limit the migration to.
2. **Whether to include roles and grants**, and at which scope (account, database, or schema).
3. **Which connection and role to migrate as.** If your role cannot see every object, the skill offers to migrate only what that role owns, so you can repeat the run per role instead of hitting permission errors.
4. **A target project**: a new one it creates for you, or an existing one to merge into.
5. **Whether to look for templating opportunities** at the end, if you want the same definitions to serve several environments.

## Where it stops for your approval

The skill pauses at six checkpoints and will not continue until you confirm:

- After resolving the target project, before generating anything
- After generating object definitions, so you can review what was and was not migrated
- After generating roles and grants
- After PLAN, so you can confirm the adoption is clean before anything is applied
- After DEPLOY, to confirm the result
- Before applying any templating changes

## Before you start

- Cortex Code with the `dcm-migrate` skill installed (place the skill directory under `.cortex/skills/`)
- A Snowflake connection whose role can see the objects you want to migrate
- `CREATE DCM PROJECT` privilege in the target schema, if the skill is creating the project for you

One thing catches people out: in Snowflake, owning a database does **not** automatically give you access to objects inside it that another role created. The migration can only capture what your role can actually see. A role in one of these categories will see everything:

- `ACCOUNTADMIN`
- A role with the global `MANAGE GRANTS` privilege (by default only `SECURITYADMIN`)
- A role that owns the database and everything inside it, common where one role created it all
- A role with explicit privileges on every object you want migrated

If none of those fit, migrate role by role. The skill will offer this, and you repeat the run for each role that owns part of the database. See [Overview of Access Control](https://docs.snowflake.com/en/user-guide/security-access-control-overview) for background.

## Getting started

Tell the agent what you want to migrate, for example:

> "Migrate the ANALYTICS_DB database into a new DCM project"

> "Import the RAW and SERVE schemas from PROD_DB into my existing DCM project"

> "Migrate ANALYTICS_DB including roles and grants, but only the objects my role owns"

The agent takes it from there and stops at each checkpoint above.

## If the plan is not clean

A clean adoption means PLAN shows no changes to your objects beyond the entry that records the project association. `GRANT` operations are also expected, since they only add access. The skill works through any remaining differences with you, and these four are known and benign:

- **A view shows a one-time difference.** Views written with a trailing semicolon, or referencing objects without their full name, differ harmlessly on the first plan and settle after the first deploy.
- **A stage wants an `ALTER`.** Inline file format and copy options cannot be read back from Snowflake, so a stage configured with non-default values needs those clauses added by hand.
- **A stream fails the plan on `SHOW_INITIAL_ROWS`.** This property is not readable back from Snowflake but is still enforced, so add `SHOW_INITIAL_ROWS = TRUE` to that stream's definition and plan again. Everything else about a stream adopts as is, and its offset is preserved.
- **Expected grants are missing.** Only grants made by the role you ran as are captured. The result names the other granting roles so you can re-run as each.

## Without Cortex Code

Two stored procedures cover the file generation step on their own, for environments where Cortex Code is not available: `DDL_TO_DCM_DEFINITIONS` for object structure and `GRANTS_TO_DCM_DEFINITIONS` for roles and grants. They write the same definition files, but you set up the project and run ANALYZE, PLAN and DEPLOY yourself.

Both procedures and their full documentation, including parameters and the manual workflow, are in [`../python_procedures/`](../python_procedures/).
