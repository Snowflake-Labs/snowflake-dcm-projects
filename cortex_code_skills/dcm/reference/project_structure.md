# DCM Project Structure Reference

This document describes the structure of DCM (Database Change Management) projects, including the manifest file, deployment targets, and configuration management.

> **Note for Cortex Agents**: This document covers project structure only. For DCM definition syntax (`DEFINE` statements, grants, data quality, etc.), see `syntax_overview.md` and the per-object guides in `primitives/`.

---

## Project Overview

A DCM project is a directory containing:

1. **`manifest.yml`** — The project manifest (required)
2. **Definition files** — SQL files with `DEFINE` statements (`.sql` files) in `sources/definitions/`

### Recommended Structure

```
my_project/
├── manifest.yml
├── pre_deploy.sql          (optional — runs before snow dcm plan)
├── post_deploy.sql         (optional — runs after snow dcm deploy)
├── post_deployment_grants.sql  (optional — unsupported grants)
└── sources/
    ├── definitions/
    │   ├── <definition_name>.sql
    │   ├── <definition_name>.sql
    │   └── <definition_name>.sql
    └── macros/
        └── <macro_name>.sql
```

> **Companion scripts** (`pre_deploy.sql`, `post_deploy.sql`) live at the project root alongside `manifest.yml`. They are NOT placed in `sources/definitions/` and are NOT referenced in the manifest. See `primitives/unsupported_objects.md` for details on what goes in each file.

> **Macros directory**: Place global Jinja macro files in `sources/macros/`. Unlike macros defined inline in definition files, macros in `sources/macros/` are accessible from all definition files.

> **Important**: In manifest v2, definition files must be placed in `sources/definitions/`. This path is fixed and auto-discovered. Nest files logically within it (by purpose or area). In simple cases, prefer a flat structure.

### Alternative Structures

Nested folders within `sources/definitions/` are supported but add complexity without significant benefit for most use cases:

```
project/
├── manifest.yml
└── sources/
    └── definitions/
        ├── expectations.sql
        ├── grants.sql
        ├── high_level_objects.sql
        ├── TPCDI_ODS/
        │   └── DYNAMIC_TABLES.sql
        ├── TPCDI_STG/
        │   ├── DYNAMIC_TABLES.sql
        │   └── TABLES.sql
        └── TPCDI_WH/
            ├── DYNAMIC_TABLES.sql
            └── VIEWS.sql
```

---

## The Manifest File (`manifest.yml`)

The manifest file is the heart of a DCM project. It defines deployment targets, templating configurations, and project metadata.

### Complete Schema

```yaml
manifest_version: 2
type: DCM_PROJECT
default_target: 'DEV'

targets:
  DEV:
    account_identifier: MY_DEV_ACCOUNT
    project_name: 'MY_DB.MY_SCHEMA.MY_PROJECT_DEV'
    project_owner: DCM_DEVELOPER
    templating_config: 'DEV'
  PROD:
    account_identifier: MY_PROD_ACCOUNT
    project_name: 'MY_DB.MY_SCHEMA.MY_PROJECT'
    project_owner: DCM_PROD_DEPLOYER
    templating_config: 'PROD'

templating:
  defaults:
    suffix: '_DEV'
    wh_size: 'XSMALL'
  configurations:
    DEV:
      wh_size: 'XSMALL'
    PROD:
      wh_size: 'LARGE'
      suffix: ''
```

### Required Fields

> **Manifest Schema:** Only the fields documented below are valid. Any other fields will cause "is not defined in the schema" errors.

- **`manifest_version`** (`number`): The manifest schema version. Use version `2`.
- **`type`** (`string`, case-insensitive): Must be `DCM_PROJECT`.
- **`default_target`** (`string`, optional): The target used when `--target` is not specified on the CLI. Must match a key in `targets`.

#### `targets`

**Type**: `object` (Record of target names to target configurations)

Defines named deployment targets. Each target specifies:

- `account_identifier` (optional): The Snowflake account identifier (run `SELECT CURRENT_ACCOUNT()` to find yours)
- `project_name` (required): The fully qualified DCM project identifier (`DATABASE.SCHEMA.PROJECT_NAME`)
- `project_owner` (optional): The role with OWNERSHIP on the DCM project object (run `DESCRIBE DCM PROJECT` to find the owner)
- `templating_config` (optional): Which templating configuration to use for this target

> **Best Practice**: Embed the project identifier in the manifest targets rather than passing it as a CLI argument. This makes the project self-describing.

> **Note**: The `--target` CLI flag refers to target names defined here (e.g., `--target DEV`). Each target can point to a different DCM project and a different templating configuration.

> **CRITICAL -- Unique project_name per account**: When multiple targets share the same `account_identifier`, each MUST have a unique `project_name`. If two targets on the same account have the same `project_name`, deploying one target will overwrite the other. Use environment suffixes (e.g., `_DEV`, `_STG`, `_PROD`) to differentiate. Targets on *different* accounts may safely share a `project_name` since they are isolated Snowflake instances.

### Optional Fields

#### `templating`

**Type**: `object` with `defaults` and `configurations` sub-keys

Defines Jinja template variables available in definition files. Only needed when definitions use Jinja templating.

- `defaults`: Variables shared across all configurations. Used as-is when a configuration does not override them.
- `configurations`: Named sets of variable overrides. Each configuration can override any default value.

When a target specifies `templating_config: 'PROD'`, the template variables are resolved by merging `defaults` with the `PROD` configuration (configuration values take precedence).

---

## Multi-Target Patterns

The following example shows a typical multi-environment, multi-region setup:

```yaml
targets:
  DCM_DEV:
    account_identifier: PM-DCM_DEV
    project_name: DCM_DEMO.PROJECTS.DCM_PROJECT_DEV
    project_owner: DCM_DEVELOPER
    templating_config: DEV
  DCM_STAGE:
    account_identifier: PM-DCM_STAGE
    project_name: DCM_DEMO.PROJECTS.DCM_PROJECT_STG
    project_owner: DCM_STAGE_DEPLOYER
    templating_config: STAGE
  DCM_PROD_US:
    account_identifier: PM-DCM_PROD
    project_name: DCM_DEMO.PROJECTS.DCM_PROJECT_PROD
    project_owner: DCM_PROD_DEPLOYER
    templating_config: PROD
  DCM_PROD_EU:
    account_identifier: PM-DCM_PROD_EU
    project_name: DCM_DEMO.PROJECTS.DCM_PROJECT_PROD
    project_owner: DCM_PROD_DEPLOYER
    templating_config: PROD
```

- Each target on a unique account has a distinct `project_name`: `DCM_PROJECT_DEV`, `DCM_PROJECT_STG`, `DCM_PROJECT_PROD`
- `DCM_PROD_US` and `DCM_PROD_EU` safely share `DCM_PROJECT_PROD` because they deploy to different accounts
- Multiple targets can share the same `templating_config` -- both PROD targets use the `PROD` configuration

---

## Variable Resolution Hierarchy

Variables are resolved in a three-tier hierarchy where later tiers override earlier ones:

1. **`templating.defaults`** -- Base values shared by all configurations
2. **`templating.configurations.<name>`** -- Configuration-specific overrides
3. **Runtime `--variable` flag** -- CLI execution-time overrides

Example: If `defaults` sets `wh_size: 'XSMALL'` and the PROD configuration sets `wh_size: 'LARGE'`, the effective value for PROD targets is `'LARGE'`. A runtime `--variable "wh_size='MEDIUM'"` would override both.

```yaml
templating:
  defaults:
    wh_size: 'XSMALL'
    suffix: '_DEV'
    retention: '14 days'
  configurations:
    DEV:
      wh_size: 'XSMALL'
    PROD:
      wh_size: 'LARGE'
      suffix: ''
```

**Effective variables for DEV**: `wh_size='XSMALL'`, `suffix='_DEV'`, `retention='14 days'`
**Effective variables for PROD**: `wh_size='LARGE'`, `suffix=''`, `retention='14 days'`

### Supported Value Types

| Type    | Example             | Usage in Jinja         |
| ------- | ------------------- | ---------------------- |
| String  | `db: "PROD"`        | `{{db}}`               |
| Number  | `timeout: 300`      | `{{timeout}}`          |
| Boolean | `enabled: true`     | `{% if enabled %}`     |
| Array   | `users: ["A", "B"]` | `{% for u in users %}` |
| Dict    | `teams: [{name: "HR", wh_size: "LARGE"}]` | `{% for team in teams %}{{ team.name }}{% endfor %}` |

For Jinja templating syntax and examples, see `primitives/jinja_templating.md`.

---

## Definition Files

Definition files are SQL files containing DCM `DEFINE` statements. They describe the desired state of Snowflake objects. All definition files must be placed in `sources/definitions/` (or subdirectories within it).

### File Organization

| File                        | Contents                                   |
| --------------------------- | ------------------------------------------ |
| `database.sql` or `raw.sql` | Databases, schemas, base tables            |
| `analytics.sql`             | Dynamic tables, analytical transformations |
| `serve.sql`                 | Views for consumption                      |
| `access.sql`                | Roles, grants, permissions                 |
| `expectations.sql`          | Data metric functions, data quality rules  |

For definition syntax and examples for each object type, see `syntax_overview.md` and the individual primitive files.

---

## Working with Multiple Projects

A single DCM project manages a set of related Snowflake objects deployed together. When deciding whether to use one project or split into multiple, consider:

- **Ownership boundaries**: Objects managed by different teams or roles are natural candidates for separate projects, since each project has a single `project_owner` per target.
- **Templating scope**: All definitions in a project share the same templating variables. If two groups of objects need fundamentally different variable sets, separate projects avoid complexity.
- **Deployment independence**: Separate projects can be deployed independently on different schedules. Objects that must always be deployed together belong in the same project.
- **Divergence complexity**: Multi-target templating works well for small differences (warehouse sizes, suffixes). If environments diverge significantly in object structure, separate projects with simpler definitions may be clearer than heavy Jinja branching.

---

## Naming Conventions

| Convention                      | Example                            | Purpose                             |
| ------------------------------- | ---------------------------------- | ----------------------------------- |
| Environment suffix in names     | `MY_DB_{{env_suffix}}`             | Distinguish objects per environment |
| Uppercase for Snowflake objects | `DEFINE TABLE MY_DB.RAW.CUSTOMERS` | Match Snowflake conventions         |
| Descriptive file names          | `access.sql`, `expectations.sql`   | Easy navigation                     |

### Templating Variable Naming

| Variable             | Description            | Example Values         |
| -------------------- | ---------------------- | ---------------------- |
| `env_suffix`         | Object name suffix     | `"_DEV"`, `""`         |
| `wh_size`            | Warehouse size         | `"XSMALL"`, `"LARGE"`  |
| `users`              | User list for grants   | `["USER1", "USER2"]`   |
| `teams`              | Team/schema list       | `["Finance", "HR"]`    |
| `project_owner_role` | Top-level role         | `"DCM_DEVELOPER"`      |

---

## Summary

| Component                 | Required         | Purpose                               |
| ------------------------- | ---------------- | ------------------------------------- |
| `manifest.yml`            | Yes              | Project configuration and metadata    |
| `targets`                 | Yes              | Deployment target definitions         |
| `default_target`          | Yes              | Default target when CLI omits it      |
| `templating`              | No (recommended) | Environment-specific variables        |
| `sources/definitions/`    | Yes              | SQL files with DEFINE statements      |
| `pre_deploy.sql`          | No               | Imperative SQL run before plan (integrations, network policies) |
| `post_deploy.sql`         | No               | Imperative SQL run after deploy (streams, alerts, external stages) |
| `post_deployment_grants.sql` | No            | Grants that DCM cannot apply          |

**The simplest valid project**:

```yaml
# manifest.yml
manifest_version: 2
type: DCM_PROJECT
default_target: 'DEV'

targets:
  DEV:
    account_identifier: MY_ACCOUNT
    project_name: 'MY_DB.MY_SCHEMA.MY_PROJECT'
    project_owner: DCM_DEVELOPER
```

```sql
-- sources/definitions/main.sql
DEFINE DATABASE MY_PROJECT;
DEFINE SCHEMA MY_PROJECT.RAW;
```

For syntax details on `DEFINE` statements, grants, and data quality rules, see `syntax_overview.md` and the per-object guides in `primitives/`.
