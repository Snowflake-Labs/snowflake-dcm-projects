# DCM Syntax Overview

This document provides the core principles and a compact reference for DCM definition syntax. For detailed syntax, examples, and best practices for each object type, load the corresponding primitive reference file.

---

## Core Principle: DEFINE vs CREATE

DCM uses the `DEFINE` keyword instead of `CREATE` for named Snowflake objects. The syntax is identical to the corresponding `CREATE` statement with only the keyword changed:

```sql
-- Standard Snowflake DDL (imperative)
CREATE TABLE my_db.my_schema.my_table (
    id NUMBER,
    name VARCHAR
);

-- DCM Definition (declarative)
DEFINE TABLE my_db.my_schema.my_table (
    id NUMBER,
    name VARCHAR
);
```

This declarative approach means:
- Running the same definition multiple times is idempotent
- DCM determines whether to create, alter, or leave the object unchanged
- Object dependencies are resolved automatically
- Removing a DEFINE statement causes the object to be dropped on next deployment

### Critical Constraint

A DCM project CANNOT define its own parent database or schema. If the project identifier is `MY_DB.MY_SCHEMA.MY_PROJECT`, you cannot use `DEFINE DATABASE MY_DB` or `DEFINE SCHEMA MY_DB.MY_SCHEMA` in your definitions. These containers must already exist.

### Imperative Syntax (Non-DEFINE)

Some operations retain standard SQL syntax:
- **Grants**: `GRANT ... TO ROLE/DATABASE ROLE ...` (standard SQL, not DEFINE)
- **DMF Attachments**: `ATTACH DATA METRIC FUNCTION ... TO TABLE ...`

### Fully Qualified Names

All objects MUST use fully qualified names: `database.schema.object_name`.

---

## Supported Entities

| Object Type | Keyword | Notes | Primitive Reference |
|-------------|---------|-------|---------------------|
| Database | `DEFINE DATABASE` | Cannot rename | `primitives/databases_and_schemas.md` |
| Schema | `DEFINE SCHEMA` | WITH MANAGED ACCESS supported | `primitives/databases_and_schemas.md` |
| Table | `DEFINE TABLE` | CHANGE_TRACKING, DATA_METRIC_SCHEDULE | `primitives/tables.md` |
| View | `DEFINE VIEW` | DATA_METRIC_SCHEDULE (cron only) | `primitives/views.md` |
| Dynamic Table | `DEFINE DYNAMIC TABLE` | WAREHOUSE, TARGET_LAG required | `primitives/dynamic_tables.md` |
| Warehouse | `DEFINE WAREHOUSE` | Uses WITH clause | `primitives/warehouses.md` |
| Account Role | `DEFINE ROLE` | Account-wide scope | `primitives/roles_and_grants.md` |
| Database Role | `DEFINE DATABASE ROLE` | Database-scoped | `primitives/roles_and_grants.md` |
| Internal Stage | `DEFINE STAGE` | Encryption immutable after creation | `primitives/stages.md` |
| Task | `DEFINE TASK` | Auto suspend/resume during deploy | `primitives/tasks.md` |
| SQL Function | `DEFINE FUNCTION` | No auto dependency sorting | `primitives/sql_functions.md` |
| Data Metric Function | `DEFINE DATA METRIC FUNCTION` | Custom DMFs | `primitives/data_quality.md` |
| Tag | `DEFINE TAG` | Can define but cannot attach to objects | -- |
| Authentication Policy | `DEFINE AUTHENTICATION POLICY` | PAT policies | -- |
| Grants | `GRANT ... TO ...` | Imperative syntax | `primitives/roles_and_grants.md` |
| DMF Attachments | `ATTACH DATA METRIC FUNCTION` | With EXPECTATION clause | `primitives/data_quality.md` |
| Jinja Templating | `{{ }}`, `{% %}` | Variables, loops, macros | `primitives/jinja_templating.md` |

> **Object types not listed above** (streams, alerts, external stages, integrations, network rules/policies, shares, file formats, semantic views) are not supported by DEFINE. Load `primitives/unsupported_objects.md` for guidance on managing these with companion SQL scripts.

---

## Primitive Loading Guide

Load ONLY the references needed for the current task.

**Object primitives** -- load based on what the user explicitly needs:

| When the user needs... | Load |
|------------------------|------|
| Databases, schemas | `reference/primitives/databases_and_schemas.md` |
| Source/staging tables | `reference/primitives/tables.md` |
| Consumption views | `reference/primitives/views.md` |
| Pipeline transformations | `reference/primitives/dynamic_tables.md` |
| Scheduled operations, ETL DAGs | `reference/primitives/tasks.md` |
| File staging (internal) | `reference/primitives/stages.md` |
| Compute resources | `reference/primitives/warehouses.md` |
| Access control, permissions | `reference/primitives/roles_and_grants.md` |
| User-defined functions | `reference/primitives/sql_functions.md` |

**Mechanism references** -- load when derived conditions apply:

| Reference | Trigger conditions |
|-----------|--------------------|
| `reference/primitives/jinja_templating.md` | Multi-environment support; templated naming; team/user loops; conditional objects; macros; parameterized configs |
| `reference/primitives/unsupported_objects.md` | User mentions streams, alerts, external stages, integrations, network rules/policies, shares, file formats, semantic views; OR existing files contain `ATTACH PRE_HOOK` / `POST_HOOK`; OR user asks about object types not in the Supported Entities table |
| `reference/primitives/data_quality.md` | Data quality expectations; DMF attachments; DATA_METRIC_SCHEDULE; null/uniqueness/freshness checks; custom metric functions |

---

## Common Object Properties

| Property | Applies To | Description |
|----------|-----------|-------------|
| `COMMENT` | Most objects | Descriptive text |
| `CHANGE_TRACKING` | Tables | Enable CDC tracking |
| `DATA_METRIC_SCHEDULE` | Tables, Views, Dynamic Tables | When DMF expectations are evaluated |

---

## Definition File Organization

All definition files go in `sources/definitions/`. Recommended file grouping:

| File | Contents |
|------|----------|
| `infrastructure.sql` | Databases, schemas, warehouses, internal stages |
| `tables.sql` or `raw.sql` | Table definitions |
| `analytics.sql` | Dynamic tables, transformations |
| `serve.sql` | Views for consumption |
| `access.sql` | Roles, grants, permissions |
| `expectations.sql` | Data metric functions, quality rules |

Definition files can ONLY contain DEFINE, GRANT, ATTACH, or Jinja statements. Standard SQL commands (ALTER, INSERT, SELECT) are not supported.
