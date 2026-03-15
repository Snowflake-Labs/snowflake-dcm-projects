---
name: roles-and-grants
description: "Best practices for roles and grants in DCM projects. Triggers: dcm roles, dcm grants, warehouse grant error, permission model, database role limitations"
---

# DCM Role and Grant Guidelines

## Purpose

This skill provides best practices and patterns for defining roles and grants in DCM projects. It covers recommended role structures, grant patterns, and how to handle DCM limitations.

## When to Use

- Designing role hierarchies for a new DCM project
- Troubleshooting DCM plan failures related to roles or grants
- Understanding which grant patterns DCM supports vs. requires manual handling
- Implementing clean permission models in DCM

## Stopping Points

- ✋ **After analysis**: Present findings, get approval before designing
- ✋ **After design**: Present proposed structure, get approval before writing definitions
- ✋ **Before DCM commands**: Get explicit approval before running any DCM commands

## Reference

For complete syntax details, see:
- [Roles and Grants Primitive](../reference/primitives/roles_and_grants.md) - Grant syntax, role types, and supported patterns

## Recommended Role Structure

### The Three-Tier Pattern

For most DCM projects, use this simplified role hierarchy:

```
DATABASE ROLE <DB>.ADMIN      -- Full control (DDL + DML)
DATABASE ROLE <DB>.DEVELOPER  -- Read/write (DML only)
DATABASE ROLE <DB>.ANALYST    -- Read-only (SELECT only)
ROLE <PROJECT>_WAREHOUSE_USER -- Warehouse access (account role)
```

**Why this works:**
- Database roles are scoped to the database lifecycle
- Role inheritance minimizes grant duplication
- Separate account role handles warehouse access (required due to Snowflake constraint)

### Role Hierarchy

Use inheritance so higher roles automatically get lower role privileges:

```sql
-- Analyst is base level (read-only)
GRANT DATABASE ROLE <DB>.ANALYST TO DATABASE ROLE <DB>.DEVELOPER;

-- Developer inherits Analyst + gets write access
GRANT DATABASE ROLE <DB>.DEVELOPER TO DATABASE ROLE <DB>.ADMIN;

-- Admin inherits Developer + gets DDL access
GRANT DATABASE ROLE <DB>.ADMIN TO ROLE SYSADMIN;
```

### Warehouse Access Pattern

**⚠️ Critical:** Database roles CANNOT be granted warehouse privileges. Use an account role:

```sql
-- Account role for warehouse access
DEFINE ROLE <PROJECT>_WAREHOUSE_USER
COMMENT = 'Warehouse access for project users';

-- Grant warehouses to account role only
GRANT USAGE ON WAREHOUSE <PROJECT>_WH TO ROLE <PROJECT>_WAREHOUSE_USER;

-- Connect to role hierarchy
GRANT ROLE <PROJECT>_WAREHOUSE_USER TO ROLE SYSADMIN;
```

Users receive warehouse access by being granted this account role separately from their database role.

## Complete Example: access.sql

```sql
-- Database roles
DEFINE DATABASE ROLE <DATABASE_NAME>.ADMIN
COMMENT = 'Full administrative access';

DEFINE DATABASE ROLE <DATABASE_NAME>.DEVELOPER
COMMENT = 'Read/write access for developers';

DEFINE DATABASE ROLE <DATABASE_NAME>.ANALYST
COMMENT = 'Read-only access for analysts';

-- Account role for warehouse access
DEFINE ROLE <PROJECT>_WAREHOUSE_USER
COMMENT = 'Warehouse access for project users';

-- Role hierarchy
GRANT DATABASE ROLE <DATABASE_NAME>.ANALYST TO DATABASE ROLE <DATABASE_NAME>.DEVELOPER;
GRANT DATABASE ROLE <DATABASE_NAME>.DEVELOPER TO DATABASE ROLE <DATABASE_NAME>.ADMIN;
GRANT DATABASE ROLE <DATABASE_NAME>.ADMIN TO ROLE SYSADMIN;
GRANT ROLE <PROJECT>_WAREHOUSE_USER TO ROLE SYSADMIN;

-- Warehouse grants (to account role only)
GRANT USAGE ON WAREHOUSE <PROJECT>_WH TO ROLE <PROJECT>_WAREHOUSE_USER;

-- Database usage (required before schema/object access)
GRANT USAGE ON DATABASE <DATABASE_NAME> TO DATABASE ROLE <DATABASE_NAME>.ANALYST;

-- Schema grants
GRANT USAGE ON SCHEMA <DATABASE_NAME>.RAW TO DATABASE ROLE <DATABASE_NAME>.ANALYST;
GRANT USAGE ON SCHEMA <DATABASE_NAME>.STAGING TO DATABASE ROLE <DATABASE_NAME>.ANALYST;
GRANT USAGE ON SCHEMA <DATABASE_NAME>.ANALYTICS TO DATABASE ROLE <DATABASE_NAME>.ANALYST;

-- Object grants: Analyst (read-only)
GRANT SELECT ON ALL TABLES IN SCHEMA <DATABASE_NAME>.RAW TO DATABASE ROLE <DATABASE_NAME>.ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA <DATABASE_NAME>.STAGING TO DATABASE ROLE <DATABASE_NAME>.ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA <DATABASE_NAME>.ANALYTICS TO DATABASE ROLE <DATABASE_NAME>.ANALYST;

-- Object grants: Developer (read/write) - inherits SELECT from ANALYST
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA <DATABASE_NAME>.RAW TO DATABASE ROLE <DATABASE_NAME>.DEVELOPER;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA <DATABASE_NAME>.STAGING TO DATABASE ROLE <DATABASE_NAME>.DEVELOPER;

-- Schema grants: Admin (DDL)
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA <DATABASE_NAME>.RAW TO DATABASE ROLE <DATABASE_NAME>.ADMIN;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA <DATABASE_NAME>.STAGING TO DATABASE ROLE <DATABASE_NAME>.ADMIN;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA <DATABASE_NAME>.ANALYTICS TO DATABASE ROLE <DATABASE_NAME>.ADMIN;
```

## Access Roles vs Functional Roles

For larger projects, Snowflake recommends separating **access roles** (object permissions) from **functional roles** (business functions):

```sql
-- Access roles: granular object permissions
DEFINE DATABASE ROLE <DB>.DATA_READ
COMMENT = 'Read access to all data';

DEFINE DATABASE ROLE <DB>.DATA_WRITE
COMMENT = 'Write access to staging tables';

DEFINE DATABASE ROLE <DB>.SCHEMA_DDL
COMMENT = 'DDL privileges on schemas';

-- Functional roles: business functions that combine access roles
DEFINE DATABASE ROLE <DB>.ANALYST
COMMENT = 'Analyst functional role';

DEFINE DATABASE ROLE <DB>.DEVELOPER
COMMENT = 'Developer functional role';

DEFINE DATABASE ROLE <DB>.ADMIN
COMMENT = 'Admin functional role';

-- Grant access roles to functional roles
GRANT DATABASE ROLE <DB>.DATA_READ TO DATABASE ROLE <DB>.ANALYST;
GRANT DATABASE ROLE <DB>.DATA_READ TO DATABASE ROLE <DB>.DEVELOPER;
GRANT DATABASE ROLE <DB>.DATA_WRITE TO DATABASE ROLE <DB>.DEVELOPER;
GRANT DATABASE ROLE <DB>.DATA_READ TO DATABASE ROLE <DB>.ADMIN;
GRANT DATABASE ROLE <DB>.DATA_WRITE TO DATABASE ROLE <DB>.ADMIN;
GRANT DATABASE ROLE <DB>.SCHEMA_DDL TO DATABASE ROLE <DB>.ADMIN;

-- Roll up to SYSADMIN
GRANT DATABASE ROLE <DB>.ADMIN TO ROLE SYSADMIN;
```

**When to use this pattern:**
- Multiple functional roles need overlapping permissions
- Fine-grained audit of which permissions each role has
- Easier to modify access without touching functional role assignments

**For simpler projects**, the three-tier pattern (ADMIN/DEVELOPER/ANALYST with inheritance) is sufficient.

## Handling Unsupported Grants

Some grants are not supported in DCM. Create a separate `post_deployment_grants.sql` script:

```sql
-- Run manually after DCM deployment
USE ROLE ACCOUNTADMIN;

GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <ROLE_NAME>;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE <ROLE_NAME>;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE <ROLE_NAME>;
```

**⚠️ CHECKPOINT**: Confirm post-deployment grants are documented and acceptable.

For the full list of unsupported patterns, see [Roles and Grants Primitive](../reference/primitives/roles_and_grants.md).

## Common Errors and Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `Unsupported grant type` | Account-level or bulk grants | Move to post_deployment_grants.sql |
| `Warehouse does not belong to database` | Database role + warehouse grant | Use account role for warehouse |
| `Object does not exist` | Grant references undefined object | Ensure object is defined before grant |

## Best Practices

1. **Prefer database roles** - Scoped to database lifecycle, cleaner for DCM
2. **Use role inheritance** - Minimize grant duplication
3. **Explicit over implicit** - Use specific privileges (SELECT, INSERT) instead of ALL
4. **No future grants** - DCM manages objects declaratively; add grants when objects are created
5. **Separate warehouse access** - Use a single account role for all warehouse grants
6. **Document unsupported grants** - Keep a post_deployment_grants.sql for manual application
7. **Use "WAREHOUSE" in account role name** - Name the warehouse access role with "WAREHOUSE" (e.g., `<PROJECT>_WAREHOUSE_USER`)

## Pre-Completion Checklist

Before finishing role/grant definitions, verify:

- [ ] Account role for warehouse access includes "WAREHOUSE" in name (`DEFINE ROLE <name>_WAREHOUSE_USER` or similar)
- [ ] Warehouse grants go to account role, NOT database roles
- [ ] Database roles use proper hierarchy (ANALYST → DEVELOPER → ADMIN)
- [ ] All object grants use fully qualified names

## Related Documentation

- [Roles and Grants Primitive](../reference/primitives/roles_and_grants.md) - Complete grant and role syntax
- [Main DCM Skill](../SKILL.md) - Parent skill for all DCM operations
- [Snowflake Database Roles](https://docs.snowflake.com/en/user-guide/security-access-control-overview#database-roles)

## Verifying Grants After Deployment

After running `snow dcm deploy`, verify grants were applied correctly:

```sql
-- View all grants on a database role
SHOW GRANTS TO DATABASE ROLE <DATABASE_NAME>.ANALYST;

-- View all grants on an account role
SHOW GRANTS TO ROLE <PROJECT>_WAREHOUSE_USER;

-- View grants on a specific schema
SHOW GRANTS ON SCHEMA <DATABASE_NAME>.RAW;

-- View grants on a specific table
SHOW GRANTS ON TABLE <DATABASE_NAME>.RAW.<TABLE_NAME>;

-- Check role hierarchy (who has this role)
SHOW GRANTS OF DATABASE ROLE <DATABASE_NAME>.ADMIN;
SHOW GRANTS OF ROLE <PROJECT>_WAREHOUSE_USER;
```

For ongoing monitoring, query the `SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES` view:

```sql
-- All grants to your project roles
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE GRANTEE_NAME LIKE '%<PROJECT>%'
ORDER BY CREATED_ON DESC;
```

## Output

This skill helps produce:

1. **access.sql** - DCM definition file with roles and grants
2. **post_deployment_grants.sql** - Unsupported grants to run manually after DCM deploy
