# Roles and Grants in DCM

## Role Syntax

### Account Roles

```sql
DEFINE ROLE role_name
[COMMENT = 'description'];
```

### Database Roles

```sql
DEFINE DATABASE ROLE database_name.role_name
[COMMENT = 'description'];
```

### Account Roles vs Database Roles

| Aspect | Account Role | Database Role |
|--------|-------------|---------------|
| Scope | Account-wide | Single database |
| Syntax | `DEFINE ROLE role_name` | `DEFINE DATABASE ROLE db.role_name` |
| Best for | Warehouse access, cross-database access | Database-scoped permissions (preferred for most cases) |

## Grant Syntax

Grants use standard SQL syntax. They are imperative statements, NOT `DEFINE` declarations.

```sql
GRANT privilege ON object_type object_name TO ROLE role_name;
GRANT privilege ON object_type object_name TO DATABASE ROLE database_name.role_name;
```

### Supported Grant Patterns

```sql
GRANT USAGE ON DATABASE database_name TO ROLE role_name;
GRANT USAGE ON SCHEMA database_name.schema_name TO ROLE role_name;
GRANT USAGE ON WAREHOUSE warehouse_name TO ROLE role_name;

GRANT SELECT ON ALL TABLES IN DATABASE database_name TO ROLE role_name;
GRANT SELECT ON ALL TABLES IN SCHEMA database_name.schema_name TO ROLE role_name;
GRANT SELECT ON ALL VIEWS IN DATABASE database_name TO ROLE role_name;
GRANT SELECT ON ALL DYNAMIC TABLES IN DATABASE database_name TO ROLE role_name;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA database_name.schema_name TO ROLE role_name;

GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE
    ON SCHEMA database_name.schema_name TO ROLE role_name;

GRANT OWNERSHIP ON SCHEMA database_name.schema_name TO ROLE role_name;

GRANT ROLE child_role TO ROLE parent_role;
GRANT DATABASE ROLE database_name.child_role TO DATABASE ROLE database_name.parent_role;
GRANT ROLE role_name TO USER user_name;
```

### Database Roles Cannot Have Warehouse Privileges

Warehouses are account-level objects. Database roles are scoped to a single database. Snowflake does not allow granting warehouse privileges to database roles.

Use a separate account role for warehouse access:

```sql
DEFINE ROLE PROJECT_WAREHOUSE_USER
COMMENT = 'Warehouse access for project users';

GRANT USAGE ON WAREHOUSE PROJECT_WH TO ROLE PROJECT_WAREHOUSE_USER;
```

### Unsupported Grant Patterns

| Pattern | Workaround |
|---------|------------|
| `GRANT ... ON ACCOUNT` | Document in `post_deployment_grants.sql` for manual application |
| `GRANT IMPORTED PRIVILEGES` | Document in `post_deployment_grants.sql` for manual application |
| `GRANT ALL ON ALL SCHEMAS IN DATABASE` | Grant to each schema explicitly |
| Warehouse grants to database roles | Use account role for warehouse access |

## Examples

### Basic: Account Role with Grants

```sql
DEFINE ROLE SALES_PROJECT_READER
COMMENT = 'Read-only access to sales data';

GRANT USAGE ON DATABASE SALES_DB TO ROLE SALES_PROJECT_READER;
GRANT USAGE ON SCHEMA SALES_DB.RAW TO ROLE SALES_PROJECT_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA SALES_DB.RAW TO ROLE SALES_PROJECT_READER;
```

### Three-Tier Pattern: Database Roles with Inheritance

```sql
DEFINE DATABASE ROLE SALES_DB.ADMIN
COMMENT = 'Full control: DDL + DML';

DEFINE DATABASE ROLE SALES_DB.DEVELOPER
COMMENT = 'Read/write: DML only';

DEFINE DATABASE ROLE SALES_DB.ANALYST
COMMENT = 'Read-only: SELECT only';

DEFINE ROLE SALES_WAREHOUSE_USER
COMMENT = 'Warehouse access for sales project users';

-- Role hierarchy: ANALYST -> DEVELOPER -> ADMIN
GRANT DATABASE ROLE SALES_DB.ANALYST TO DATABASE ROLE SALES_DB.DEVELOPER;
GRANT DATABASE ROLE SALES_DB.DEVELOPER TO DATABASE ROLE SALES_DB.ADMIN;
GRANT DATABASE ROLE SALES_DB.ADMIN TO ROLE SYSADMIN;
GRANT ROLE SALES_WAREHOUSE_USER TO ROLE SYSADMIN;

-- Warehouse (account role only)
GRANT USAGE ON WAREHOUSE SALES_WH TO ROLE SALES_WAREHOUSE_USER;

-- Database access
GRANT USAGE ON DATABASE SALES_DB TO DATABASE ROLE SALES_DB.ANALYST;

-- Schema access
GRANT USAGE ON SCHEMA SALES_DB.RAW TO DATABASE ROLE SALES_DB.ANALYST;
GRANT USAGE ON SCHEMA SALES_DB.ANALYTICS TO DATABASE ROLE SALES_DB.ANALYST;

-- Analyst: read-only
GRANT SELECT ON ALL TABLES IN SCHEMA SALES_DB.RAW TO DATABASE ROLE SALES_DB.ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA SALES_DB.ANALYTICS TO DATABASE ROLE SALES_DB.ANALYST;

-- Developer: write access (inherits SELECT from ANALYST)
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SALES_DB.RAW TO DATABASE ROLE SALES_DB.DEVELOPER;

-- Admin: DDL
GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE
    ON SCHEMA SALES_DB.RAW TO DATABASE ROLE SALES_DB.ADMIN;
GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE
    ON SCHEMA SALES_DB.ANALYTICS TO DATABASE ROLE SALES_DB.ADMIN;
```

### Access Roles vs Functional Roles (Larger Projects)

Separates granular object permissions (access roles) from business functions (functional roles).

```sql
-- Access roles: granular permissions
DEFINE DATABASE ROLE SALES_DB.DATA_READ
COMMENT = 'Read access to all data objects';

DEFINE DATABASE ROLE SALES_DB.DATA_WRITE
COMMENT = 'Write access to staging tables';

DEFINE DATABASE ROLE SALES_DB.SCHEMA_DDL
COMMENT = 'DDL privileges on project schemas';

-- Functional roles: combine access roles for business functions
DEFINE DATABASE ROLE SALES_DB.ANALYST
COMMENT = 'Analyst functional role';

DEFINE DATABASE ROLE SALES_DB.DEVELOPER
COMMENT = 'Developer functional role';

DEFINE DATABASE ROLE SALES_DB.ADMIN
COMMENT = 'Admin functional role';

-- Compose functional roles from access roles
GRANT DATABASE ROLE SALES_DB.DATA_READ TO DATABASE ROLE SALES_DB.ANALYST;
GRANT DATABASE ROLE SALES_DB.DATA_READ TO DATABASE ROLE SALES_DB.DEVELOPER;
GRANT DATABASE ROLE SALES_DB.DATA_WRITE TO DATABASE ROLE SALES_DB.DEVELOPER;
GRANT DATABASE ROLE SALES_DB.DATA_READ TO DATABASE ROLE SALES_DB.ADMIN;
GRANT DATABASE ROLE SALES_DB.DATA_WRITE TO DATABASE ROLE SALES_DB.ADMIN;
GRANT DATABASE ROLE SALES_DB.SCHEMA_DDL TO DATABASE ROLE SALES_DB.ADMIN;

GRANT DATABASE ROLE SALES_DB.ADMIN TO ROLE SYSADMIN;

-- Access role grants
GRANT USAGE ON DATABASE SALES_DB TO DATABASE ROLE SALES_DB.DATA_READ;
GRANT USAGE ON SCHEMA SALES_DB.RAW TO DATABASE ROLE SALES_DB.DATA_READ;
GRANT USAGE ON SCHEMA SALES_DB.ANALYTICS TO DATABASE ROLE SALES_DB.DATA_READ;
GRANT SELECT ON ALL TABLES IN DATABASE SALES_DB TO DATABASE ROLE SALES_DB.DATA_READ;
GRANT SELECT ON ALL VIEWS IN DATABASE SALES_DB TO DATABASE ROLE SALES_DB.DATA_READ;

GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SALES_DB.RAW TO DATABASE ROLE SALES_DB.DATA_WRITE;

GRANT CREATE TABLE, CREATE VIEW ON SCHEMA SALES_DB.RAW TO DATABASE ROLE SALES_DB.SCHEMA_DDL;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA SALES_DB.ANALYTICS TO DATABASE ROLE SALES_DB.SCHEMA_DDL;
```
