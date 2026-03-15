# Databases and Schemas in DCM

## Syntax

```sql
DEFINE DATABASE database_name
    [COMMENT = 'description'];

DEFINE SCHEMA database_name.schema_name
    [WITH MANAGED ACCESS]
    [COMMENT = 'description']
    [DATA_RETENTION_TIME_IN_DAYS = n];
```

## Supported Changes

- `COMMENT` on both databases and schemas
- `DATA_RETENTION_TIME_IN_DAYS` on schemas
- Adding or removing `WITH MANAGED ACCESS` on schemas

## Immutable / Unsupported

- Renaming databases or schemas after creation

## Examples

### Basic Example

```sql
DEFINE DATABASE ANALYTICS_DB
    COMMENT = 'Central analytics platform';

DEFINE SCHEMA ANALYTICS_DB.RAW
    COMMENT = 'Landing zone for ingested data'
    DATA_RETENTION_TIME_IN_DAYS = 14;

DEFINE SCHEMA ANALYTICS_DB.CURATED
    WITH MANAGED ACCESS
    COMMENT = 'Governed data ready for consumption'
    DATA_RETENTION_TIME_IN_DAYS = 90;
```

### With Jinja Templating

```sql
DEFINE DATABASE ANALYTICS_DB{{env_suffix}}
    COMMENT = 'Analytics platform - {{env_suffix}}';

DEFINE SCHEMA ANALYTICS_DB{{env_suffix}}.RAW
    DATA_RETENTION_TIME_IN_DAYS = 14;

DEFINE SCHEMA ANALYTICS_DB{{env_suffix}}.CURATED
    WITH MANAGED ACCESS
    DATA_RETENTION_TIME_IN_DAYS = 90;
```

### Combined Pattern: Database + Schemas + Warehouse Foundation

```sql
DEFINE DATABASE SALES_DB{{env_suffix}}
    COMMENT = 'Sales domain data';

DEFINE SCHEMA SALES_DB{{env_suffix}}.RAW;

DEFINE SCHEMA SALES_DB{{env_suffix}}.ANALYTICS
    WITH MANAGED ACCESS
    DATA_RETENTION_TIME_IN_DAYS = 30;

DEFINE SCHEMA SALES_DB{{env_suffix}}.SERVE
    WITH MANAGED ACCESS;

DEFINE WAREHOUSE SALES_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;
```
