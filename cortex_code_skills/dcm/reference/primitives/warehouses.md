# Warehouses in DCM

## Syntax

```sql
DEFINE WAREHOUSE warehouse_name
WITH
    WAREHOUSE_SIZE = 'size'
    [AUTO_SUSPEND = seconds]
    [AUTO_RESUME = TRUE | FALSE]
    [INITIALLY_SUSPENDED = TRUE | FALSE]
    [COMMENT = 'description'];
```

Warehouse properties are specified inside a `WITH` clause, not as standalone clauses.

## Properties

| Property | Values | Description |
|----------|--------|-------------|
| `WAREHOUSE_SIZE` | `'XSMALL'`, `'SMALL'`, `'MEDIUM'`, `'LARGE'`, `'XLARGE'`, `'2XLARGE'`, `'3XLARGE'`, `'4XLARGE'` | Compute capacity |
| `AUTO_SUSPEND` | Seconds (e.g., `300`) | Idle time before automatic suspension |
| `AUTO_RESUME` | `TRUE` / `FALSE` | Whether the warehouse resumes automatically on query |
| `INITIALLY_SUSPENDED` | `TRUE` / `FALSE` | Whether the warehouse starts suspended on creation |
| `COMMENT` | String | Warehouse description |

## Supported Changes

- `WAREHOUSE_SIZE`
- `AUTO_SUSPEND`
- `AUTO_RESUME`
- `COMMENT`

## Immutable

- `INITIALLY_SUSPENDED` only applies at creation time and cannot be altered afterward.

## Examples

### Basic Example

```sql
DEFINE WAREHOUSE ANALYTICS_WH
WITH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = 'General-purpose analytics warehouse';
```

### With Jinja Templating

```sql
DEFINE WAREHOUSE ETL_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = 'ETL warehouse for {{env_suffix}} environment';
```

### Combined Pattern: Warehouse + Account Role for Access

Database roles cannot be granted warehouse privileges because warehouses are account-level objects. Use an account role to bridge access.

```sql
DEFINE WAREHOUSE PROJECT_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

DEFINE ROLE PROJECT_WH_USER{{env_suffix}}
    COMMENT = 'Grants warehouse access to project users';

GRANT USAGE ON WAREHOUSE PROJECT_WH{{env_suffix}} TO ROLE PROJECT_WH_USER{{env_suffix}};

{% for user_name in users %}
GRANT ROLE PROJECT_WH_USER{{env_suffix}} TO USER {{user_name}};
{% endfor %}
```
