# Dynamic Tables in DCM

## Syntax

```sql
DEFINE DYNAMIC TABLE database_name.schema_name.table_name
WAREHOUSE = warehouse_name
TARGET_LAG = 'lag_value'
[INITIALIZE = 'ON_SCHEDULE' | 'ON_CREATE']
[REFRESH_MODE = 'AUTO' | 'FULL' | 'INCREMENTAL']
[DATA_METRIC_SCHEDULE = 'schedule_expression']
AS
SELECT ...;
```

### Required Properties

| Property | Values | Description |
|----------|--------|-------------|
| `WAREHOUSE` | Warehouse name | Compute used for refreshes |
| `TARGET_LAG` | `'DOWNSTREAM'` or a time interval (`'1 hour'`, `'30 minutes'`) | `DOWNSTREAM` refreshes only when a downstream consumer needs fresh data. Time intervals refresh on a fixed cadence. |

### Optional Properties

| Property | Values | Description |
|----------|--------|-------------|
| `INITIALIZE` | `'ON_SCHEDULE'` (default), `'ON_CREATE'` | `ON_CREATE` populates immediately at deploy time. `ON_SCHEDULE` defers until the first scheduled refresh. |
| `REFRESH_MODE` | `'AUTO'`, `'FULL'`, `'INCREMENTAL'` | Controls how Snowflake refreshes the table. `AUTO` lets Snowflake decide. |
| `DATA_METRIC_SCHEDULE` | Schedule expression | When to evaluate attached Data Metric Functions |

## Supported Changes

**Without full refresh:**
- `WAREHOUSE`
- `TARGET_LAG`

**Triggers a full refresh:**
- `REFRESH_MODE`
- Body changes (modifying the `AS SELECT ...`), including adding or dropping columns

## Immutable / Unsupported

- `INITIALIZE` cannot be changed after creation
- Column reordering is not tracked
- Renaming dynamic tables

## Examples

### Basic Example

```sql
DEFINE DYNAMIC TABLE SALES_DB.ANALYTICS.DT_ORDER_SUMMARY
WAREHOUSE = 'ANALYTICS_WH'
TARGET_LAG = '1 hour'
INITIALIZE = 'ON_CREATE'
AS
SELECT
    DATE_TRUNC('DAY', o.ORDER_DATE) AS ORDER_DAY,
    p.CATEGORY,
    COUNT(DISTINCT o.ORDER_ID) AS ORDER_COUNT,
    SUM(o.TOTAL_AMOUNT) AS REVENUE
FROM SALES_DB.RAW.ORDERS o
JOIN SALES_DB.RAW.PRODUCTS p ON o.PRODUCT_ID = p.PRODUCT_ID
GROUP BY ORDER_DAY, p.CATEGORY;
```

### With Jinja Templating

```sql
DEFINE DYNAMIC TABLE SALES_DB{{env_suffix}}.ANALYTICS.DT_ORDER_SUMMARY
WAREHOUSE = 'ANALYTICS_WH{{env_suffix}}'
TARGET_LAG = '{{dt_lag}}'
INITIALIZE = 'ON_CREATE'
AS
SELECT
    DATE_TRUNC('DAY', o.ORDER_DATE) AS ORDER_DAY,
    p.CATEGORY,
    COUNT(DISTINCT o.ORDER_ID) AS ORDER_COUNT,
    SUM(o.TOTAL_AMOUNT) AS REVENUE
FROM SALES_DB{{env_suffix}}.RAW.ORDERS o
JOIN SALES_DB{{env_suffix}}.RAW.PRODUCTS p ON o.PRODUCT_ID = p.PRODUCT_ID
GROUP BY ORDER_DAY, p.CATEGORY;
```

### Combined Pattern: Source Table + Dynamic Table Pipeline

The source table must have `CHANGE_TRACKING = TRUE` for dynamic tables to detect changes incrementally.

```sql
DEFINE TABLE SALES_DB.RAW.TRANSACTIONS (
    TXN_ID NUMBER NOT NULL,
    CUSTOMER_ID NUMBER NOT NULL,
    PRODUCT_ID NUMBER,
    QUANTITY NUMBER,
    UNIT_PRICE NUMBER(10,2),
    TXN_TIMESTAMP TIMESTAMP_NTZ
)
CHANGE_TRACKING = TRUE
COMMENT = 'Raw point-of-sale transactions';

DEFINE DYNAMIC TABLE SALES_DB.ANALYTICS.DT_CUSTOMER_SPEND
WAREHOUSE = 'ANALYTICS_WH'
TARGET_LAG = 'DOWNSTREAM'
INITIALIZE = 'ON_SCHEDULE'
AS
SELECT
    CUSTOMER_ID,
    COUNT(*) AS TXN_COUNT,
    SUM(QUANTITY * UNIT_PRICE) AS TOTAL_SPEND,
    MIN(TXN_TIMESTAMP) AS FIRST_TXN,
    MAX(TXN_TIMESTAMP) AS LAST_TXN
FROM SALES_DB.RAW.TRANSACTIONS
GROUP BY CUSTOMER_ID;

DEFINE DYNAMIC TABLE SALES_DB.SERVE.DT_TOP_CUSTOMERS
WAREHOUSE = 'ANALYTICS_WH'
TARGET_LAG = '30 minutes'
INITIALIZE = 'ON_SCHEDULE'
AS
SELECT
    CUSTOMER_ID,
    TOTAL_SPEND,
    TXN_COUNT,
    LAST_TXN
FROM SALES_DB.ANALYTICS.DT_CUSTOMER_SPEND
WHERE TOTAL_SPEND > 1000
ORDER BY TOTAL_SPEND DESC;
```
