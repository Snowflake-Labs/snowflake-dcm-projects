# Views in DCM

## Syntax

```sql
DEFINE VIEW database_name.schema_name.view_name
[DATA_METRIC_SCHEDULE = 'USING CRON <expr> <timezone>']
AS
SELECT ...;
```

`DATA_METRIC_SCHEDULE` on views only supports cron schedules. `TRIGGER_ON_CHANGES` is not supported because views do not have underlying storage to track changes against.

## Supported Changes

- Modifying the view body (`AS SELECT ...`)
- Adding or removing columns in the SELECT list
- Changing `DATA_METRIC_SCHEDULE`

## Immutable / Unsupported

- Renaming views
- Column reordering is not tracked; column identity is determined by position

## Examples

### Basic Example

```sql
DEFINE VIEW SALES_DB.SERVE.V_DAILY_REVENUE
AS
SELECT
    DATE_TRUNC('DAY', ORDER_DATE) AS SALE_DATE,
    COUNT(DISTINCT ORDER_ID) AS TOTAL_ORDERS,
    SUM(TOTAL_AMOUNT) AS DAILY_REVENUE
FROM SALES_DB.RAW.ORDERS
WHERE STATUS != 'CANCELLED'
GROUP BY SALE_DATE;
```

### With Jinja Templating

```sql
DEFINE VIEW SALES_DB{{env_suffix}}.SERVE.V_DAILY_REVENUE
DATA_METRIC_SCHEDULE = 'USING CRON 0 7 * * * UTC'
AS
SELECT
    DATE_TRUNC('DAY', ORDER_DATE) AS SALE_DATE,
    COUNT(DISTINCT ORDER_ID) AS TOTAL_ORDERS,
    SUM(TOTAL_AMOUNT) AS DAILY_REVENUE
FROM SALES_DB{{env_suffix}}.RAW.ORDERS
WHERE STATUS != 'CANCELLED'
GROUP BY SALE_DATE;
```

### Combined Pattern: Table + View Consumption Layer

```sql
DEFINE TABLE SALES_DB.RAW.CUSTOMERS (
    CUSTOMER_ID NUMBER NOT NULL,
    FULL_NAME VARCHAR(200),
    EMAIL VARCHAR(300),
    REGION VARCHAR(50),
    SIGNUP_DATE DATE
)
CHANGE_TRACKING = TRUE;

DEFINE VIEW SALES_DB.SERVE.V_CUSTOMERS_BY_REGION
AS
SELECT
    REGION,
    COUNT(*) AS CUSTOMER_COUNT,
    MIN(SIGNUP_DATE) AS EARLIEST_SIGNUP,
    MAX(SIGNUP_DATE) AS LATEST_SIGNUP
FROM SALES_DB.RAW.CUSTOMERS
GROUP BY REGION;
```
