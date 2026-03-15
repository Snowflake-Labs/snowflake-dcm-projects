# Data Quality in DCM

## Custom Data Metric Function Syntax

Custom DMFs are named objects and use the `DEFINE` keyword:

```sql
DEFINE DATA METRIC FUNCTION database_name.schema_name.function_name(
    TABLE_NAME TABLE(
        COLUMN_VALUE data_type
    )
)
RETURNS return_type
AS
$$
    SELECT expression
    FROM TABLE_NAME
    WHERE conditions
$$
;
```

The parameter signature `TABLE_NAME TABLE(COLUMN_VALUE data_type)` is required. `TABLE_NAME` references the table being evaluated; `COLUMN_VALUE` references the column(s) being measured.

## Attach Data Metric Function Syntax

Attaching DMFs to objects uses imperative syntax, NOT `DEFINE`:

```sql
ATTACH DATA METRIC FUNCTION dmf_reference
    TO object_type database_name.schema_name.object_name
    ON (column_name)
    EXPECTATION expectation_name (condition);
```

- `dmf_reference` -- system DMF (`SNOWFLAKE.CORE.*`) or custom DMF (fully qualified)
- `object_type` -- `TABLE`, `DYNAMIC TABLE`, or `VIEW`
- `ON (column_name)` -- column(s) to evaluate
- `EXPECTATION` -- named condition defining pass/fail criteria; `value` refers to the DMF result

## System DMFs

Built-in DMFs available in `SNOWFLAKE.CORE`:

| Category | DMF | Description |
|----------|-----|-------------|
| Accuracy | `NULL_COUNT` | Count of NULL values in a column |
| Accuracy | `NULL_PERCENT` | Percentage of NULL values |
| Accuracy | `BLANK_COUNT` | Count of blank values |
| Accuracy | `BLANK_PERCENT` | Percentage of blank values |
| Freshness | `FRESHNESS` | Data freshness based on timestamp column |
| Statistics | `AVG` | Average value of a column |
| Statistics | `MAX` | Maximum value |
| Statistics | `MIN` | Minimum value |
| Statistics | `STDDEV` | Standard deviation |
| Uniqueness | `DUPLICATE_COUNT` | Number of duplicate values |
| Uniqueness | `UNIQUE_COUNT` | Number of unique non-NULL values |
| Uniqueness | `ACCEPTED_VALUES` | Whether values match a Boolean expression |
| Volume | `ROW_COUNT` | Total rows in table/view |

## DATA_METRIC_SCHEDULE

Objects with attached DMFs must have `DATA_METRIC_SCHEDULE` set on the object definition. Without it, expectations never run.

### Schedule Options

| Type | Syntax | Applies to |
|------|--------|------------|
| Trigger on changes | `'TRIGGER_ON_CHANGES'` | Tables, dynamic tables |
| Cron schedule | `'USING CRON <minute> <hour> <dom> <month> <dow> <tz>'` | Tables, views, dynamic tables |
| Interval | `'<N> MINUTE'` or `'<N> HOUR'` | Tables, views, dynamic tables |

### Schedule Best Practices

| Data pattern | Recommended schedule | Rationale |
|-------------|---------------------|-----------|
| Streaming / CDC tables | `'TRIGGER_ON_CHANGES'` | Evaluate as data arrives |
| Batch ETL tables | Cron after ETL window | Avoid evaluating during load |
| Reporting views | Cron before business hours | Ensure quality before users access dashboards |
| Dynamic tables with `DOWNSTREAM` lag | `'TRIGGER_ON_CHANGES'` | Evaluate after each refresh |

## Examples

### Complete Example: Table with Custom and System DMFs

```sql
DEFINE TABLE FINANCE_DB.RAW.TRANSACTIONS (
    TXN_ID NUMBER NOT NULL,
    AMOUNT NUMBER(15,2),
    STATUS VARCHAR(20),
    PROCESSED_AT TIMESTAMP_NTZ
)
CHANGE_TRACKING = TRUE
DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES'
;

DEFINE DATA METRIC FUNCTION FINANCE_DB.RAW.AMOUNT_SPREAD(
    TABLE_NAME TABLE(COLUMN_VALUE NUMBER)
)
RETURNS NUMBER
AS
$$
    SELECT MAX(COLUMN_VALUE) - MIN(COLUMN_VALUE)
    FROM TABLE_NAME
    WHERE COLUMN_VALUE IS NOT NULL
$$
;

ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
    TO TABLE FINANCE_DB.RAW.TRANSACTIONS
    ON (AMOUNT)
    EXPECTATION NO_NULL_AMOUNTS (value = 0);

ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN
    TO TABLE FINANCE_DB.RAW.TRANSACTIONS
    ON (AMOUNT)
    EXPECTATION AMOUNTS_POSITIVE (value >= 0);

ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT
    TO TABLE FINANCE_DB.RAW.TRANSACTIONS
    ON (TXN_ID)
    EXPECTATION HAS_DATA (value > 0);

ATTACH DATA METRIC FUNCTION FINANCE_DB.RAW.AMOUNT_SPREAD
    TO TABLE FINANCE_DB.RAW.TRANSACTIONS
    ON (AMOUNT)
    EXPECTATION REASONABLE_SPREAD (value < 1000000);
```

### Dynamic Table with Expectations

```sql
DEFINE DYNAMIC TABLE FINANCE_DB.ANALYTICS.DT_DAILY_TOTALS
WAREHOUSE = FINANCE_WH
TARGET_LAG = 'DOWNSTREAM'
INITIALIZE = 'ON_SCHEDULE'
DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES'
AS
SELECT
    DATE_TRUNC('DAY', PROCESSED_AT) AS TXN_DATE,
    COUNT(*) AS TXN_COUNT,
    SUM(AMOUNT) AS TOTAL_AMOUNT
FROM FINANCE_DB.RAW.TRANSACTIONS
WHERE STATUS = 'COMPLETED'
GROUP BY TXN_DATE;

ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
    TO DYNAMIC TABLE FINANCE_DB.ANALYTICS.DT_DAILY_TOTALS
    ON (TXN_DATE)
    EXPECTATION NO_NULL_DATES (value = 0);
```

### View with Cron-Based Expectations

```sql
DEFINE VIEW FINANCE_DB.SERVE.V_REVENUE_SUMMARY
DATA_METRIC_SCHEDULE = 'USING CRON 0 7 * * * UTC'
AS
SELECT
    TXN_DATE,
    TOTAL_AMOUNT AS DAILY_REVENUE,
    TXN_COUNT AS DAILY_TRANSACTIONS
FROM FINANCE_DB.ANALYTICS.DT_DAILY_TOTALS
WHERE TXN_DATE >= DATEADD('DAY', -30, CURRENT_DATE());

ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN
    TO VIEW FINANCE_DB.SERVE.V_REVENUE_SUMMARY
    ON (DAILY_REVENUE)
    EXPECTATION REVENUE_NOT_NEGATIVE (value >= 0);
```
