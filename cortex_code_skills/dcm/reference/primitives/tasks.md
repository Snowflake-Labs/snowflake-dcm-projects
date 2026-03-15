# Tasks in DCM

## Syntax

```sql
DEFINE TASK database_name.schema_name.task_name
    [WAREHOUSE = 'warehouse_name']
    [USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'size']
    [SCHEDULE = 'schedule_expression']
    [AFTER database_name.schema_name.predecessor_task [, ...]]
    [WHEN condition]
    [COMMENT = 'description']
AS
    task_body;
```

## Properties

| Property | Description |
|----------|-------------|
| `WAREHOUSE` | Named warehouse for task execution. Mutually exclusive with serverless sizing. |
| `SCHEDULE` | When the task runs. Required on root tasks; omit on child tasks that use `AFTER`. |
| `USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE` | Serverless compute size (`'XSMALL'` through `'4XLARGE'`). Mutually exclusive with `WAREHOUSE`. |
| `WHEN` | Boolean condition evaluated before each run. Task is skipped when condition returns FALSE. |
| `AFTER` | Predecessor task(s) for DAG scheduling. Accepts comma-separated fully qualified task names. |
| `COMMENT` | Task description. |

### Schedule Expressions

**Cron format:**

```
'USING CRON <minute> <hour> <day-of-month> <month> <day-of-week> <timezone>'
```

| Expression | Description |
|------------|-------------|
| `'USING CRON 0 4 * * * UTC'` | Daily at 4:00 AM UTC |
| `'USING CRON 0 */2 * * * America/New_York'` | Every 2 hours ET |
| `'USING CRON 0 0 * * 1 UTC'` | Weekly on Monday at midnight |

**Interval format:**

```sql
SCHEDULE = '60 MINUTE'
SCHEDULE = '5 MINUTE'
```

### Task Body Options

The body after `AS` accepts: a single SQL statement, a `BEGIN...END` block with multiple statements, or a stored procedure call (`CALL database_name.schema_name.my_procedure()`).

## Deployment Behavior

DCM handles task state transitions automatically during deployment:

1. Suspends the root task (and its entire task graph)
2. Applies all definition changes
3. Resumes the root task after deployment completes

No manual suspend/resume is required. This applies regardless of whether the task is currently active or suspended.

## Supported Changes

- `WAREHOUSE` or `USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE`
- `SCHEDULE` expression
- `WHEN` condition
- `COMMENT`
- Task body (the SQL after `AS`)

## Limitations

- Maximum task DAG depth: 1000 tasks
- Maximum predecessors per task: 100
- Root tasks require `SCHEDULE`; child tasks with `AFTER` must not have `SCHEDULE`

## Examples

### Basic Example

```sql
DEFINE TASK ETL_DB.PIPELINE.TSK_DAILY_ORDER_INGEST
WAREHOUSE = 'ETL_WH'
SCHEDULE = 'USING CRON 0 4 * * * UTC'
COMMENT = 'Ingests orders from external source daily at 4 AM UTC'
AS
BEGIN
    INSERT INTO ETL_DB.RAW.ORDERS
    SELECT *
    FROM ETL_DB.STAGING.EXTERNAL_ORDERS
    WHERE ORDER_DATE = CURRENT_DATE - INTERVAL '1 DAY';
END;
```

### Serverless Task with WHEN Condition

```sql
DEFINE TASK ETL_DB.PIPELINE.TSK_PROCESS_ORDER_STREAM
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('ETL_DB.RAW.ORDER_STREAM')
COMMENT = 'Merges new order stream data into summary table'
AS
BEGIN
    MERGE INTO ETL_DB.ANALYTICS.ORDERS_SUMMARY target
    USING ETL_DB.RAW.ORDER_STREAM source
    ON target.ORDER_ID = source.ORDER_ID
    WHEN MATCHED THEN UPDATE SET
        target.STATUS = source.STATUS,
        target.UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (ORDER_ID, STATUS, UPDATED_AT)
        VALUES (source.ORDER_ID, source.STATUS, CURRENT_TIMESTAMP());
END;
```

### With Jinja Templating

```sql
DEFINE TASK ETL_DB{{env_suffix}}.PIPELINE.TSK_REFRESH_INVENTORY
WAREHOUSE = 'ETL_WH{{env_suffix}}'
SCHEDULE = 'USING CRON 0 */{{refresh_interval_hours}} * * * UTC'
COMMENT = 'Refreshes inventory snapshot'
AS
BEGIN
    TRUNCATE TABLE ETL_DB{{env_suffix}}.ANALYTICS.INVENTORY_SNAPSHOT;
    INSERT INTO ETL_DB{{env_suffix}}.ANALYTICS.INVENTORY_SNAPSHOT
    SELECT * FROM ETL_DB{{env_suffix}}.RAW.INVENTORY;
END;
```

### Combined Pattern: Task DAG with Root + Children + Finalizer

```sql
DEFINE TASK ETL_DB.PIPELINE.TSK_NIGHTLY_ROOT
WAREHOUSE = 'ETL_WH'
SCHEDULE = 'USING CRON 0 2 * * * UTC'
COMMENT = 'Root task: initializes nightly ETL run'
AS
CALL ETL_DB.PIPELINE.SP_INIT_NIGHTLY_LOAD();

DEFINE TASK ETL_DB.PIPELINE.TSK_STAGE_ORDERS
WAREHOUSE = 'ETL_WH'
AFTER ETL_DB.PIPELINE.TSK_NIGHTLY_ROOT
COMMENT = 'Stages new orders into analytics layer'
AS
BEGIN
    INSERT INTO ETL_DB.STAGING.ORDERS
    SELECT * FROM ETL_DB.RAW.ORDERS
    WHERE LOADED_AT >= CURRENT_DATE - INTERVAL '1 DAY';
END;

DEFINE TASK ETL_DB.PIPELINE.TSK_STAGE_CUSTOMERS
WAREHOUSE = 'ETL_WH'
AFTER ETL_DB.PIPELINE.TSK_NIGHTLY_ROOT
AS
BEGIN
    INSERT INTO ETL_DB.STAGING.CUSTOMERS
    SELECT * FROM ETL_DB.RAW.CUSTOMERS
    WHERE LOADED_AT >= CURRENT_DATE - INTERVAL '1 DAY';
END;

DEFINE TASK ETL_DB.PIPELINE.TSK_BUILD_DAILY_SUMMARY
WAREHOUSE = 'ETL_WH'
AFTER ETL_DB.PIPELINE.TSK_STAGE_ORDERS, ETL_DB.PIPELINE.TSK_STAGE_CUSTOMERS
COMMENT = 'Runs after both staging tasks complete'
AS
BEGIN
    INSERT INTO ETL_DB.ANALYTICS.DAILY_SUMMARY
    SELECT CURRENT_DATE - 1, COUNT(*), SUM(o.AMOUNT)
    FROM ETL_DB.STAGING.ORDERS o
    JOIN ETL_DB.STAGING.CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID;
END;
```
