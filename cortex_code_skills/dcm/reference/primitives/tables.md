# Tables in DCM

## Syntax

```sql
DEFINE TABLE database_name.schema_name.table_name (
    column_name DATA_TYPE [DEFAULT expression] [NOT NULL],
    ...
)
[CHANGE_TRACKING = TRUE | FALSE]
[DATA_METRIC_SCHEDULE = 'schedule_expression']
[COMMENT = 'description']
[DATA_RETENTION_TIME_IN_DAYS = n];
```

## Supported Changes

- Adding new columns
- Widening column types to compatible types (e.g. `VARCHAR(50)` to `VARCHAR(100)`, `NUMBER(10,2)` to `NUMBER(15,2)`)
- Dropping columns
- Changing `CHANGE_TRACKING`, `DATA_METRIC_SCHEDULE`, `COMMENT`, `DATA_RETENTION_TIME_IN_DAYS`

## Immutable / Unsupported

- Renaming tables
- Renaming columns
- Reordering columns
- Changing column types to incompatible types (e.g. `VARCHAR` to `NUMBER`)

## Examples

### Basic Example

```sql
DEFINE TABLE SALES_DB.RAW.ORDERS (
    ORDER_ID NUMBER NOT NULL,
    CUSTOMER_ID NUMBER NOT NULL,
    ORDER_DATE DATE,
    TOTAL_AMOUNT NUMBER(12,2),
    STATUS VARCHAR(20) DEFAULT 'PENDING',
    IS_FULFILLED BOOLEAN DEFAULT FALSE,
    SHIPPED_AT TIMESTAMP_NTZ,
    LINE_ITEMS ARRAY
)
CHANGE_TRACKING = TRUE
COMMENT = 'Raw order records from POS system'
DATA_RETENTION_TIME_IN_DAYS = 30;
```

### With Jinja Templating

```sql
DEFINE TABLE SALES_DB{{env_suffix}}.RAW.ORDERS (
    ORDER_ID NUMBER NOT NULL,
    CUSTOMER_ID NUMBER NOT NULL,
    ORDER_DATE DATE,
    TOTAL_AMOUNT NUMBER(12,2),
    STATUS VARCHAR(20) DEFAULT 'PENDING',
    IS_FULFILLED BOOLEAN DEFAULT FALSE,
    SHIPPED_AT TIMESTAMP_NTZ,
    LINE_ITEMS ARRAY
)
CHANGE_TRACKING = TRUE
DATA_RETENTION_TIME_IN_DAYS = {{retention_days}};
```

### Combined Pattern: Source Table + View Consumption Layer

```sql
DEFINE TABLE SALES_DB.RAW.PRODUCTS (
    PRODUCT_ID NUMBER NOT NULL,
    PRODUCT_NAME VARCHAR(200),
    CATEGORY VARCHAR(100),
    UNIT_PRICE NUMBER(10,2),
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE
COMMENT = 'Product catalog from ERP';

DEFINE VIEW SALES_DB.SERVE.V_ACTIVE_PRODUCTS
AS
SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    UNIT_PRICE
FROM SALES_DB.RAW.PRODUCTS
WHERE IS_ACTIVE = TRUE;
```
