# SQL Functions in DCM

## Syntax

```sql
DEFINE FUNCTION database_name.schema_name.function_name(param_name DATA_TYPE [, ...])
RETURNS return_type
[COMMENT = 'description']
AS
$$
    function_body
$$;
```

## Properties

| Property | Description |
|----------|-------------|
| Parameters | `(param_name DATA_TYPE, ...)` — input parameters with Snowflake data types |
| `RETURNS` | Return data type (e.g., `NUMBER(10,2)`, `VARCHAR`, `BOOLEAN`) |
| `COMMENT` | Function description |
| Function body | SQL expression enclosed in `$$` delimiters |

## Supported Changes

- Function body, `COMMENT`, `RETURNS` type

## Critical Limitation: Dependency Ordering

DCM does not sort SQL functions by dependency. If a view references a function, DCM may create the view first, causing deployment failure. **Workaround:** run `snow dcm deploy` once for functions, then again for dependent objects.

## Examples

### Basic Example

```sql
DEFINE FUNCTION FINANCE_DB.ANALYTICS.CALCULATE_PROFIT_MARGIN(REVENUE NUMBER, COST NUMBER)
RETURNS NUMBER(10,2)
COMMENT = 'Returns profit margin as a percentage'
AS
$$
    CASE WHEN REVENUE = 0 THEN 0
         ELSE ROUND(((REVENUE - COST) / REVENUE) * 100, 2)
    END
$$;
```

### With Multiple Parameters

```sql
DEFINE FUNCTION SALES_DB.UTILS.APPLY_TIERED_DISCOUNT(PRICE NUMBER, QUANTITY NUMBER, TIER VARCHAR)
RETURNS NUMBER(10,2)
AS
$$
    PRICE * QUANTITY * CASE WHEN TIER = 'GOLD' THEN 0.80
                            WHEN TIER = 'SILVER' THEN 0.90
                            ELSE 1.00 END
$$;
```

### With Jinja Templating

```sql
DEFINE FUNCTION ETL_DB{{env_suffix}}.UTILS.CLEAN_PHONE_NUMBER(PHONE_NUMBER VARCHAR)
RETURNS VARCHAR
AS
$$
    REGEXP_REPLACE(PHONE_NUMBER, '[^0-9]', '')
$$;
```

### Combined Pattern: Function + View That References It

Deploy the function first (`snow dcm deploy`), then deploy the view in a second pass.

```sql
DEFINE FUNCTION FINANCE_DB.ANALYTICS.CALCULATE_PROFIT_MARGIN(REVENUE NUMBER, COST NUMBER)
RETURNS NUMBER(10,2)
AS
$$
    CASE WHEN REVENUE = 0 THEN 0
         ELSE ROUND(((REVENUE - COST) / REVENUE) * 100, 2)
    END
$$;

-- Deploy the function above before this view, or deployment will fail.
DEFINE VIEW FINANCE_DB.ANALYTICS.V_PRODUCT_PROFITABILITY AS
SELECT
    PRODUCT_ID, PRODUCT_NAME, REVENUE, COST,
    FINANCE_DB.ANALYTICS.CALCULATE_PROFIT_MARGIN(REVENUE, COST) AS PROFIT_MARGIN_PCT
FROM FINANCE_DB.RAW.PRODUCTS;
```
