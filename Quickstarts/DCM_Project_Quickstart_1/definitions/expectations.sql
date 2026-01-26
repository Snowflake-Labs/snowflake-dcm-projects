-- ! to attach expectations to columns you need to for grant data metric functions to the owner role (manually outside of DCM!)
    -- grant application role SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER to role DCM_DEVELOPER;
    -- grant application role SNOWFLAKE.DATA_QUALITY_MONITORING_ADMIN to role DCM_DEVELOPER;
    -- grant execute data metric function on account to role DCM_DEVELOPER;
-- ! also make sure you set a 'data_metric_schedule' for all tables/DTs/views that should have data expectations



-- user-defined DMF
define data metric function DCM_PROJECT_{{env_suffix}}.RAW.INVENTORY_SPREAD(TABLE_NAME table(COLUMN_VALUE number))
returns NUMBER
as
$$
  select
    max(COLUMN_VALUE) - min(COLUMN_VALUE)
  from 
    TABLE_NAME
  where
    COLUMN_VALUE is not NULL
$$
;


-- attach system DMF to Table column
attach data metric function SNOWFLAKE.CORE.MIN
    to table DCM_PROJECT_{{env_suffix}}.RAW.INVENTORY
    on (IN_STOCK)
    expectation MIN_10_ITEMS_INVENTORY (value > 10); 

-- attach system DMF to Dynamic Table column
attach data metric function SNOWFLAKE.CORE.NULL_COUNT
    to dynamic table DCM_PROJECT_{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
    on (CUSTOMER_CITY)
    expectation NO_MISSING_CITIES (value = 0);

-- attach system DMF to View column
attach data metric function SNOWFLAKE.CORE.MIN
    to view DCM_PROJECT_{{env_suffix}}.SERVE.V_DASHBOARD_DAILY_SALES
    on (DAILY_ORDERS)
    expectation ORDERS_POSITIVE (value >= 0);

--attach UDMF to Table column
attach data metric function DCM_PROJECT_{{env_suffix}}.RAW.INVENTORY_SPREAD
    to table DCM_PROJECT_{{env_suffix}}.RAW.INVENTORY
    on (IN_STOCK)
    expectation EVEN_INVENTORY (value < 100);

--attach UDMF to dynamic table column for demo
attach data metric function SNOWFLAKE.CORE.UNIQUE_COUNT
    to view DCM_PROJECT_{{env_suffix}}.SERVE.V_DASHBOARD_SALES_BY_CATEGORY_CITY
    on (CUSTOMER_CITY)
    expectation ALL_CITIES_REPORTED_SALES (value = 5);