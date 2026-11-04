-- LOG_EVENT_LEVEL = ERROR makes every dynamic table in the database log its
-- refresh failures to the event table, which is what the DT_REFRESH_FAILURE
-- alert in alerts.sql reads.
DEFINE DATABASE DCM_DEMO_3{{env_suffix}}
    LOG_EVENT_LEVEL = ERROR
    COMMENT = 'This is a Quickstart Demo for DCM Projects with Dynamic Tables';

DEFINE SCHEMA DCM_DEMO_3{{env_suffix}}.RAW
    COMMENT = 'Landing tables for the food truck orders';

DEFINE TABLE DCM_DEMO_3{{env_suffix}}.RAW.MENU (
    MENU_ITEM_ID NUMBER,
    MENU_ITEM_NAME VARCHAR,
    ITEM_CATEGORY VARCHAR,
    COST_OF_GOODS_USD NUMBER(10, 2),
    SALE_PRICE_USD NUMBER(10, 2)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Menu items with cost and sale price';

DEFINE TABLE DCM_DEMO_3{{env_suffix}}.RAW.CUSTOMER (
    CUSTOMER_ID NUMBER,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    CITY VARCHAR
)
CHANGE_TRACKING = TRUE
COMMENT = 'Food truck customers';

DEFINE TABLE DCM_DEMO_3{{env_suffix}}.RAW.ORDERS (
    ORDER_ID NUMBER,
    ORDER_TS TIMESTAMP_NTZ,
    CUSTOMER_ID NUMBER,
    TRUCK_BRAND_NAME VARCHAR,
    MENU_ITEM_ID NUMBER,
    QUANTITY NUMBER
)
CHANGE_TRACKING = TRUE
DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES'
COMMENT = 'One row per order line, loaded by the ingestion task';
