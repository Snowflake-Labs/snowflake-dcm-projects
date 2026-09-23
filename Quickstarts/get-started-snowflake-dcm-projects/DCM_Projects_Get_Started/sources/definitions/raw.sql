-- ### RAW — the three landing tables the pipeline reads from
--
-- CHANGE_TRACKING is required on every table a dynamic table reads, so the
-- incremental refresh can see what changed instead of rescanning the table.

DEFINE DATABASE DCM_DEMO_1{{env_suffix}}
    COMMENT = 'Quickstart demo database for DCM Projects';

-- TEMPLATES RENDER BEFORE SQL IS PARSED, and that includes comments. Jinja runs
-- over this file first and hands the result to Snowflake, so a templated
-- expression inside a SQL comment is still evaluated -- commenting a line out
-- does NOT hide it from the template engine. The line below is inert SQL but
-- live Jinja:
--
--   this database is DCM_DEMO_1{{env_suffix}}
--
-- Run `snow dcm plan --save-output` and read it back in
-- out/rendered/sources/definitions/raw.sql: the suffix has been substituted
-- inside the comment. That rendered folder is the single best way to see what
-- DCM actually evaluated, and it is where to look first when a template
-- surprises you.

DEFINE SCHEMA DCM_DEMO_1{{env_suffix}}.RAW
    COMMENT = 'Landing tables, seeded by scripts/02_post_deploy.sql';

DEFINE TABLE DCM_DEMO_1{{env_suffix}}.RAW.MENU (
    MENU_ITEM_ID NUMBER,
    MENU_ITEM_NAME VARCHAR,
    ITEM_CATEGORY VARCHAR,
    COST_OF_GOODS_USD NUMBER(10, 2),
    SALE_PRICE_USD NUMBER(10, 2)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Menu items with cost of goods and sale price';

DEFINE TABLE DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER (
    ORDER_ID NUMBER,
    ORDER_TS TIMESTAMP_NTZ -- Using a timezone-neutral timestamp
)
CHANGE_TRACKING = TRUE
COMMENT = 'One row per order';

DEFINE TABLE DCM_DEMO_1{{env_suffix}}.RAW.ORDER_DETAIL (
    ORDER_ID NUMBER,
    MENU_ITEM_ID NUMBER,
    QUANTITY NUMBER
)
CHANGE_TRACKING = TRUE
COMMENT = 'One row per menu item on an order';
