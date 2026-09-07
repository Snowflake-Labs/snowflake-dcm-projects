-- ### RAW — the three landing tables the pipeline reads from
--
-- CHANGE_TRACKING is required on every table a dynamic table reads, so the
-- incremental refresh can see what changed instead of rescanning the table.

DEFINE DATABASE DCM_DEMO_1{{env_suffix}}
    COMMENT = 'Quickstart demo database for DCM Projects';

-- ENVIRONMENT VARIABLES: the manifest declares BUILD_NUMBER, DEPLOYMENT_REGION
-- and the secret API_KEY as names only, with no values. Reference a declared
-- name with _snow.env_var() or _snow.env_secret() and supply the value at deploy
-- time, so it never lands in Git:
--
--   export BUILD_NUMBER=482
--   snow dcm deploy --target DCM_DEV
--
-- or from a file: snow dcm deploy --env-file .env
--
-- To try it, replace the COMMENT line above with the one below and export a
-- value first. A declared name that is never referenced is only a warning; once
-- a definition needs it to render, the value becomes required at plan time.
--
{% raw %}--  COMMENT = 'Quickstart demo database for DCM Projects (build {{ _snow.env_var("BUILD_NUMBER") }})';{% endraw %}
--
-- Note the raw/endraw wrapper around that example line. Jinja renders BEFORE
-- Snowflake parses the SQL, so a templated expression is still evaluated even
-- inside a SQL comment -- commenting a line out does NOT hide it from the
-- template engine. Without the wrapper, this file would demand a BUILD_NUMBER
-- value at plan time even though the line is commented out.

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
