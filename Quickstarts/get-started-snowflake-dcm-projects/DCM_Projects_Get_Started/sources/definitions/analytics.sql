-- ### ANALYTICS — the modelled layer
--
-- One dynamic table joins the three raw tables and derives revenue and profit.
-- WAREHOUSE and TARGET_LAG are the only attributes that alter in place; any
-- change to the body below forces a re-initialization or a full refresh, which
-- is exactly the blast radius a plan shows you before you pay for it.

DEFINE SCHEMA DCM_DEMO_1{{env_suffix}}.ANALYTICS
    COMMENT = 'Modelled layer, built and kept fresh by a dynamic table';

DEFINE DYNAMIC TABLE DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
WAREHOUSE = DCM_DEMO_1_WH{{env_suffix}}
TARGET_LAG = '1 hour'
-- CHANGE STEP 1 of 3: comment the line above and uncomment the line below, then
-- re-plan. TARGET_LAG and WAREHOUSE are the only dynamic-table attributes that
-- alter in place. Any change to the body below forces a re-initialization or a
-- full refresh instead, which is exactly the blast radius a plan shows you
-- before you pay for it.
-- TARGET_LAG = '30 minutes'
INITIALIZE = 'ON_SCHEDULE'
COMMENT = 'Order lines enriched with menu attributes, revenue and profit'
AS
SELECT
    oh.ORDER_ID,
    oh.ORDER_TS,
    od.QUANTITY,
    m.MENU_ITEM_NAME,
    m.ITEM_CATEGORY,
    m.SALE_PRICE_USD,
    m.COST_OF_GOODS_USD,
    (od.QUANTITY * m.SALE_PRICE_USD) AS LINE_ITEM_REVENUE,
    (od.QUANTITY * (m.SALE_PRICE_USD - m.COST_OF_GOODS_USD)) AS LINE_ITEM_PROFIT
FROM
    DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER oh
JOIN
    DCM_DEMO_1{{env_suffix}}.RAW.ORDER_DETAIL od
    ON oh.ORDER_ID = od.ORDER_ID
JOIN
    DCM_DEMO_1{{env_suffix}}.RAW.MENU m
    ON od.MENU_ITEM_ID = m.MENU_ITEM_ID
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        oh.ORDER_ID,
        m.MENU_ITEM_NAME
    ORDER BY
        oh.ORDER_TS DESC
    ) = 1
;
