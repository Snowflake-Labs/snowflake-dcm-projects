define schema DCM_DEMO_1{{env_suffix}}.ANALYTICS;

define dynamic table DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
warehouse = DCM_DEMO_1_WH{{env_suffix}}
target_lag = 'DOWNSTREAM'
initialize = 'ON_SCHEDULE'
data_metric_schedule = 'TRIGGER_ON_CHANGES'
as
select
    oh.ORDER_ID,
    oh.ORDER_TS,
    od.QUANTITY,
    m.MENU_ITEM_NAME,
    m.ITEM_CATEGORY,
    m.SALE_PRICE_USD,
    m.COST_OF_GOODS_USD,
    (od.QUANTITY * m.SALE_PRICE_USD) as LINE_ITEM_REVENUE,
    (od.QUANTITY * (m.SALE_PRICE_USD - m.COST_OF_GOODS_USD)) as LINE_ITEM_PROFIT,
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    initcap(c.CITY) as CUSTOMER_CITY, 
    t.TRUCK_ID,
    t.TRUCK_BRAND_NAME
from
    DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER oh
join 
    DCM_DEMO_1{{env_suffix}}.RAW.ORDER_DETAIL od 
    on oh.ORDER_ID = od.ORDER_ID
join 
    DCM_DEMO_1{{env_suffix}}.RAW.MENU m 
    on od.MENU_ITEM_ID = m.MENU_ITEM_ID
join 
    DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER c 
    on oh.CUSTOMER_ID = c.CUSTOMER_ID
join 
    DCM_DEMO_1{{env_suffix}}.RAW.TRUCK t 
    on oh.TRUCK_ID = t.TRUCK_ID
qualify row_number() over (
    partition by 
        oh.ORDER_ID, 
        m.MENU_ITEM_NAME 
    order by 
        oh.ORDER_TS desc
    ) = 1
;


define dynamic table DCM_DEMO_1{{env_suffix}}.ANALYTICS.MENU_ITEM_POPULARITY
warehouse = DCM_DEMO_1_WH{{env_suffix}}
target_lag = '1 day'
initialize = 'ON_SCHEDULE'
data_metric_schedule = 'TRIGGER_ON_CHANGES'
as
select
    MENU_ITEM_NAME,
    ITEM_CATEGORY,
    count(distinct ORDER_ID) as NUMBER_OF_ORDERS,
    sum(QUANTITY) as TOTAL_QUANTITY_SOLD,
    sum(LINE_ITEM_REVENUE) as TOTAL_REVENUE
from
    DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
group by
    MENU_ITEM_NAME, 
    ITEM_CATEGORY
order by
    TOTAL_REVENUE desc
;


define dynamic table DCM_DEMO_1{{env_suffix}}.ANALYTICS.CUSTOMER_SPENDING_SUMMARY
warehouse = DCM_DEMO_1_WH{{env_suffix}}
target_lag = '2 days'
initialize = 'ON_SCHEDULE'
as
select
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    CUSTOMER_CITY,
    count(distinct ORDER_ID) as TOTAL_ORDERS,
    sum(LINE_ITEM_REVENUE) as TOTAL_SPEND_USD,
    MIN(ORDER_TS) as FIRST_ORDER_DATE,
    MAX(ORDER_TS) as LATEST_ORDER_DATE
from
    DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
group by
    CUSTOMER_ID, 
    FIRST_NAME, 
    LAST_NAME, 
    CUSTOMER_CITY
order by
    TOTAL_SPEND_USD desc
;


define dynamic table DCM_DEMO_1{{env_suffix}}.ANALYTICS.TRUCK_PERFORMANCE
warehouse = DCM_DEMO_1_WH{{env_suffix}}
target_lag = '12 hours'
initialize = 'ON_SCHEDULE'
as
select
    TRUCK_BRAND_NAME,
    count(distinct ORDER_ID) as TOTAL_ORDERS,
    sum(LINE_ITEM_REVENUE) as TOTAL_REVENUE,
    sum(LINE_ITEM_PROFIT) as TOTAL_PROFIT,
    DCM_DEMO_1{{env_suffix}}.ANALYTICS.CALCULATE_PROFIT_MARGIN(sum(LINE_ITEM_REVENUE),
    sum(LINE_ITEM_REVENUE) - sum(LINE_ITEM_PROFIT)) as PROFIT_MARGIN_PCT
from
    DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
group by
    TRUCK_BRAND_NAME
order by
    TOTAL_REVENUE desc
;


define function DCM_DEMO_1{{env_suffix}}.ANALYTICS.CALCULATE_PROFIT_MARGIN(REVENUE number, COST number)
returns NUMBER(10,2)
as
$$
    case when REVENUE = 0 
then 0
else round(((REVENUE - COST) / REVENUE) * 100, 2)
    end
$$
;