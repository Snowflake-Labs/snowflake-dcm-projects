define schema DCM_DEMO_1{{env_suffix}}.SERVE;


define view DCM_DEMO_1{{env_suffix}}.SERVE.V_DASHBOARD_DAILY_SALES
data_metric_schedule = 'USING CRON 0 8 * * * UTC'
as
select
    date_trunc('DAY', ORDER_TS) as SALE_DATE,
    count(distinct ORDER_ID) as DAILY_ORDERS,
    sum(LINE_ITEM_REVENUE) as DAILY_REVENUE,
    sum(LINE_ITEM_PROFIT) as DAILY_PROFITS
from
    DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
group by
    SALE_DATE
order by
    SALE_DATE
;



define view DCM_DEMO_1{{env_suffix}}.SERVE.V_DASHBOARD_KPI_SUMMARY
data_metric_schedule = 'USING CRON 0 8 * * * UTC'
as
select
    sum(TOTAL_SPEND_USD) as TOTAL_LIFETIME_REVENUE,
    count(distinct CUSTOMER_ID) as TOTAL_CUSTOMERS,
    sum(TOTAL_ORDERS) as TOTAL_ORDERS,
    sum(TOTAL_SPEND_USD) / sum(TOTAL_ORDERS) as AVERAGE_ORDER_VALUE
from
    DCM_DEMO_1{{env_suffix}}.ANALYTICS.CUSTOMER_SPENDING_SUMMARY
;



define view DCM_DEMO_1{{env_suffix}}.SERVE.V_DASHBOARD_SALES_BY_CATEGORY_CITY
data_metric_schedule = 'USING CRON 0 4 * * * UTC'
as
select
    ITEM_CATEGORY,
    CUSTOMER_CITY,
    sum(LINE_ITEM_REVENUE) as TOTAL_REVENUE
from
    DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
group by
    ITEM_CATEGORY, 
    CUSTOMER_CITY
order by
    ITEM_CATEGORY, 
    CUSTOMER_CITY
;


define secure view DCM_DEMO_1{{env_suffix}}.SERVE.V_DASHBOARD_NEW_VS_RETURNING_CUSTOMERS
data_metric_schedule = 'USING CRON 0 8 * * * UTC'
as
with
customer_order_dates as (
    select
        e.ORDER_ID,
        e.CUSTOMER_ID,
        date_trunc('DAY', e.ORDER_TS) as ORDER_DATE,
        date_trunc('DAY', s.FIRST_ORDER_DATE) as CUSTOMER_FIRST_ORDER_DATE,
        e.LINE_ITEM_REVENUE
    from
        DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS e
    join
        DCM_DEMO_1{{env_suffix}}.ANALYTICS.CUSTOMER_SPENDING_SUMMARY s 
        on e.CUSTOMER_ID = s.CUSTOMER_ID
)
select
    ORDER_DATE,
    case when ORDER_DATE = CUSTOMER_FIRST_ORDER_DATE 
        then 'New Customer'
        else 'Returning Customer'
        end as CUSTOMER_TYPE,
    sum(LINE_ITEM_REVENUE) as REVENUE
from
    customer_order_dates
group by
    ORDER_DATE, 
    CUSTOMER_TYPE
order by
    ORDER_DATE desc, 
    CUSTOMER_TYPE
;