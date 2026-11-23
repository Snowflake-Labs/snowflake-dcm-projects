DEFINE SCHEMA DCM_DEMO_3{{env_suffix}}.SERVE
    COMMENT = 'Views for dashboards and reporting';

DEFINE VIEW DCM_DEMO_3{{env_suffix}}.SERVE.V_DASHBOARD_DAILY_SALES
    DATA_METRIC_SCHEDULE = 'USING CRON 0 8 * * * UTC'
    COMMENT = 'Orders, revenue and profit per day'
AS
SELECT
    DATE_TRUNC('DAY', ORDER_TS) AS SALE_DATE,
    COUNT(DISTINCT ORDER_ID) AS DAILY_ORDERS,
    SUM(LINE_ITEM_REVENUE) AS DAILY_REVENUE,
    SUM(LINE_ITEM_PROFIT) AS DAILY_PROFITS
FROM DCM_DEMO_3{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
GROUP BY SALE_DATE;

DEFINE VIEW DCM_DEMO_3{{env_suffix}}.SERVE.V_DASHBOARD_KPI_SUMMARY
    COMMENT = 'Headline revenue and order KPIs'
AS
SELECT
    SUM(TOTAL_SPEND_USD) AS TOTAL_LIFETIME_REVENUE,
    COUNT(DISTINCT CUSTOMER_ID) AS TOTAL_CUSTOMERS,
    SUM(TOTAL_ORDERS) AS TOTAL_ORDERS,
    SUM(TOTAL_SPEND_USD) / SUM(TOTAL_ORDERS) AS AVERAGE_ORDER_VALUE
FROM DCM_DEMO_3{{env_suffix}}.ANALYTICS.CUSTOMER_SPENDING_SUMMARY;

DEFINE VIEW DCM_DEMO_3{{env_suffix}}.SERVE.V_DASHBOARD_SALES_BY_CATEGORY_CITY
    COMMENT = 'Revenue per menu category and customer city'
AS
SELECT
    ITEM_CATEGORY,
    CUSTOMER_CITY,
    SUM(LINE_ITEM_REVENUE) AS TOTAL_REVENUE
    -- CHANGE STEP 3 of 4: comment the line above and uncomment the two lines
    -- below, then do the same for the GROUP BY. DIET_CLASSIFICATION goes last,
    -- because CREATE OR ALTER VIEW cannot reorder columns either.
    -- SUM(LINE_ITEM_REVENUE) AS TOTAL_REVENUE,
    -- DIET_CLASSIFICATION
FROM DCM_DEMO_3{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
GROUP BY ITEM_CATEGORY, CUSTOMER_CITY;
-- GROUP BY ITEM_CATEGORY, CUSTOMER_CITY, DIET_CLASSIFICATION;

-- A semantic view over the same dynamic table the dashboard views read, so
-- the business vocabulary for Cortex Analyst and agents is versioned and
-- deployed with the pipeline that produces the numbers.
DEFINE SEMANTIC VIEW DCM_DEMO_3{{env_suffix}}.SERVE.SALES_ANALYTICS

  TABLES (
    order_lines AS DCM_DEMO_3{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
      WITH SYNONYMS ('order lines', 'order details', 'sales lines')
      COMMENT = 'One row per menu item on an order, with revenue and profit'
  )

  DIMENSIONS (
    order_lines.ORDER_DATE AS DATE_TRUNC('DAY', order_lines.ORDER_TS)
      WITH SYNONYMS = ('date', 'day', 'trading day')
      COMMENT = 'Calendar day the order was placed',
    order_lines.ITEM_CATEGORY AS order_lines.ITEM_CATEGORY
      WITH SYNONYMS = ('category', 'menu category', 'food type')
      COMMENT = 'Menu category, for example Dessert or BBQ',
    order_lines.TRUCK_BRAND AS order_lines.TRUCK_BRAND_NAME
      WITH SYNONYMS = ('truck', 'brand', 'food truck')
      COMMENT = 'Food truck brand that served the order',
    order_lines.CUSTOMER_CITY AS order_lines.CUSTOMER_CITY
      WITH SYNONYMS = ('city', 'customer city')
      COMMENT = 'City of the customer who placed the order'
    -- CHANGE STEP 4 of 4: comment the line above and uncomment the four lines
    -- below, so the semantic view exposes the new column as a dimension.
    --   COMMENT = 'City of the customer who placed the order',
    -- order_lines.DIET_CLASSIFICATION AS order_lines.DIET_CLASSIFICATION
    --   WITH SYNONYMS = ('diet', 'healthy or indulgent')
    --   COMMENT = 'Healthy or Indulgent, from AI_CLASSIFY; NULL for frozen rows'
  )

  METRICS (
    order_lines.TOTAL_REVENUE AS SUM(order_lines.LINE_ITEM_REVENUE)
      WITH SYNONYMS = ('revenue', 'sales', 'turnover')
      COMMENT = 'Total revenue taken, in USD',
    order_lines.TOTAL_PROFIT AS SUM(order_lines.LINE_ITEM_PROFIT)
      WITH SYNONYMS = ('profit', 'gross profit')
      COMMENT = 'Total gross profit, in USD',
    order_lines.ORDER_COUNT AS COUNT(DISTINCT order_lines.ORDER_ID)
      WITH SYNONYMS = ('orders', 'order count')
      COMMENT = 'Number of distinct orders'
  )

  COMMENT = 'Semantic layer over the enriched order details dynamic table';
