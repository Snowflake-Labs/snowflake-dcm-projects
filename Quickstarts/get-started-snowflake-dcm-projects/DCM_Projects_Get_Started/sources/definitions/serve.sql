-- ### SERVE — the semantic view
--
-- The serve layer is a semantic view rather than a plain view, so the business
-- vocabulary an agent depends on is versioned and reviewed alongside the
-- pipeline that produces the numbers. Rename a metric here and the change shows
-- up in a plan diff.
--
-- It reads only from the modelled dynamic table, never from RAW, so the
-- semantic layer always sits on modelled data.
--
-- Single-table scope means there are no relationships to declare, which
-- sidesteps the rule that every constraint and relationship must be named.

DEFINE SCHEMA DCM_DEMO_1{{env_suffix}}.SERVE
    COMMENT = 'Serve layer exposed to BI tools and AI agents';

DEFINE SEMANTIC VIEW DCM_DEMO_1{{env_suffix}}.SERVE.ORDER_ANALYTICS

  TABLES (
    order_lines AS DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
      WITH SYNONYMS ('order lines', 'order details', 'sales lines')
      COMMENT = 'One row per menu item on an order, with revenue and profit'
  )

  DIMENSIONS (
    order_lines.ORDER_DATE AS DATE_TRUNC('DAY', order_lines.ORDER_TS)
      WITH SYNONYMS = ('date', 'day', 'trading day')
      COMMENT = 'Calendar day the order was placed',
    order_lines.ORDER_MONTH AS DATE_TRUNC('MONTH', order_lines.ORDER_TS)
      WITH SYNONYMS = ('month', 'monthly')
      COMMENT = 'Calendar month the order was placed',
    order_lines.MENU_ITEM_NAME AS order_lines.MENU_ITEM_NAME
      WITH SYNONYMS = ('menu item', 'item', 'dish', 'product')
      COMMENT = 'Name of the menu item sold',
    order_lines.ITEM_CATEGORY AS order_lines.ITEM_CATEGORY
      WITH SYNONYMS = ('category', 'menu category', 'food type')
      COMMENT = 'Menu category, for example Tacos or Pizza'
  )

  METRICS (
    order_lines.TOTAL_REVENUE AS SUM(order_lines.LINE_ITEM_REVENUE)
      WITH SYNONYMS = ('revenue', 'sales', 'turnover')
      COMMENT = 'Total revenue taken, in USD',
    order_lines.TOTAL_PROFIT AS SUM(order_lines.LINE_ITEM_PROFIT)
      WITH SYNONYMS = ('profit', 'gross profit', 'earnings')
      COMMENT = 'Total gross profit, in USD',
    order_lines.ORDER_COUNT AS COUNT(DISTINCT order_lines.ORDER_ID)
      WITH SYNONYMS = ('orders', 'order count', 'transactions')
      COMMENT = 'Number of distinct orders',
    order_lines.ITEMS_SOLD AS SUM(order_lines.QUANTITY)
      WITH SYNONYMS = ('units', 'units sold', 'quantity sold')
      COMMENT = 'Total number of menu items sold'
  )

  COMMENT = 'AI-ready semantic layer over the enriched order details dynamic table'

  AI_SQL_GENERATION 'Round all currency values to 2 decimal places. Revenue and profit are in USD.'

  AI_QUESTION_CATEGORIZATION 'This model exposes aggregated order lines only and holds no customer or delivery data. Treat any question about a named customer, a single order, or a location as UNCLEAR and explain that limitation.';

-- The dashboard that consumes the semantic view is deployed by the same project,
-- so the pipeline and the app that reads it are promoted together. Its Python
-- lives in streamlit/dashboard/, outside sources/, and the manifest names that
-- folder as the `dashboard` asset. DEFINE STREAMLIT cannot take a folder path
-- directly -- only an asset:// URI -- which is why the asset exists.
--
-- MAIN_FILE is relative to the imported asset root, not to this file. Edit the
-- Python and the next plan shows the Streamlit object in the changeset, because
-- PLAN tracks the asset contents as well as the DEFINE statement.

DEFINE STREAMLIT DCM_DEMO_1{{env_suffix}}.SERVE.ORDERS_DASHBOARD
    FROM 'asset://dashboard/'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = DCM_DEMO_1_WH{{env_suffix}}
    TITLE = 'Orders Dashboard'
    COMMENT = 'Reads the ORDER_ANALYTICS semantic view; deployed from the dashboard asset';
