-- ### Semantic view over the gold layer
--
-- The business vocabulary for this pipeline: which tables, which dimensions,
-- which metrics. Declared here, it is versioned and reviewed with the pipeline
-- that produces the numbers, and a renamed metric shows up in a plan diff.
--
-- It reads only the gold tables. Cortex Analyst and SQL users who query it
-- never touch raw data.

DEFINE SEMANTIC VIEW DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.TRADING_ANALYTICS
  TABLES (
    daily AS DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.DAILY_TRADING
      PRIMARY KEY (TRADE_DATE)
      WITH SYNONYMS = ('daily trading', 'trading by day')
      COMMENT = 'One row per trading day',
    customers AS DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.CUSTOMER_TRADING
      WITH SYNONYMS = ('customer trading', 'trading by customer')
      COMMENT = 'One row per customer'
  )
  DIMENSIONS (
    daily.TRADE_DATE AS TRADE_DATE
      WITH SYNONYMS = ('date', 'day')
      COMMENT = 'Calendar day the trades were placed',
    customers.CUSTOMER_NAME AS CUSTOMER_NAME
      WITH SYNONYMS = ('customer', 'client')
      COMMENT = 'Full name of the customer',
    customers.CUSTOMER_TIER AS C_TIER
      WITH SYNONYMS = ('tier')
      COMMENT = 'Customer tier, 1 to 3'
  )
  METRICS (
    daily.TOTAL_TRADES AS SUM(TRADE_COUNT)
      WITH SYNONYMS = ('trades', 'number of trades')
      COMMENT = 'Number of completed trades',
    daily.TOTAL_TRADE_VALUE AS SUM(TOTAL_VALUE)
      WITH SYNONYMS = ('trading value', 'volume in dollars')
      COMMENT = 'Quantity times trade price, summed',
    customers.CUSTOMER_TRADE_VALUE AS SUM(TOTAL_VALUE)
      WITH SYNONYMS = ('value by customer')
      COMMENT = 'Trading value attributed to customers'
  )
  COMMENT = 'Semantic layer over the Finance gold tables';
