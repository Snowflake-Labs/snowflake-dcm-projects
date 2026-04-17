-- =============================================================================
-- Script 4: Query Results
-- Run this AFTER the Pipeline project has been deployed successfully
-- =============================================================================

-- 1. Query fact tables
SELECT * FROM dcm_demo_2_finance_dev.gold.fact_market_history LIMIT 10;

SELECT * FROM dcm_demo_2_finance_dev.gold.fact_prospect LIMIT 10;

SELECT * FROM dcm_demo_2_finance_dev.gold.fact_cash_balances LIMIT 10;

SELECT * FROM dcm_demo_2_finance_dev.gold.fact_holdings LIMIT 10;

-- 2. Check data quality expectations
SELECT *
FROM TABLE(dcm_demo_2_finance_dev.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'dcm_demo_2_finance_dev.gold.fact_prospect',
    REF_ENTITY_DOMAIN => 'TABLE'
));
