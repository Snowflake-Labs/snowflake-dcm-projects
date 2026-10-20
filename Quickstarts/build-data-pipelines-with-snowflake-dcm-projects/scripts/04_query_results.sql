-- =============================================================================
-- Script 4: Query Results
-- Run this AFTER the Pipeline project has been deployed successfully
-- =============================================================================

-- 1. Refresh the pipeline's dynamic tables in one statement (created with INITIALIZE = ON_SCHEDULE).
--    Listing the GOLD fact DTs merges their shared SILVER upstream and refreshes at a single
--    data timestamp. DIM_TRADE is included because the FACT_HOLDINGS view (queried below) reads
--    it -- a view can't be refreshed, so its upstream DT must be refreshed instead.
USE ROLE dcm_demo_2_finance_dev_admin;
ALTER DYNAMIC TABLE dcm_demo_2_finance_dev.gold.fact_market_history,
                    dcm_demo_2_finance_dev.gold.fact_prospect,
                    dcm_demo_2_finance_dev.gold.fact_cash_balances,
                    dcm_demo_2_finance_dev.silver.dim_trade REFRESH;

-- 2. Query fact tables (allow a minute for refresh to complete)
SELECT * FROM dcm_demo_2_finance_dev.gold.fact_prospect LIMIT 10;

SELECT * FROM dcm_demo_2_finance_dev.gold.fact_cash_balances LIMIT 10;

SELECT * FROM dcm_demo_2_finance_dev.gold.fact_holdings LIMIT 10;

-- 3. Check data quality expectations
SELECT *
FROM TABLE(dcm_demo_2_finance_dev.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'dcm_demo_2_finance_dev.gold.fact_prospect',
    REF_ENTITY_DOMAIN => 'TABLE'
));

-- 4. Run all expectations and check pass/fail
USE ROLE dcm_demo_2_finance_dev_admin;
EXECUTE DCM PROJECT dcm_demo_2_finance_dev.projects.finance_pipeline TEST ALL;
