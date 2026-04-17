-- =============================================================================
-- Script 2: After Platform Deployment
-- Run this AFTER the Platform project has been deployed successfully
-- =============================================================================

-- 1. Copy CSV files to the ingestion stage
COPY FILES INTO
    @dcm_demo_2_dev.ingest.dcm_sample_data
FROM
    'snow://workspace/USER$.PUBLIC."snowflake-dcm-projects"/versions/live/Quickstarts/build-data-pipelines-with-snowflake-dcm-projects/sample_data'
DETAILED_OUTPUT = TRUE;

-- 2. Trigger the load Task
EXECUTE TASK dcm_demo_2_dev.ingest.load_new_data;

-- 3. Verify data was loaded
SELECT COUNT(*) AS customer_count FROM dcm_demo_2_dev.raw.customer_stg;
SELECT COUNT(*) AS trade_count FROM dcm_demo_2_dev.raw.trade_stg;
SELECT COUNT(*) AS dailymarket_count FROM dcm_demo_2_dev.raw.dailymarket_stg;
