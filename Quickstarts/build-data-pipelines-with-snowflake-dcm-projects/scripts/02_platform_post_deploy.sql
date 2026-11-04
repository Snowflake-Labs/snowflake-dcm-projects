/*=============================================================================
  02_platform_post_deploy.sql: Run AFTER the Platform project is deployed

  Loads the sample data into the three raw staging tables, then adds the two
  trades that arrive today. Runs as DCM_DEVELOPER, which owns the raw tables,
  on the platform warehouse the project created.
=============================================================================*/

USE ROLE DCM_DEVELOPER;
USE WAREHOUSE DCM_DEMO_2_PLATFORM_WH_DEV;

----------------------------------------------------------------------
-- 1. Copy the CSV files into the ingestion stage
--
-- The source is this repository inside your Snowsight Workspace. The path
-- assumes the workspace name Snowsight derives from the repository URL,
-- snowflake-dcm-projects. If you named your workspace differently, replace
-- snowflake-dcm-projects in the path with your workspace name.
--
-- Running the script with the Snowflake CLI instead? Replace this statement
-- with a PUT from your local clone:
--   PUT file://<path-to-repo>/Quickstarts/build-data-pipelines-with-snowflake-dcm-projects/sample_data/*.csv
--       @DCM_DEMO_2_DEV.RAW.DCM_SAMPLE_DATA AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
-- Or upload the three files in Snowsight (Ingestion > Add Data > Load files
-- into a Stage, stage DCM_DEMO_2_DEV.RAW.DCM_SAMPLE_DATA) and skip this step.
----------------------------------------------------------------------
COPY FILES INTO @DCM_DEMO_2_DEV.RAW.DCM_SAMPLE_DATA
FROM 'snow://workspace/USER$.PUBLIC."snowflake-dcm-projects"/versions/live/Quickstarts/build-data-pipelines-with-snowflake-dcm-projects/sample_data'
DETAILED_OUTPUT = TRUE;

----------------------------------------------------------------------
-- 2. Run the load task once
--
-- The task is suspended (DCM creates new tasks suspended), so its schedule
-- never fires; EXECUTE TASK runs it once on demand. It runs asynchronously, so
-- wait a few seconds before checking.
----------------------------------------------------------------------
EXECUTE TASK DCM_DEMO_2_DEV.RAW.LOAD_NEW_DATA;
CALL SYSTEM$WAIT(15);

----------------------------------------------------------------------
-- 3. Today's trades
--
-- Two trades placed today. Unlike the files, they carry CURRENT_TIMESTAMP(),
-- which matters later in the guide.
----------------------------------------------------------------------
INSERT INTO DCM_DEMO_2_DEV.RAW.TRADE_STG
    (T_ID, T_DTS, T_ST_ID, T_TT_ID, T_S_SYMB, T_QTY, T_TRADE_PRICE, T_CHRG, T_COMM, T_CA_ID)
VALUES
    (2021, CURRENT_TIMESTAMP(), 'CMPT', 'TMB', 'SNOW', 40, 150.10, 9.95, 6.00, 1001),
    (2022, CURRENT_TIMESTAMP(), 'CMPT', 'TMS', 'ACME', 75,  41.30, 4.95, 3.10, 1006);

----------------------------------------------------------------------
-- 4. Verify
----------------------------------------------------------------------
SELECT 'CUSTOMER_STG' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM DCM_DEMO_2_DEV.RAW.CUSTOMER_STG
UNION ALL
SELECT 'ACCOUNT_STG', COUNT(*) FROM DCM_DEMO_2_DEV.RAW.ACCOUNT_STG
UNION ALL
SELECT 'TRADE_STG', COUNT(*) FROM DCM_DEMO_2_DEV.RAW.TRADE_STG;
