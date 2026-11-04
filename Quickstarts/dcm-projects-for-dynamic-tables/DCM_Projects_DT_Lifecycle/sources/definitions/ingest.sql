DEFINE STAGE DCM_DEMO_3{{env_suffix}}.RAW.TASTY_BYTES_ORDERS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for daily incoming Tasty Bytes order files (CSV)';

DEFINE TASK DCM_DEMO_3{{env_suffix}}.RAW.TSK_INGEST_DAILY_ORDERS
    WAREHOUSE = DCM_DEMO_3_WH{{env_suffix}}
    SCHEDULE = 'USING CRON 0 5 * * * UTC'
    COMMENT = 'Loads the day''s order CSV files from the stage into RAW.ORDERS'
AS
COPY INTO DCM_DEMO_3{{env_suffix}}.RAW.ORDERS
FROM @DCM_DEMO_3{{env_suffix}}.RAW.TASTY_BYTES_ORDERS_STAGE
FILE_FORMAT = (
    TYPE = 'CSV',
    SKIP_HEADER = 1,
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
ON_ERROR = 'CONTINUE'
PURGE = TRUE;
