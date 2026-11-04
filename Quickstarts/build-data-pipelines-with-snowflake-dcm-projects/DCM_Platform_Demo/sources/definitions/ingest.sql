-- ### Ingestion
--
-- A stage for the CSV files, a file format that describes them, and a task that
-- copies them into the three staging tables on a schedule. The task runs on the
-- platform's own warehouse: every scheduled object in this guide uses a
-- warehouse, so there is one compute model to reason about.
--
-- DCM creates a new task suspended, so the schedule below does nothing until
-- someone resumes it. In this guide you run it once by hand instead.

DEFINE WAREHOUSE DCM_DEMO_2_PLATFORM_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh_size}}'
    AUTO_SUSPEND = 60
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Runs the platform load task for the DCM Build Data Pipelines Quickstart';

DEFINE STAGE DCM_DEMO_2{{env_suffix}}.RAW.DCM_SAMPLE_DATA
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'CSV files with sample data for the DCM Build Data Pipelines Quickstart';

DEFINE FILE FORMAT DCM_DEMO_2{{env_suffix}}.RAW.CSV_FORMAT
    TYPE = CSV
    COMPRESSION = NONE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    FIELD_DELIMITER = ','
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMMENT = 'Comma-separated files with one header row';

DEFINE TASK DCM_DEMO_2{{env_suffix}}.RAW.LOAD_NEW_DATA
    WAREHOUSE = DCM_DEMO_2_PLATFORM_WH{{env_suffix}}
    SCHEDULE = 'USING CRON 15 8-18 * * MON-FRI Europe/Berlin'
    COMMENT = 'Loads the staged CSV files into the raw staging tables'
AS
BEGIN
    COPY INTO DCM_DEMO_2{{env_suffix}}.RAW.CUSTOMER_STG
        FROM @DCM_DEMO_2{{env_suffix}}.RAW.DCM_SAMPLE_DATA/CUSTOMER_STG.csv
        FILE_FORMAT = (FORMAT_NAME = 'DCM_DEMO_2{{env_suffix}}.RAW.CSV_FORMAT');

    COPY INTO DCM_DEMO_2{{env_suffix}}.RAW.ACCOUNT_STG
        FROM @DCM_DEMO_2{{env_suffix}}.RAW.DCM_SAMPLE_DATA/ACCOUNT_STG.csv
        FILE_FORMAT = (FORMAT_NAME = 'DCM_DEMO_2{{env_suffix}}.RAW.CSV_FORMAT');

    COPY INTO DCM_DEMO_2{{env_suffix}}.RAW.TRADE_STG
        FROM @DCM_DEMO_2{{env_suffix}}.RAW.DCM_SAMPLE_DATA/TRADE_STG.csv
        FILE_FORMAT = (FORMAT_NAME = 'DCM_DEMO_2{{env_suffix}}.RAW.CSV_FORMAT');

    CALL SYSTEM$SET_RETURN_VALUE('raw dataset loaded into all staging tables');
END;
