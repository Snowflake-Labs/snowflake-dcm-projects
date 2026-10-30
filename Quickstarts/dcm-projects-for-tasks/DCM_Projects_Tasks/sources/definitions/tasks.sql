/*=============================================================================
  tasks.sql: one task graph and its member tasks

      LOAD_WEATHER (root) ─► BUILD_DAILY_SUMMARY
                          └► BUILD_HEAT_WARNINGS   (Act 2)
      LOG_RUN (finalizer, runs after every other task)

  Member tasks hold only their body and their AFTER links. The graph holds the
  schedule, the root, the finalizer and the overlap policy, and passes the
  warehouse and timeout to every member through TASK_DEFAULTS.
=============================================================================*/

DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.LOAD_WEATHER
    COMMENT = 'Root task: appends new source rows to RAW_WEATHER_DATA'
AS
    DECLARE
        RESULT VARCHAR;
    BEGIN
        INSERT INTO DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA
        SELECT ROW_ID, CURRENT_TIMESTAMP(), DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F
        FROM DCM_DEMO_4{{env_suffix}}.PIPELINE.WEATHER_DATA_SOURCE
        WHERE ROW_ID > (SELECT COALESCE(MAX(ROW_ID), 0) FROM DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA);
        RESULT := SQLROWCOUNT || ' rows loaded';
        CALL SYSTEM$SET_RETURN_VALUE(:RESULT);
    END;

DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.BUILD_DAILY_SUMMARY
    COMMENT = 'Rebuilds DAILY_WEATHER_SUMMARY from RAW_WEATHER_DATA'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.LOAD_WEATHER
AS
    INSERT OVERWRITE INTO DCM_DEMO_4{{env_suffix}}.PIPELINE.DAILY_WEATHER_SUMMARY
    SELECT DS, ZIPCODE, AVG(AVG_TEMP_IN_F), MAX(MAX_TEMP_IN_F)
    FROM DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA
    GROUP BY DS, ZIPCODE;

-- Act 2: uncomment this task together with the HEAT_WARNINGS table in tables.sql
-- DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.BUILD_HEAT_WARNINGS
--     COMMENT = 'Rebuilds HEAT_WARNINGS from RAW_WEATHER_DATA'
--     AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.LOAD_WEATHER
-- AS
--     INSERT OVERWRITE INTO DCM_DEMO_4{{env_suffix}}.PIPELINE.HEAT_WARNINGS
--     SELECT DS, ZIPCODE, MAX(MAX_TEMP_IN_F)
--     FROM DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA
--     GROUP BY DS, ZIPCODE
--     HAVING MAX(MAX_TEMP_IN_F) >= 85;

DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.LOG_RUN
    COMMENT = 'Finalizer: writes one row per graph run to PIPELINE_RUN_LOG'
AS
    INSERT INTO DCM_DEMO_4{{env_suffix}}.PIPELINE.PIPELINE_RUN_LOG
    SELECT SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
           CURRENT_TIMESTAMP(),
           (SELECT COUNT(*) FROM DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA);

DEFINE TASK GRAPH DCM_DEMO_4{{env_suffix}}.PIPELINE.WEATHER_PIPELINE
    SCHEDULE = 'USING CRON 0 * * * * UTC'
    ROOTS = (DCM_DEMO_4{{env_suffix}}.PIPELINE.LOAD_WEATHER)
    FINALIZER = DCM_DEMO_4{{env_suffix}}.PIPELINE.LOG_RUN
    OVERLAP_POLICY = NO_OVERLAP
    COMMENT = 'Hourly weather pipeline managed by a DCM project'
    TASK_DEFAULTS (
        WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}',
        USER_TASK_TIMEOUT_MS = 600000
    )
    STARTED;
