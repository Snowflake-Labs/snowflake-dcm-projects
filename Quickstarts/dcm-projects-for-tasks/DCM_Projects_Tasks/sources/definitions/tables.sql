/*=============================================================================
  tables.sql: tables the task graph reads and writes

      WEATHER_DATA_SOURCE ─► RAW_WEATHER_DATA ─► DAILY_WEATHER_SUMMARY
                                             (Act 2 adds HEAT_WARNINGS)
      PIPELINE_RUN_LOG gets one row per graph run from the finalizer.
=============================================================================*/

DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.WEATHER_DATA_SOURCE (
    ROW_ID         NUMBER AUTOINCREMENT START 1 INCREMENT 1 ORDER,
    DS             DATE,
    ZIPCODE        VARCHAR,
    MIN_TEMP_IN_F  NUMBER,
    AVG_TEMP_IN_F  NUMBER,
    MAX_TEMP_IN_F  NUMBER
)
COMMENT = 'Source rows, seeded by scripts/02_run_and_inspect.sql';

DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA (
    ROW_ID         NUMBER,
    LOADED_AT      TIMESTAMP_LTZ,
    DS             DATE,
    ZIPCODE        VARCHAR,
    MIN_TEMP_IN_F  NUMBER,
    AVG_TEMP_IN_F  NUMBER,
    MAX_TEMP_IN_F  NUMBER
)
COMMENT = 'Landing table, appended to by the root task LOAD_WEATHER';

DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.DAILY_WEATHER_SUMMARY (
    DS             DATE,
    ZIPCODE        VARCHAR,
    AVG_TEMP_IN_F  NUMBER(10,1),
    MAX_TEMP_IN_F  NUMBER
)
COMMENT = 'One row per day and ZIP code, rebuilt by BUILD_DAILY_SUMMARY';

DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.PIPELINE_RUN_LOG (
    RUN_GROUP_ID   VARCHAR,
    LOGGED_AT      TIMESTAMP_LTZ,
    RAW_ROWS       NUMBER
)
COMMENT = 'One row per graph run, written by the finalizer LOG_RUN';

-- Act 2: uncomment the table below together with the BUILD_HEAT_WARNINGS task in tasks.sql
-- DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.HEAT_WARNINGS (
--     DS             DATE,
--     ZIPCODE        VARCHAR,
--     MAX_TEMP_IN_F  NUMBER
-- )
-- COMMENT = 'Days where a ZIP code reached 85 F or more, rebuilt by BUILD_HEAT_WARNINGS';
