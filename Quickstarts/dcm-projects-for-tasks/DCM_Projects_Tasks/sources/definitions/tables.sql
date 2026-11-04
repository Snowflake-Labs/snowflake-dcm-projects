/*=============================================================================
  tables.sql: tables the task graph reads and writes

      RAW_WEATHER_DATA ─► DAILY_WEATHER_SUMMARY
                       (Act 2 adds HEAT_WARNINGS)
=============================================================================*/

DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA (
    DS             DATE,
    ZIPCODE        VARCHAR,
    MIN_TEMP_IN_F  NUMBER,
    AVG_TEMP_IN_F  NUMBER,
    MAX_TEMP_IN_F  NUMBER,
    LOADED_AT      TIMESTAMP_LTZ
)
COMMENT = 'Readings inserted by the scripts; the root task LOAD_WEATHER stamps LOADED_AT';

DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.DAILY_WEATHER_SUMMARY (
    DS             DATE,
    ZIPCODE        VARCHAR,
    AVG_TEMP_IN_F  NUMBER(10,1),
    MAX_TEMP_IN_F  NUMBER
)
COMMENT = 'One row per day and ZIP code, rebuilt by BUILD_DAILY_SUMMARY';

-- Act 2: uncomment the table below together with the BUILD_HEAT_WARNINGS task in tasks.sql
-- DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.HEAT_WARNINGS (
--     DS             DATE,
--     ZIPCODE        VARCHAR,
--     MAX_TEMP_IN_F  NUMBER
-- )
-- COMMENT = 'Days where a ZIP code reached 85 F or more, rebuilt by BUILD_HEAT_WARNINGS';
