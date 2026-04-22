/*=============================================================================
  tables.sql — Source, landing, target, and quarantine tables

  The task graph demonstrates a realistic ELT pattern:
  source ─► landing (quality gate) ─► target
                               └───► quarantine (if DMFs fail)
  plus a generic TASK_DEMO_TABLE used by the stream-conditional task.
=============================================================================*/

--- weather landing table where new batches are loaded and checked
DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA (
    ROW_ID         NUMBER,
    INSERTED       TIMESTAMP_NTZ,
    DS             DATE,
    ZIPCODE        VARCHAR,
    MIN_TEMP_IN_F  NUMBER,
    AVG_TEMP_IN_F  NUMBER,
    MAX_TEMP_IN_F  NUMBER
)
CHANGE_TRACKING = TRUE
DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES'
COMMENT = 'Landing table where DMF quality gates run';

--- clean rows that passed quality checks
DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.CLEAN_WEATHER_DATA (
    INSERTED       TIMESTAMP_NTZ,
    DS             DATE,
    ZIPCODE        VARCHAR,
    MIN_TEMP_IN_F  NUMBER,
    AVG_TEMP_IN_F  NUMBER,
    MAX_TEMP_IN_F  NUMBER
)
COMMENT = 'Target table for data that passed quality gates';

--- rows that failed quality checks, isolated for later review
DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.QUARANTINED_WEATHER_DATA (
    INSERTED       TIMESTAMP_NTZ,
    DS             DATE,
    ZIPCODE        VARCHAR,
    MIN_TEMP_IN_F  NUMBER,
    AVG_TEMP_IN_F  NUMBER,
    MAX_TEMP_IN_F  NUMBER
)
COMMENT = 'Quarantine table for rows that failed quality gates';

--- source table that the pipeline reads rows from
DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.WEATHER_DATA_SOURCE (
    ROW_ID         NUMBER AUTOINCREMENT START 1 INCREMENT 1 ORDER,
    DS             DATE,
    ZIPCODE        VARCHAR,
    MIN_TEMP_IN_F  NUMBER,
    AVG_TEMP_IN_F  NUMBER,
    MAX_TEMP_IN_F  NUMBER
)
CHANGE_TRACKING = TRUE
COMMENT = 'Source rows that LOAD_RAW_DATA pulls into the landing table';

--- generic demo table used by the stream-conditional task DEMO_TASK_8
DEFINE TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.TASK_DEMO_TABLE (
    TIME_STAMP TIMESTAMP_NTZ(9),
    ID         NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1 ORDER,
    MESSAGE    VARCHAR,
    COMMENT    VARCHAR
)
CHANGE_TRACKING = TRUE
COMMENT = 'Generic table used by the stream-conditional task';
