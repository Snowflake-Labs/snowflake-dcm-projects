/*=============================================================================
  tasks.sql — Complete task graph, defined declaratively with DCM

  Every task uses the new DCM `DEFINE TASK ... STARTED | SUSPENDED` property
  (early-access), so the graph is STARTED after deployment without any
  post-deploy `ALTER TASK ... RESUME` statements. DCM automatically resolves
  the dependency order between root and children.

  Graph structure (same as the classic Task Graphs notebook, plus a quality
  gate branch feeding into the target/quarantine tables):

      DEMO_TASK_1 (root) ─┬─► DEMO_TASK_2 ──► DEMO_TASK_4 ─┬─► DEMO_TASK_5 (serverless)
                          │                                ├─► DEMO_TASK_9 (retries) ─┬─► DEMO_TASK_10
                          │                                │                          └─► DEMO_TASK_14 (suspended)
                          │                                │                                      └─► DEMO_TASK_15
                          │                                └─► DEMO_TASK_13 (two predecessors)
                          │
                          ├─► DEMO_TASK_3 ─┬─► DEMO_TASK_6 ─┬─► DEMO_TASK_7 ─► DEMO_TASK_8 (stream cond)
                          │                │                └─► DEMO_TASK_11 (return-value cond)
                          │                └─► DEMO_TASK_12 ──► DEMO_TASK_13
                          │
                          └─► LOAD_RAW_DATA ─► CHECK_DATA_QUALITY ─┬─► TRANSFORM_DATA  (passed)
                                                                   └─► ISOLATE_DATA_ISSUES (failed)
                                                                             └─► NOTIFY_ABOUT_QUALITY_ISSUE

      DEMO_FINALIZER  (finalize = DEMO_TASK_1, sends plain-text email summary)
=============================================================================*/

----------------------------------------------------------------------
-- 1. ROOT TASK
--    Scheduled hourly during EU business hours, with retries and a
--    graph-level `config` parameter (RUNTIME_MULTIPLIER) picked up by
--    every child task via SYSTEM$GET_TASK_GRAPH_CONFIG.
----------------------------------------------------------------------
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_1
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    SCHEDULE = 'USING CRON 15 8-18 * * MON-FRI CET'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 0
    TASK_AUTO_RETRY_ATTEMPTS = 2
    OVERLAP_POLICY = 'NO_OVERLAP'
    CONFIG = $${"RUNTIME_MULTIPLIER": {{runtime_multiplier}}}$$
    COMMENT = 'Root task with retries, overlap policy, and a runtime-multiplier config'
    STARTED
AS
    DECLARE
        RUNTIME_MULTIPLIER INTEGER := SYSTEM$GET_TASK_GRAPH_CONFIG('RUNTIME_MULTIPLIER');
        RANDOM_RUNTIME     VARCHAR := DCM_DEMO_4{{env_suffix}}.PIPELINE.RUNTIME_WITH_OUTLIERS(:RUNTIME_MULTIPLIER * 1000);
    BEGIN
        SELECT SYSTEM$WAIT(:RANDOM_RUNTIME, 'MILLISECONDS');
        CALL SYSTEM$SET_RETURN_VALUE('✅ All stage files scanned');
    END;

----------------------------------------------------------------------
-- 2. FINALIZER — runs last, even on failure.
--    Uses the email integration from 01_pre_deploy.sql and the helper
--    functions to build a rich HTML summary of this graph run.
----------------------------------------------------------------------
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_FINALIZER
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    FINALIZE = DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_1
    COMMENT = 'Sends a plain-text JSON email summary after every graph run'
    STARTED
AS
    DECLARE
        MY_ROOT_TASK_ID STRING;
        MY_START_TIME   TIMESTAMP_LTZ;
        SUMMARY_JSON    STRING;
    BEGIN
        MY_ROOT_TASK_ID := (CALL SYSTEM$TASK_RUNTIME_INFO('CURRENT_ROOT_TASK_UUID'));
        MY_START_TIME   := (CALL SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP'));

        SUMMARY_JSON := (SELECT DCM_DEMO_4{{env_suffix}}.PIPELINE.GET_TASK_GRAPH_RUN_SUMMARY(:MY_ROOT_TASK_ID, :MY_START_TIME));

        CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
            SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(:SUMMARY_JSON),
            SNOWFLAKE.NOTIFICATION.EMAIL_INTEGRATION_CONFIG(
                'dcm_demo_email_notifications',
                'DCM Task Graph Run Summary ({{env_suffix}})',
                ARRAY_CONSTRUCT('{{notification_recipient}}'),
                NULL, NULL));

        CALL SYSTEM$SET_RETURN_VALUE('✅ Graph run summary email sent');
    END;

----------------------------------------------------------------------
-- 3. CHILD TASKS — the classic demo set showing dependencies, return
--    values, and different run states.
----------------------------------------------------------------------

-- Successful task with random duration
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_2
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Successful task with random duration'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_1
    STARTED
AS
    DECLARE
        RUNTIME_MULTIPLIER INTEGER := SYSTEM$GET_TASK_GRAPH_CONFIG('RUNTIME_MULTIPLIER');
        RANDOM_RUNTIME     VARCHAR := DCM_DEMO_4{{env_suffix}}.PIPELINE.RUNTIME_WITH_OUTLIERS(:RUNTIME_MULTIPLIER * 3000);
    BEGIN
        SELECT SYSTEM$WAIT(:RANDOM_RUNTIME, 'MILLISECONDS');
        CALL SYSTEM$SET_RETURN_VALUE(:RANDOM_RUNTIME || ' new entries loaded');
    END;

-- Task that calls a stored procedure
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_3
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Successful task that calls DEMO_PROCEDURE_1'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_1
    STARTED
AS
    DECLARE
        RUNTIME_MULTIPLIER INTEGER := SYSTEM$GET_TASK_GRAPH_CONFIG('RUNTIME_MULTIPLIER');
        RANDOM_RUNTIME     VARCHAR := DCM_DEMO_4{{env_suffix}}.PIPELINE.RUNTIME_WITH_OUTLIERS(:RUNTIME_MULTIPLIER * 4000);
    BEGIN
        CALL DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_PROCEDURE_1();
        SELECT SYSTEM$WAIT(:RANDOM_RUNTIME, 'MILLISECONDS');
        CALL SYSTEM$SET_RETURN_VALUE(:RANDOM_RUNTIME || ' new files processed');
    END;

-- Short successful task
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_4
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Successful task with random duration'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_2
    STARTED
AS
    DECLARE
        RUNTIME_MULTIPLIER INTEGER := SYSTEM$GET_TASK_GRAPH_CONFIG('RUNTIME_MULTIPLIER');
        RANDOM_RUNTIME     VARCHAR := DCM_DEMO_4{{env_suffix}}.PIPELINE.RUNTIME_WITH_OUTLIERS(:RUNTIME_MULTIPLIER * 1000);
    BEGIN
        SELECT SYSTEM$WAIT(:RANDOM_RUNTIME, 'MILLISECONDS');
        CALL SYSTEM$SET_RETURN_VALUE('Delay: ' || :RANDOM_RUNTIME || ' milliseconds');
    END;

-- Serverless task (no warehouse) depending on two predecessors
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_5
    COMMENT = 'Serverless task with two predecessors'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_1,
          DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_4
    STARTED
AS
    DECLARE
        RUNTIME_MULTIPLIER INTEGER := SYSTEM$GET_TASK_GRAPH_CONFIG('RUNTIME_MULTIPLIER');
        RANDOM_RUNTIME     VARCHAR := DCM_DEMO_4{{env_suffix}}.PIPELINE.RUNTIME_WITH_OUTLIERS(:RUNTIME_MULTIPLIER * 200);
    BEGIN
        SELECT SYSTEM$WAIT(:RANDOM_RUNTIME, 'MILLISECONDS');
        CALL SYSTEM$SET_RETURN_VALUE('Delay: ' || :RANDOM_RUNTIME || ' milliseconds');
    END;

-- Task that sets a random return value — used as a condition upstream
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_6
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Sets a random return value 1/3'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_3
    STARTED
AS
    DECLARE
        RANDOM_VALUE VARCHAR;
    BEGIN
        RANDOM_VALUE := (SELECT UNIFORM(1, 3, RANDOM()));
        CASE WHEN :RANDOM_VALUE = 1
            THEN CALL SYSTEM$SET_RETURN_VALUE('✅ Quality Check Passed');
        ELSE     CALL SYSTEM$SET_RETURN_VALUE('⚠️ Quality Check Failed');
        END;
    END;

-- Task returning a URL as its return value
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_7
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Returns a URL as its value (click-through in Snowsight)'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_6
    STARTED
AS
    DECLARE
        RUNTIME_MULTIPLIER INTEGER := SYSTEM$GET_TASK_GRAPH_CONFIG('RUNTIME_MULTIPLIER');
        RANDOM_RUNTIME     VARCHAR := DCM_DEMO_4{{env_suffix}}.PIPELINE.RUNTIME_WITH_OUTLIERS(:RUNTIME_MULTIPLIER * 5000);
    BEGIN
        CALL SYSTEM$WAIT(:RANDOM_RUNTIME, 'MILLISECONDS');
        CALL SYSTEM$SET_RETURN_VALUE('https://docs.snowflake.com/en/user-guide/tasks-intro');
    END;

-- Stream-conditional task — skipped when the stream is empty
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_8
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Runs only when DEMO_STREAM has data; otherwise skipped'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_7
    STARTED
    WHEN SYSTEM$STREAM_HAS_DATA('DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_STREAM')
AS
    SELECT SYSTEM$WAIT(4);

-- Fails randomly — demonstrates retry behavior and downstream impact
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_9
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Fails randomly; retries inherited from root, then skips TASK_10 on failure'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_4
    STARTED
AS
    BEGIN
        SELECT SYSTEM$WAIT(3);
        CALL DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_PROCEDURE_2();
    END;

-- Task that does not run when TASK_9 fails
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_10
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Skipped when predecessor task 9 fails'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_9
    STARTED
AS
    DECLARE
        RUNTIME_MULTIPLIER INTEGER := SYSTEM$GET_TASK_GRAPH_CONFIG('RUNTIME_MULTIPLIER');
        RANDOM_RUNTIME     VARCHAR := DCM_DEMO_4{{env_suffix}}.PIPELINE.RUNTIME_WITH_OUTLIERS(:RUNTIME_MULTIPLIER * 2000);
    BEGIN
        SELECT SYSTEM$WAIT(:RANDOM_RUNTIME, 'MILLISECONDS');
        RETURN 'Delay: ' || :RANDOM_RUNTIME || ' milliseconds';
    END;

-- Return-value conditional task — only runs when TASK_6 reports "Passed"
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_11
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Runs only when DEMO_TASK_6 returns the Passed value'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_6
    STARTED
    WHEN SYSTEM$GET_PREDECESSOR_RETURN_VALUE('DEMO_TASK_6') = '✅ Quality Check Passed'
AS
    DECLARE
        RUNTIME_MULTIPLIER INTEGER := SYSTEM$GET_TASK_GRAPH_CONFIG('RUNTIME_MULTIPLIER');
        RANDOM_RUNTIME     VARCHAR := DCM_DEMO_4{{env_suffix}}.PIPELINE.RUNTIME_WITH_OUTLIERS(:RUNTIME_MULTIPLIER * 3000);
    BEGIN
        SELECT SYSTEM$WAIT(:RANDOM_RUNTIME, 'MILLISECONDS');
        RETURN 'Delay: ' || :RANDOM_RUNTIME || ' milliseconds';
    END;

-- Occasionally self-cancels
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_12
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Self-cancels 1/10 runs'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_3
    STARTED
AS
    DECLARE
        RANDOM_VALUE NUMBER(2,0);
    BEGIN
        RANDOM_VALUE := (SELECT UNIFORM(1, 10, RANDOM()));
        IF (:RANDOM_VALUE = 10) THEN
            SELECT SYSTEM$WAIT(12);
            SELECT SYSTEM$USER_TASK_CANCEL_ONGOING_EXECUTIONS('DEMO_TASK_12');
        END IF;
        SELECT SYSTEM$WAIT(2);
    END;

-- Task with two predecessors
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_13
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Successful task with two predecessors'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_12,
          DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_2
    STARTED
AS
    SELECT SYSTEM$WAIT(3);

-- Always SUSPENDED — demonstrates the new target-state property
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_14
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Deployed as SUSPENDED — shows DCM target-state control'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_9
    SUSPENDED
AS
    SELECT SYSTEM$WAIT(3);

-- Never runs because its predecessor is SUSPENDED
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_15
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Never runs because predecessor is SUSPENDED'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_14
    STARTED
AS
    SELECT 1;

----------------------------------------------------------------------
-- 4. QUALITY-GATE BRANCH
--    Load → DMF check → transform OR quarantine → notify
----------------------------------------------------------------------

-- Loads any new rows from the source stream into the landing table
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.LOAD_RAW_DATA
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Load new weather rows into landing table'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_TASK_1
    STARTED
AS
    DECLARE
        ROWS_LOADED NUMBER;
        RESULT_STRING VARCHAR;
    BEGIN
        INSERT INTO DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA
            (ROW_ID, INSERTED, DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F)
        SELECT
            CASE WHEN UNIFORM(1, 10, RANDOM()) = 1
                 THEN 1                    -- occasional duplicate ROW_ID to exercise the quality gate
                 ELSE ROW_ID END,
            CURRENT_TIMESTAMP(), DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F
        FROM DCM_DEMO_4{{env_suffix}}.PIPELINE.WEATHER_DATA_SOURCE
        LIMIT 10;
        ROWS_LOADED := (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        RESULT_STRING := :ROWS_LOADED || ' rows loaded into RAW_WEATHER_DATA';
        CALL SYSTEM$SET_RETURN_VALUE(:RESULT_STRING);
    END;

-- Runs every DMF assigned to the landing table and returns pass/fail
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.CHECK_DATA_QUALITY
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Run all DMFs attached to RAW_WEATHER_DATA'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.LOAD_RAW_DATA
    STARTED
AS
    DECLARE
        TEST_RESULT      NUMBER;
        RESULTS_SUMMARY  NUMBER DEFAULT 0;
        RESULT_STRING    VARCHAR;
        c1 CURSOR FOR
            SELECT DMF, COL FROM TABLE(
                DCM_DEMO_4{{env_suffix}}.PIPELINE.GET_ACTIVE_QUALITY_CHECKS('DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA'));
    BEGIN
        OPEN c1;
        FOR REC IN c1 DO
            EXECUTE IMMEDIATE
                'SELECT ' || REC.DMF || '(SELECT ' || REC.COL ||
                ' FROM DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA);';
            TEST_RESULT := (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
            IF (:TEST_RESULT != 0) THEN
                RESULTS_SUMMARY := (:RESULTS_SUMMARY + :TEST_RESULT);
            END IF;
        END FOR;
        CLOSE c1;
        RESULT_STRING := :RESULTS_SUMMARY || ' separate quality issues found in RAW_WEATHER_DATA';
        CASE WHEN :RESULTS_SUMMARY = 0 THEN
            CALL SYSTEM$SET_RETURN_VALUE('✅ All quality checks on RAW_WEATHER_DATA passed');
        ELSE
            CALL SYSTEM$SET_RETURN_VALUE(:RESULT_STRING);
        END;
    END;

-- Runs when all checks passed — copies rows into the target table
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.TRANSFORM_DATA
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Transform rows that passed quality checks'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.CHECK_DATA_QUALITY
    STARTED
    WHEN SYSTEM$GET_PREDECESSOR_RETURN_VALUE('CHECK_DATA_QUALITY')
         = '✅ All quality checks on RAW_WEATHER_DATA passed'
AS
    BEGIN
        INSERT INTO DCM_DEMO_4{{env_suffix}}.PIPELINE.CLEAN_WEATHER_DATA
            (INSERTED, DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F)
        SELECT INSERTED, DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F
        FROM DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA
        WHERE AVG_TEMP_IN_F > 68;
        DELETE FROM DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA;
    END;

-- Runs when quality check failed — moves bad rows to quarantine
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.ISOLATE_DATA_ISSUES
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Isolate rows that failed quality checks'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.CHECK_DATA_QUALITY
    STARTED
    WHEN SYSTEM$GET_PREDECESSOR_RETURN_VALUE('CHECK_DATA_QUALITY')
         != '✅ All quality checks on RAW_WEATHER_DATA passed'
AS
    BEGIN
        INSERT INTO DCM_DEMO_4{{env_suffix}}.PIPELINE.QUARANTINED_WEATHER_DATA
            (INSERTED, DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F)
        SELECT INSERTED, DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F
        FROM DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA;
        DELETE FROM DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA;
    END;

-- Sends a notification when rows were quarantined
DEFINE TASK DCM_DEMO_4{{env_suffix}}.PIPELINE.NOTIFY_ABOUT_QUALITY_ISSUE
    WAREHOUSE = 'DCM_DEMO_4_WH{{env_suffix}}'
    COMMENT = 'Email when quality issues force data into quarantine'
    AFTER DCM_DEMO_4{{env_suffix}}.PIPELINE.ISOLATE_DATA_ISSUES
    STARTED
AS
    BEGIN
        CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
            SNOWFLAKE.NOTIFICATION.TEXT_HTML(
                '<b>Quality issue detected.</b> Rows were quarantined in DCM_DEMO_4{{env_suffix}}.PIPELINE.QUARANTINED_WEATHER_DATA.'),
            SNOWFLAKE.NOTIFICATION.EMAIL_INTEGRATION_CONFIG(
                'dcm_demo_email_notifications',
                'DCM Pipeline Quality Alert ({{env_suffix}})',
                ARRAY_CONSTRUCT('{{notification_recipient}}'),
                NULL, NULL));
        CALL SYSTEM$SET_RETURN_VALUE('Quality notification sent');
    END;
