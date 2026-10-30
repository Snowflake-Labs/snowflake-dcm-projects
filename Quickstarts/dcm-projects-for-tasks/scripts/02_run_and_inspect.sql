/*=============================================================================
  02_run_and_inspect.sql: run AFTER the first deployment

  Checks the deployed graph, seeds the source table, runs the graph once and
  shows which committed version of the graph the run used. Safe to run as a
  whole file and safe to run again: the seed skips rows that already exist,
  and the graph only runs when there are rows it hasn't loaded yet.
=============================================================================*/

USE ROLE DCM_DEVELOPER;
USE WAREHOUSE DCM_DEMO_4_WH_DEV;

----------------------------------------------------------------------
-- 1. The graph is STARTED and its first version is committed
----------------------------------------------------------------------
SHOW TASK GRAPHS IN SCHEMA DCM_DEMO_4_DEV.PIPELINE;
SELECT "name", "state", "has_pending_changes", "schedule", "task_defaults"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SHOW COMMITTED TASKS IN SCHEMA DCM_DEMO_4_DEV.PIPELINE;
SELECT "name", "version", "predecessors", "warehouse"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY "name";

----------------------------------------------------------------------
-- 2. Seed the source table (skips rows that are already there)
----------------------------------------------------------------------
INSERT INTO DCM_DEMO_4_DEV.PIPELINE.WEATHER_DATA_SOURCE (DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F)
SELECT V.DS, V.ZIPCODE, V.MIN_TEMP_IN_F, V.AVG_TEMP_IN_F, V.MAX_TEMP_IN_F
FROM (VALUES ('2026-06-01'::DATE, '94105', 55, 68, 80), ('2026-06-01'::DATE, '10001', 60, 72, 85), ('2026-06-01'::DATE, '60601', 50, 65, 78),
             ('2026-06-02'::DATE, '94105', 57, 70, 82), ('2026-06-02'::DATE, '10001', 62, 76, 91), ('2026-06-02'::DATE, '60601', 52, 67, 79))
     AS V (DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F)
WHERE NOT EXISTS (SELECT 1 FROM DCM_DEMO_4_DEV.PIPELINE.WEATHER_DATA_SOURCE S
                  WHERE S.DS = V.DS AND S.ZIPCODE = V.ZIPCODE);

----------------------------------------------------------------------
-- 3. Run the graph now instead of waiting for the hourly schedule,
--    but only if some source rows haven't been loaded yet
----------------------------------------------------------------------
EXECUTE IMMEDIATE $$
BEGIN
    IF ((SELECT COUNT(*) FROM DCM_DEMO_4_DEV.PIPELINE.WEATHER_DATA_SOURCE) >
        (SELECT COUNT(*) FROM DCM_DEMO_4_DEV.PIPELINE.RAW_WEATHER_DATA)) THEN
        EXECUTE TASK GRAPH DCM_DEMO_4_DEV.PIPELINE.WEATHER_PIPELINE;
        CALL SYSTEM$WAIT(90);
        RETURN 'Graph run started and given 90 seconds to finish';
    END IF;
    RETURN 'Nothing new to load, graph not run';
END;
$$;

----------------------------------------------------------------------
-- 4. Inspect the run
--
-- GRAPH_ID limits the history to this graph, so runs of an earlier graph
-- with the same name don't show up. Runs where LOAD_WEATHER loaded 0 rows
-- (for example the hourly scheduled run) are left out.
----------------------------------------------------------------------
SHOW TASK GRAPHS IN SCHEMA DCM_DEMO_4_DEV.PIPELINE;
SET GRAPH_ID = (SELECT "id" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

SELECT GRAPH_VERSION, NAME, STATE, RETURN_VALUE, SCHEDULED_TIME
FROM TABLE(DCM_DEMO_4_DEV.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('DAY', -1, CURRENT_TIMESTAMP()),
    GRAPH_ID => $GRAPH_ID,
    RESULT_LIMIT => 10000))
WHERE STATE <> 'SCHEDULED'
QUALIFY COUNT_IF(RETURN_VALUE = '0 rows loaded') OVER (PARTITION BY RUN_ID) = 0
ORDER BY SCHEDULED_TIME, NAME;

SELECT * FROM DCM_DEMO_4_DEV.PIPELINE.DAILY_WEATHER_SUMMARY ORDER BY DS, ZIPCODE;
SELECT * FROM DCM_DEMO_4_DEV.PIPELINE.PIPELINE_RUN_LOG ORDER BY LOGGED_AT;
