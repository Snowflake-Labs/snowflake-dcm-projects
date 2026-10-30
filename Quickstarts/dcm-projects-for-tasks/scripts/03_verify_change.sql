/*=============================================================================
  03_verify_change.sql: run AFTER the second deployment (the new branch)

  Shows that the graph stayed STARTED, that DCM committed a new version that
  includes the new task, and that the next run used it. Safe to run as a
  whole file and safe to run again, like 02_run_and_inspect.sql.
=============================================================================*/

USE ROLE DCM_DEVELOPER;
USE WAREHOUSE DCM_DEMO_4_WH_DEV;

----------------------------------------------------------------------
-- 1. Still STARTED, nothing waiting to be committed, never suspended
----------------------------------------------------------------------
SHOW TASK GRAPHS IN SCHEMA DCM_DEMO_4_DEV.PIPELINE;
SELECT "name", "state", "has_pending_changes", "last_suspended_on", "task_defaults"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

----------------------------------------------------------------------
-- 2. The committed version now includes BUILD_HEAT_WARNINGS
----------------------------------------------------------------------
SHOW COMMITTED TASKS IN SCHEMA DCM_DEMO_4_DEV.PIPELINE;
SELECT "name", "version", "predecessors", "task_parameters"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY "name";

----------------------------------------------------------------------
-- 3. Add two hot days (skipped if already there) and run the graph again
----------------------------------------------------------------------
INSERT INTO DCM_DEMO_4_DEV.PIPELINE.WEATHER_DATA_SOURCE (DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F)
SELECT V.DS, V.ZIPCODE, V.MIN_TEMP_IN_F, V.AVG_TEMP_IN_F, V.MAX_TEMP_IN_F
FROM (VALUES ('2026-06-03'::DATE, '94105', 61, 75, 88), ('2026-06-03'::DATE, '10001', 64, 79, 93))
     AS V (DS, ZIPCODE, MIN_TEMP_IN_F, AVG_TEMP_IN_F, MAX_TEMP_IN_F)
WHERE NOT EXISTS (SELECT 1 FROM DCM_DEMO_4_DEV.PIPELINE.WEATHER_DATA_SOURCE S
                  WHERE S.DS = V.DS AND S.ZIPCODE = V.ZIPCODE);

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
-- 4. Both runs side by side (runs that loaded 0 rows are left out)
----------------------------------------------------------------------
SHOW TASK GRAPHS IN SCHEMA DCM_DEMO_4_DEV.PIPELINE;
SET GRAPH_ID = (SELECT "id" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

SELECT GRAPH_VERSION, NAME, STATE, RETURN_VALUE
FROM TABLE(DCM_DEMO_4_DEV.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('DAY', -1, CURRENT_TIMESTAMP()),
    GRAPH_ID => $GRAPH_ID,
    RESULT_LIMIT => 10000))
WHERE STATE <> 'SCHEDULED'
QUALIFY COUNT_IF(RETURN_VALUE = '0 rows loaded') OVER (PARTITION BY RUN_ID) = 0
ORDER BY GRAPH_VERSION, SCHEDULED_TIME, NAME;

SELECT * FROM DCM_DEMO_4_DEV.PIPELINE.HEAT_WARNINGS ORDER BY DS, ZIPCODE;
