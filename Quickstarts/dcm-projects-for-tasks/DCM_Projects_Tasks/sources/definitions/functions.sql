/*=============================================================================
  functions.sql — SQL helper functions used by the task graph

  These demonstrate DEFINE FUNCTION for:
    - SQL helpers (runtime randomization, task-history summary)
    - A UDTF that lists currently-assigned DMFs on a table

  Note: INFORMATION_SCHEMA table-functions must be fully qualified with the
  database name so the DCM planner can resolve them statically.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Runtime randomizer — used by every demo task to simulate load
----------------------------------------------------------------------
DEFINE FUNCTION DCM_DEMO_4{{env_suffix}}.PIPELINE.RUNTIME_WITH_OUTLIERS(REGULAR_RUNTIME NUMBER(6,0))
RETURNS NUMBER(6,0)
LANGUAGE SQL
COMMENT = 'Input and output in milliseconds; 1/10 runs are 2x as long (outliers)'
AS
$$
    SELECT CASE
        WHEN UNIFORM(1, 10, RANDOM()) = 10
            THEN CAST((REGULAR_RUNTIME * 2 + (UNIFORM(-10, 10, RANDOM()))/100 * REGULAR_RUNTIME) AS NUMBER(6,0))
        ELSE     CAST((REGULAR_RUNTIME     + (UNIFORM(-10, 10, RANDOM()))/100 * REGULAR_RUNTIME) AS NUMBER(6,0))
    END
$$;

----------------------------------------------------------------------
-- 2. Summarize a task graph run as a JSON array of task rows
--    Called by the finalizer to build the email body.
----------------------------------------------------------------------
DEFINE FUNCTION DCM_DEMO_4{{env_suffix}}.PIPELINE.GET_TASK_GRAPH_RUN_SUMMARY(
    MY_ROOT_TASK_ID STRING, MY_START_TIME TIMESTAMP_LTZ)
RETURNS STRING
LANGUAGE SQL
AS
$$
    (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
            'TASK_NAME',     NAME,
            'RUN_STATUS',    STATE,
            'RETURN_VALUE',  RETURN_VALUE,
            'STARTED',       QUERY_START_TIME,
            'DURATION',      DURATION,
            'ERROR_MESSAGE', ERROR_MESSAGE
        )) AS GRAPH_RUN_SUMMARY
    FROM (
        SELECT
            NAME,
            CASE WHEN STATE = 'SUCCEEDED' THEN '🟢 SUCCEEDED'
                 WHEN STATE = 'FAILED'    THEN '🔴 FAILED'
                 WHEN STATE = 'SKIPPED'   THEN '🔵 SKIPPED'
                 WHEN STATE = 'CANCELLED' THEN '🔘 CANCELLED'
            END AS STATE,
            RETURN_VALUE,
            TO_VARCHAR(QUERY_START_TIME, 'YYYY-MM-DD HH24:MI:SS') AS QUERY_START_TIME,
            CONCAT(TIMESTAMPDIFF('seconds', QUERY_START_TIME, COMPLETED_TIME), ' s') AS DURATION,
            ERROR_MESSAGE
        FROM TABLE(DCM_DEMO_4{{env_suffix}}.INFORMATION_SCHEMA.TASK_HISTORY(
                ROOT_TASK_ID               => MY_ROOT_TASK_ID::STRING,
                SCHEDULED_TIME_RANGE_START => MY_START_TIME,
                SCHEDULED_TIME_RANGE_END   => CURRENT_TIMESTAMP()))
        ORDER BY SCHEDULED_TIME))::STRING
$$;

----------------------------------------------------------------------
-- 3. UDTF returning all DMFs currently assigned to a given table.
--    Used by the CHECK_DATA_QUALITY task to iterate through checks.
----------------------------------------------------------------------
DEFINE FUNCTION DCM_DEMO_4{{env_suffix}}.PIPELINE.GET_ACTIVE_QUALITY_CHECKS("TABLE_NAME" VARCHAR)
RETURNS TABLE(DMF VARCHAR, COL VARCHAR)
LANGUAGE SQL
AS
$$
    SELECT
        t1.METRIC_DATABASE_NAME || '.' || METRIC_SCHEMA_NAME || '.' || METRIC_NAME AS DMF,
        REF.value:name::STRING AS COL
    FROM TABLE(
        DCM_DEMO_4{{env_suffix}}.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
            REF_ENTITY_NAME   => TABLE_NAME,
            REF_ENTITY_DOMAIN => 'table'
        )) AS t1,
        LATERAL FLATTEN(input => PARSE_JSON(t1.REF_ARGUMENTS)) AS REF
    WHERE SCHEDULE_STATUS = 'STARTED'
$$;

----------------------------------------------------------------------
-- 4. Python function: render the JSON run summary as an HTML table.
--    Uses DCM Python DEFINE FUNCTION so the finalizer can email a
--    formatted table instead of a raw JSON string.
----------------------------------------------------------------------
DEFINE FUNCTION DCM_DEMO_4{{env_suffix}}.PIPELINE.HTML_FROM_JSON_TASK_RUNS(JSON_DATA STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'generate_html_table'
COMMENT = 'Converts the GET_TASK_GRAPH_RUN_SUMMARY JSON into an HTML table for email'
AS
$$
import json

def generate_html_table(json_data):
    if not json_data:
        return '<p>No task runs found for this graph run.</p>'

    rows = json.loads(json_data)
    headers = [
        ('TASK_NAME', 'Task'),
        ('RUN_STATUS', 'Status'),
        ('STARTED', 'Started'),
        ('DURATION', 'Duration'),
        ('RETURN_VALUE', 'Return Value'),
        ('ERROR_MESSAGE', 'Error'),
    ]

    def esc(v):
        if v is None:
            return ''
        return str(v).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    head = ''.join(
        f'<th style="text-align:left;padding:6px 10px;border:1px solid #dee3ea;">{label}</th>'
        for _, label in headers
    )
    body = ''
    for row in rows:
        cells = ''.join(
            f'<td style="padding:6px 10px;border:1px solid #dee3ea;vertical-align:top;">{esc(row.get(key))}</td>'
            for key, _ in headers
        )
        body += f'<tr>{cells}</tr>'

    return (
        '<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#1b2a3a;">'
        '<h2 style="color:#29b5e8;margin:0 0 4px;">Task Graph Run Summary</h2>'
        '<p style="margin:0 0 12px;">Log in to Snowsight to see full run details.</p>'
        '<table style="border-collapse:collapse;border:1px solid #dee3ea;">'
        f'<thead><tr style="background:#f5f7fa;">{head}</tr></thead>'
        f'<tbody>{body}</tbody>'
        '</table></div>'
    )
$$;
