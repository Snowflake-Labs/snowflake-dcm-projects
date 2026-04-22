/*=============================================================================
  functions.sql — SQL and Python helper functions used by the task graph

  These demonstrate DEFINE FUNCTION for:
    - SQL helpers (runtime randomization, task-history summary)
    - A Python function that converts JSON to a styled HTML email body
    - A UDTF that lists currently-assigned DMFs on a table
    - A custom UDMF used by the quality-gate branch
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
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                ROOT_TASK_ID               => MY_ROOT_TASK_ID::STRING,
                SCHEDULED_TIME_RANGE_START => MY_START_TIME,
                SCHEDULED_TIME_RANGE_END   => CURRENT_TIMESTAMP()))
        ORDER BY SCHEDULED_TIME))::STRING
$$;

----------------------------------------------------------------------
-- 3. Turn the JSON summary into a styled HTML table for email
----------------------------------------------------------------------
DEFINE FUNCTION DCM_DEMO_4{{env_suffix}}.PIPELINE.HTML_FROM_JSON_TASK_RUNS(JSON_DATA STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'generate_html_table'
AS
$$
import json

def generate_html_table(json_data):
    column_widths = ["320px", "120px", "400px", "160px", "80px", "480px"]
    headers = ["Task name", "Run Status", "Return Value", "Started", "Duration", "Error Message"]
    rows = json.loads(json_data) if json_data else []

    html = (
        '<p><strong>Task Graph Run Summary</strong><br>'
        'Log in to Snowsight for full run details.</p>'
        '<table border="1" style="border-color:#DEE3EA" cellpadding="5" cellspacing="0">'
        '<thead><tr>'
    )
    for i, header in enumerate(headers):
        html += f'<th scope="col" style="text-align:left; width:{column_widths[i]}">{header}</th>'
    html += '</tr></thead><tbody>'

    for row in rows:
        html += '<tr>'
        for i, header in enumerate(headers):
            key = header.replace(' ', '_').upper()
            cell = row.get(key, '') or ''
            html += f'<td style="text-align:left; width:{column_widths[i]}">{cell}</td>'
        html += '</tr>'
    html += '</tbody></table>'
    return html
$$;

----------------------------------------------------------------------
-- 4. UDTF returning all DMFs currently assigned to a given table.
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
        INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
            REF_ENTITY_NAME   => TABLE_NAME,
            REF_ENTITY_DOMAIN => 'table'
        )) AS t1,
        LATERAL FLATTEN(input => PARSE_JSON(t1.REF_ARGUMENTS)) AS REF
    WHERE SCHEDULE_STATUS = 'STARTED'
$$;

----------------------------------------------------------------------
-- 5. Custom UDMF: flag any Fahrenheit value outside a plausible range.
--    Assigned to RAW_WEATHER_DATA.MAX_TEMP_IN_F in the post-deploy script.
----------------------------------------------------------------------
DEFINE DATA METRIC FUNCTION DCM_DEMO_4{{env_suffix}}.PIPELINE.CHECK_FARENHEIT_PLAUSIBLE(
    TABLE_NAME TABLE(COLUMN_VALUE NUMBER))
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM TABLE_NAME
    WHERE COLUMN_VALUE IS NOT NULL
      AND COLUMN_VALUE NOT BETWEEN -40 AND 140
$$;
