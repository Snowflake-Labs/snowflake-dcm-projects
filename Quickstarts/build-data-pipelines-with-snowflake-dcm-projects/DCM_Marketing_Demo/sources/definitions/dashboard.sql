-- ### Marketing dashboard
--
-- A Streamlit app over CUSTOMER_SEGMENTS. The app files ship with the project
-- as the segments_dashboard asset, so the dashboard is versioned and deployed
-- with the table it reads. Jinja is not rendered into the Python file; the app
-- finds its database with CURRENT_DATABASE().

DEFINE STREAMLIT DCM_DEMO_2_MARKETING{{env_suffix}}.SEGMENTS.SEGMENTS_DASHBOARD
    FROM 'asset://segments_dashboard/'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = DCM_DEMO_2_MARKETING_WH{{env_suffix}}
    RUNTIME_NAME = 'SYSTEM$WAREHOUSE_RUNTIME'
    TITLE = 'Customer segments'
    COMMENT = 'Customer segments by activity tier and channel, for the Marketing team';
