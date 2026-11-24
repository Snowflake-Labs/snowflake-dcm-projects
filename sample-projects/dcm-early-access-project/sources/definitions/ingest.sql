-- internal stage
define stage DCM_DEMO_1{{env_suffix}}.RAW.TASTY_BYTES_ORDERS_STAGE 
    directory = ( enable = true )
    comment = 'Internal stage for daily incoming Tasty Bytes order files (CSV)'
;

-- internal stage populated during deployment
define stage DCM_DEMO_1{{env_suffix}}.RAW.SAMPLE_DATA_SCRIPTS
        from 'asset://sample_data_scripts/'
        comment = 'Stage populated from the sample_data_scripts project asset'
;

-- external stage
define stage DCM_DEMO_1{{env_suffix}}.RAW.PUBLIC_S3_BUCKET
    URL = 's3://sfquickstarts/'
    file_format = 'DCM_DEMO_1{{env_suffix}}.RAW.DCM_DEMO_CSV'
    comment = 'For demo purposes only. no imports from here.'      
;


define table DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING (
    ORDER_ID NUMBER,
    CUSTOMER_ID NUMBER,
    TRUCK_ID NUMBER,
    ORDER_TS TIMESTAMP_NTZ,
    MENU_ITEM_ID NUMBER,
    QUANTITY NUMBER
)
change_tracking = TRUE
;


define procedure DCM_DEMO_1{{env_suffix}}.RAW.SP_UPSERT_FROM_INCOMING(
    TARGET_TABLE STRING,
    COLUMN_NAMES STRING,
    DEDUP_KEY STRING DEFAULT NULL
)
returns STRING
language SQL
as
begin
    let stmt STRING;
    if (:DEDUP_KEY is not null) then
        stmt := 'INSERT INTO ' || :TARGET_TABLE || ' (' || :COLUMN_NAMES || ')' ||
                ' SELECT DISTINCT ' || :COLUMN_NAMES ||
                ' FROM DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING src' ||
                ' WHERE NOT EXISTS (SELECT 1 FROM ' || :TARGET_TABLE || ' dest' ||
                ' WHERE dest.' || :DEDUP_KEY || ' = src.' || :DEDUP_KEY || ')';
    else
        stmt := 'INSERT INTO ' || :TARGET_TABLE || ' (' || :COLUMN_NAMES || ')' ||
                ' SELECT ' || :COLUMN_NAMES ||
                ' FROM DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING';
    end if;
    execute immediate :stmt;
    return 'Inserted into ' || :TARGET_TABLE;
end;


define file format DCM_DEMO_1{{env_suffix}}.RAW.DCM_DEMO_CSV
    TYPE = 'csv'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = true
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
;


define table DCM_DEMO_1{{env_suffix}}.RAW.PIPE_ORDERS (
	ORDER_ID number,
	CUSTOMER_ID number,
	TRUCK_ID number,
	ORDER_TS timestamp_ntz,
	MENU_ITEM_ID number,
	QUANTITY number
)
;


define pipe DCM_DEMO_1{{env_suffix}}.RAW.ORDERS_PIPE
	auto_ingest = false
as
	copy into DCM_DEMO_1{{env_suffix}}.RAW.PIPE_ORDERS
	from @DCM_DEMO_1{{env_suffix}}.RAW.PUBLIC_S3_BUCKET/dcm_sample_orders/
	file_format = (format_name = 'DCM_DEMO_1{{env_suffix}}.RAW.DCM_DEMO_CSV')
;


define stream DCM_DEMO_1{{env_suffix}}.RAW.INCOMING_ORDERS_STREAM
	on table DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING
;


define view DCM_DEMO_1{{env_suffix}}.RAW.V_CUSTOMER_CHANGES
	change_tracking = true
as
select
	CUSTOMER_ID,
	CITY
from
	DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER
;


define stream DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER_VIEW_STREAM
	on view DCM_DEMO_1{{env_suffix}}.RAW.V_CUSTOMER_CHANGES
;


define stream DCM_DEMO_1{{env_suffix}}.RAW.ORDER_FILES_STREAM
	on stage DCM_DEMO_1{{env_suffix}}.RAW.TASTY_BYTES_ORDERS_STAGE
;


define stream DCM_DEMO_1{{env_suffix}}.RAW.NEW_ORDERS_STREAM
    on table DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER
    append_only = TRUE
    comment = 'Captures new order records appended to ORDER_HEADER for CDC-based downstream processing'
;
