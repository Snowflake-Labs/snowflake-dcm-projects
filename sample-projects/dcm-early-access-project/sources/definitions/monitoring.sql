define alert DCM_DEMO_1{{env_suffix}}.SERVE.LOW_INVENTORY
warehouse = 'DCM_DEMO_1_WH{{env_suffix}}'
schedule = 'USING CRON 0 9 * * * UTC'
SUSPENDED
if (exists (
    select 1 
    from 
        DCM_DEMO_1{{env_suffix}}.RAW.INVENTORY
    where 
        IN_STOCK < 10 
        and COUNTED_ON >= CURRENT_DATE() - 1
))
then
    call SYSTEM$SEND_EMAIL(
        'dcm_demo_notification',
        'your_registered_user@company.com',
        'DCM Alert: Low Inventory Detected',
        'One or more items have inventory below threshold. Please review the INVENTORY table.'
    );


define schema DCM_DEMO_1{{env_suffix}}.OBSERV;

-- advanced alerts to log events to the event table

define alert DCM_DEMO_1{{env_suffix}}.OBSERV.FAILED_TASKS
warehouse = 'DCM_DEMO_1_WH{{env_suffix}}'
schedule = 'USING CRON 0 8 * * * UTC'
comment = 'Detect failed Task runs in this DCM project and write an error log to the account event table'
SUSPENDED
if (exists (
	select
		NAME,
		SCHEMA_NAME,
		DATABASE_NAME,
		SCHEDULED_TIME,
		ERROR_MESSAGE
	from
		table(DCM_DEMO_1{{env_suffix}}.INFORMATION_SCHEMA.TASK_HISTORY(
			SCHEDULED_TIME_RANGE_START => coalesce(
				SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(),
				timeadd('day', -1, current_timestamp)
			),
			SCHEDULED_TIME_RANGE_END => SNOWFLAKE.ALERT.SCHEDULED_TIME(),
			RESULT_LIMIT => 10000
		))
	where
		SCHEMA_NAME is not null
		and STATE = 'FAILED'
))
then
	begin
		let FAILED_RUNS resultset := (
			select * from table(result_scan(SNOWFLAKE.ALERT.GET_CONDITION_QUERY_UUID()))
		);
		for TASK_RUN in FAILED_RUNS do
			call SYSTEM$LOG(
				'ERROR',
				'DCM | TASK FAILED | '
					|| TASK_RUN.DATABASE_NAME || '.' || TASK_RUN.SCHEMA_NAME || '.' || TASK_RUN.NAME
					|| ' | scheduled: ' || to_varchar(TASK_RUN.SCHEDULED_TIME)
					|| ' | ' || coalesce(TASK_RUN.ERROR_MESSAGE, 'no error message')
			);
		end for;
	end;


define alert DCM_DEMO_1{{env_suffix}}.OBSERV.DT_LAG_EXCEEDED
warehouse = 'DCM_DEMO_1_WH{{env_suffix}}'
schedule = 'USING CRON 0 8 * * * UTC'
comment = 'Detect Dynamic Tables running more than 50% above their target lag and write a warn log to the account event table'
SUSPENDED
if (exists (
	with
	DT_INFO as (
		select
			NAME,
			SCHEMA_NAME,
			DATABASE_NAME,
			DATA_TIMESTAMP,
			TARGET_LAG,
			timediff(seconds, DATA_TIMESTAMP, SNOWFLAKE.ALERT.SCHEDULED_TIME()) as ACTUAL_LAG_SECONDS,
			case
				when TARGET_LAG ilike '% second%' then to_number(split_part(TARGET_LAG, ' ', 1))
				when TARGET_LAG ilike '% minute%' then to_number(split_part(TARGET_LAG, ' ', 1)) * 60
				when TARGET_LAG ilike '% hour%'   then to_number(split_part(TARGET_LAG, ' ', 1)) * 3600
				when TARGET_LAG ilike '% day%'    then to_number(split_part(TARGET_LAG, ' ', 1)) * 86400
			end as TARGET_LAG_SECONDS
		from
			DCM_DEMO_1{{env_suffix}}.INFORMATION_SCHEMA.DYNAMIC_TABLES
		where
			TARGET_LAG != 'DOWNSTREAM'
	)
	select
		NAME,
		SCHEMA_NAME,
		DATABASE_NAME,
		DATA_TIMESTAMP,
		TARGET_LAG,
		ACTUAL_LAG_SECONDS,
		TARGET_LAG_SECONDS,
		round(ACTUAL_LAG_SECONDS / TARGET_LAG_SECONDS, 2) as LAG_RATIO
	from
		DT_INFO
	where
		ACTUAL_LAG_SECONDS > TARGET_LAG_SECONDS * 1.5
))
then
	begin
		let LAG_VIOLATIONS resultset := (
			select * from table(result_scan(SNOWFLAKE.ALERT.GET_CONDITION_QUERY_UUID()))
		);
		for DT in LAG_VIOLATIONS do
			call SYSTEM$LOG(
				'WARN',
				'DCM | DT LAG EXCEEDED | '
					|| DT.DATABASE_NAME || '.' || DT.SCHEMA_NAME || '.' || DT.NAME
					|| ' | actual: ' || to_varchar(DT.ACTUAL_LAG_SECONDS) || 's'
					|| ' | target: ' || DT.TARGET_LAG || ' (' || to_varchar(DT.TARGET_LAG_SECONDS) || 's)'
					|| ' | ratio: ' || to_varchar(DT.LAG_RATIO) || 'x'
			);
		end for;
	end;


define alert DCM_DEMO_1{{env_suffix}}.OBSERV.EXPECTATION_VIOLATIONS
warehouse = 'DCM_DEMO_1_WH{{env_suffix}}'
schedule = 'USING CRON 0 8 * * * UTC'
comment = 'Detect new, worsening, and resolved DMF expectation results and write logs to the account event table'
SUSPENDED
if (exists (
	select
		*
	from
		SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
	where
		upper(TABLE_DATABASE) = upper('DCM_DEMO_1{{env_suffix}}')
))
then
	begin
		let NEW_QUALITY_CHECKS resultset := (
			with
			PREV_RECORDS as (
				select
					MEASUREMENT_TIME,
					METRIC_NAME,
					TABLE_NAME,
					TABLE_SCHEMA,
					TABLE_DATABASE,
					ARGUMENT_NAMES[0]::string as COLUMN_NAME,
					VALUE,
					lag(VALUE, 1) over (partition by METRIC_NAME, TABLE_NAME, TABLE_SCHEMA, COLUMN_NAME order by MEASUREMENT_TIME) as PREV_VALUE
				from
					SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
				where
					MEASUREMENT_TIME > timeadd('day', -7, current_timestamp)
					and upper(TABLE_DATABASE) = upper('DCM_DEMO_1{{env_suffix}}')
			)
			select
				to_char(convert_timezone('Europe/Berlin', C.MEASUREMENT_TIME), 'YYYY-MM-DD at HH:MI:SS') as MEASUREMENT_TIME,
				case
					when C.METRIC_DATABASE = 'SNOWFLAKE' then concat('❄️ ', C.METRIC_NAME)
					else concat('💼 ', C.METRIC_NAME)
				end as DMF_NAME,
				C.METRIC_NAME,
				C.TABLE_NAME,
				C.TABLE_SCHEMA,
				C.ARGUMENT_NAMES[0]::string as COLUMN_NAME,
				P.VALUE,
				P.PREV_VALUE,
				case
					when (P.VALUE > 0 and (P.PREV_VALUE = 0 or P.PREV_VALUE is null)) then 'NEW'
					when (P.VALUE > P.PREV_VALUE and P.PREV_VALUE > 0)                then 'WORSE'
					when (P.VALUE = P.PREV_VALUE and P.PREV_VALUE > 0)                then 'SAME'
					when (P.VALUE < P.PREV_VALUE and P.VALUE > 0)                     then 'BETTER'
					when (P.VALUE = 0 and P.VALUE < P.PREV_VALUE)                     then 'RESOLVED'
				end as VALUE_CHANGE
			from
				table(result_scan(SNOWFLAKE.ALERT.GET_CONDITION_QUERY_UUID())) C
			left join
				PREV_RECORDS P
				on C.METRIC_NAME = P.METRIC_NAME
				and C.MEASUREMENT_TIME = P.MEASUREMENT_TIME
				and C.TABLE_NAME = P.TABLE_NAME
				and C.TABLE_SCHEMA = P.TABLE_SCHEMA
				and C.ARGUMENT_NAMES[0] = P.COLUMN_NAME
				and C.TABLE_DATABASE = P.TABLE_DATABASE
			having
				VALUE_CHANGE in ('NEW', 'WORSE', 'RESOLVED')
			order by
				MEASUREMENT_TIME desc
		);

    for RECORD in NEW_QUALITY_CHECKS do
      case (RECORD.VALUE_CHANGE)
        when 'NEW' then
          let MESSAGE string := ('Table ' || RECORD.TABLE_NAME || ' in ' || RECORD.TABLE_SCHEMA || ' started failing quality expectation ' || RECORD.DMF_NAME || ' for column ' || RECORD.COLUMN_NAME || '.');
          call SYSTEM$LOG('ERROR', '{"state":"NEW_QUALITY_ISSUE", "message":"' || :MESSAGE || '"}');
        when 'WORSE' then
          let MESSAGE string := ('Table ' || RECORD.TABLE_NAME || ' in ' || RECORD.TABLE_SCHEMA || ' still fails quality expectation ' || RECORD.DMF_NAME || ' for column ' || RECORD.COLUMN_NAME || '.');
          call SYSTEM$LOG('WARN', '{"state":"GROWING_QUALITY_ISSUE", "message":"' || :MESSAGE || '"}');
        when 'RESOLVED' then
          let MESSAGE string := ('Table ' || RECORD.TABLE_NAME || ' in ' || RECORD.TABLE_SCHEMA || ' now meets quality expectation again for ' || RECORD.DMF_NAME || ' for column ' || RECORD.COLUMN_NAME || '.');
          call SYSTEM$LOG('INFO', '{"state":"RESOLVED_QUALITY_ISSUE", "message":"' || :MESSAGE || '"}');
      end case;
    end for;
	end;


define alert DCM_DEMO_1{{env_suffix}}.OBSERV.TASK_DURATION_ANOMALY
warehouse = 'DCM_DEMO_1_WH{{env_suffix}}'
schedule = 'USING CRON 0 8 * * * UTC'
comment = 'Detect Task run duration outliers using Z-score over previous 50 runs and write a warn log to the account event table'
SUSPENDED
if (exists (
	with
	RUN_HISTORY as (
		select
			NAME,
			SCHEMA_NAME,
			DATABASE_NAME,
			SCHEDULED_TIME,
			timediff(seconds, QUERY_START_TIME, COMPLETED_TIME) as DURATION,
			avg(DURATION) over (partition by SCHEMA_NAME, NAME order by SCHEDULED_TIME rows between 50 preceding and 1 preceding) as PREV_50_AVG,
			stddev(DURATION) over (partition by SCHEMA_NAME, NAME order by SCHEDULED_TIME rows between 50 preceding and 1 preceding) as PREV_50_STDDEV,
			abs(DURATION - PREV_50_AVG) / nullif(PREV_50_STDDEV, 0) as Z_SCORE
		from
			table(DCM_DEMO_1{{env_suffix}}.INFORMATION_SCHEMA.TASK_HISTORY(
				SCHEDULED_TIME_RANGE_START => timeadd('day', -7, SNOWFLAKE.ALERT.SCHEDULED_TIME()),
				SCHEDULED_TIME_RANGE_END => SNOWFLAKE.ALERT.SCHEDULED_TIME(),
				RESULT_LIMIT => 10000
			))
		where
			SCHEMA_NAME is not null
			and STATE = 'SUCCEEDED'
	)
	select
		NAME as TASK_NAME,
		DATABASE_NAME,
		SCHEMA_NAME,
		SCHEDULED_TIME,
		DURATION as DURATION_IN_S,
		round(PREV_50_AVG, 1) as PREV_50_AVG_S,
		round(Z_SCORE, 1) as Z_SCORE
	from
		RUN_HISTORY
	where
		Z_SCORE > 3
		and SCHEDULED_TIME >= coalesce(
			SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(),
			timeadd('day', -1, SNOWFLAKE.ALERT.SCHEDULED_TIME())
		)
))
then
	begin
		let DURATION_ANOMALIES resultset := (
			select * from table(result_scan(SNOWFLAKE.ALERT.GET_CONDITION_QUERY_UUID()))
		);
		for TASK_RUN in DURATION_ANOMALIES do
			call SYSTEM$LOG(
				'WARN',
				'DCM | TASK DURATION ANOMALY | '
					|| TASK_RUN.DATABASE_NAME || '.' || TASK_RUN.SCHEMA_NAME || '.' || TASK_RUN.TASK_NAME
					|| ' | duration: ' || to_varchar(TASK_RUN.DURATION_IN_S) || 's'
					|| ' | avg (prev 50): ' || to_varchar(TASK_RUN.PREV_50_AVG_S) || 's'
					|| ' | z-score: ' || to_varchar(TASK_RUN.Z_SCORE)
			);
		end for;
	end;
;
