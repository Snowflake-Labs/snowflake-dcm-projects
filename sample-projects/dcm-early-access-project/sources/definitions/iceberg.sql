define schema DCM_DEMO_1{{env_suffix}}.LAKEHOUSE
	comment = 'Snowflake-managed Iceberg tables (Snowflake catalog and Snowflake storage)'
;


-- Iceberg table with Snowflake as catalog and Snowflake-managed storage (no external volume required)
define iceberg table DCM_DEMO_1{{env_suffix}}.LAKEHOUSE.DAILY_TRUCK_SALES (
	SALE_DATE date not null comment 'Calendar day of the aggregated sales',
	TRUCK_BRAND_NAME string,
	ORDER_COUNT number(38,0) default 0,
	REVENUE number(38,2)
)
	catalog = 'SNOWFLAKE'
	external_volume = 'SNOWFLAKE_MANAGED'
	comment = 'Snowflake-managed Iceberg table'
;

