/*=============================================================================
  expectations.sql — DMF attachments to the landing table

  Uses the native DCM `ATTACH DATA METRIC FUNCTION` statement so the entire
  quality gate — DMF definitions and their column attachments — is managed
  by DCM Plan & Deploy. Adding or removing an attachment here changes the
  CHECK_DATA_QUALITY task's behavior on the next deploy, with no manual
  ALTER TABLE statements needed.

  Note: DMFs will only run if the landing table is created with
  DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES' (see tables.sql).
=============================================================================*/

--- System DMFs attached to landing-table columns
ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT
    TO TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA
    ON (ROW_ID)
    EXPECTATION NO_DUPLICATE_ROW_IDS (value = 0);

ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
    TO TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA
    ON (DS)
    EXPECTATION NO_NULL_DATES (value = 0);

ATTACH DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
    TO TABLE DCM_DEMO_4{{env_suffix}}.PIPELINE.RAW_WEATHER_DATA
    ON (ZIPCODE)
    EXPECTATION NO_NULL_ZIPCODES (value = 0);

