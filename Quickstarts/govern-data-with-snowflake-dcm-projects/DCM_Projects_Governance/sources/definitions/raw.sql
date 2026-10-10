/*=============================================================================
  raw.sql — Source customer table containing PII columns to protect.
=============================================================================*/

DEFINE TABLE DCM_DEMO_5{{env_suffix}}.RAW.CUSTOMER (
    CUSTOMER_ID NUMBER,
    FIRST_NAME  VARCHAR,
    LAST_NAME   VARCHAR,
    EMAIL       VARCHAR,
    PHONE       VARCHAR,
    CITY        VARCHAR,
    COUNTRY     VARCHAR
);
