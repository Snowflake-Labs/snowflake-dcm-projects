-- ### Customer segments
--
-- The table the Marketing team's Spark job fills. DCM creates it here, with its
-- columns, so the table is reviewed and versioned like every other object; the
-- job only writes rows into it and never creates or replaces it.
--
-- It is an Apache Iceberg table with Snowflake as the catalog and Snowflake
-- storage (EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'), so other engines can read
-- the segments through the Iceberg REST catalog without a bucket to manage.
-- EXTERNAL_VOLUME is fixed when the table is created.

DEFINE SCHEMA DCM_DEMO_2_MARKETING{{env_suffix}}.SEGMENTS
    COMMENT = 'Customer segments built by the Marketing Spark job';

DEFINE ICEBERG TABLE DCM_DEMO_2_MARKETING{{env_suffix}}.SEGMENTS.CUSTOMER_SEGMENTS (
    C_ID              NUMBER(38,0)     COMMENT 'Customer identifier',
    CUSTOMER_NAME     VARCHAR          COMMENT 'First and last name',
    TRADE_COUNT       NUMBER(18,0)     COMMENT 'Completed trades across all of the customer''s accounts',
    TOTAL_VALUE       NUMBER(18,2)     COMMENT 'Quantity times trade price, summed',
    ACTIVITY_TIER     VARCHAR          COMMENT 'ACTIVE, OCCASIONAL or DORMANT',
    PREFERRED_CHANNEL VARCHAR          COMMENT 'Synthetic marketing channel, generated with Faker',
    CONTACT_EMAIL     VARCHAR          COMMENT 'Synthetic contact email, generated with Faker',
    SEGMENTED_AT      TIMESTAMP_NTZ(6) COMMENT 'When the Spark job wrote this row'
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
    COMMENT = 'One row per customer, rewritten on every run of the Marketing pipeline';
