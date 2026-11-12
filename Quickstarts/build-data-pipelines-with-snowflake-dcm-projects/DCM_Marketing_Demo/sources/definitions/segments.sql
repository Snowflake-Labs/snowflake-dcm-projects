-- ### Customer segments
--
-- The table the Marketing team's Spark job fills. DCM creates it here, with its
-- columns, so the table is reviewed and versioned like every other object; the
-- job only writes rows into it and never creates or replaces it.

DEFINE SCHEMA DCM_DEMO_2_MARKETING{{env_suffix}}.SEGMENTS
    COMMENT = 'Customer segments built by the Marketing Spark job';

DEFINE TABLE DCM_DEMO_2_MARKETING{{env_suffix}}.SEGMENTS.CUSTOMER_SEGMENTS (
    C_ID           NUMBER(38,0)     COMMENT 'Customer identifier',
    CUSTOMER_NAME  VARCHAR(50)      COMMENT 'First and last name',
    TRADE_COUNT    NUMBER(18,0)     COMMENT 'Completed trades across all of the customer''s accounts',
    TOTAL_VALUE    NUMBER(18,2)     COMMENT 'Quantity times trade price, summed',
    ACTIVITY_TIER  VARCHAR(10)      COMMENT 'ACTIVE, OCCASIONAL or DORMANT',
    SEGMENTED_AT   TIMESTAMP_NTZ(9) COMMENT 'When the Spark job wrote this row'
)
COMMENT = 'One row per customer, rewritten on every run of the Marketing pipeline';
