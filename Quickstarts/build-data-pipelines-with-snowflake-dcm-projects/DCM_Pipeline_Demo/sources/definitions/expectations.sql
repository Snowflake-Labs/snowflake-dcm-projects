-- ----------------------------------------------------------------------------
-- FACT_PROSPECT - Marketing prospect fact table
-- ----------------------------------------------------------------------------

-- Primary key must not be null
attach data metric function SNOWFLAKE.CORE.NULL_COUNT
    to table DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.FACT_PROSPECT
        on (AGENCY_ID)
        expectation NO_MISSING_ID (value = 0);

-- Age must be reasonable (not deceased)
attach data metric function SNOWFLAKE.CORE.MAX
    to table DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.FACT_PROSPECT
        on (AGE)
        expectation NO_DEAD_PROSPECTS (value < 120);

-- Age must be adult (18+)
attach data metric function SNOWFLAKE.CORE.MIN
    to table DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.FACT_PROSPECT
        on (AGE)
        expectation NO_KIDS (value > 18);