-- ### Alert on failed expectations
--
-- Snowflake ships alert templates for common monitoring jobs. This one,
-- DQ_EXPECTATION_VIOLATIONS, checks for failed expectations on a schedule and
-- notifies when it finds one. FROM TEMPLATE generates the condition and the
-- action; TEMPLATE_PARAMS only says what to watch and how to notify.
--
-- Scope is the SILVER schema, where the expectations live. IN_APP sends the
-- notification to the Snowsight user who deployed the alert, so no
-- notification integration is needed.
--
-- A template alert deploys suspended. The post-deploy script resumes it.

DEFINE ALERT DCM_DEMO_2_FINANCE{{env_suffix}}.SILVER.EXPECTATION_VIOLATIONS
    FROM TEMPLATE DQ_EXPECTATION_VIOLATIONS
    WAREHOUSE = DCM_DEMO_2_FINANCE_WH{{env_suffix}}
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Notifies when an expectation on the silver layer fails'
    TEMPLATE_PARAMS = '{"template_variables": {
        "SCOPE_ACTIVE": "SCHEMA",
        "SCOPE_DATABASE": "DCM_DEMO_2_FINANCE{{env_suffix}}",
        "SCOPE_SCHEMA": "SILVER",
        "NOTIFICATION_MODE": "IN_APP"}}';
