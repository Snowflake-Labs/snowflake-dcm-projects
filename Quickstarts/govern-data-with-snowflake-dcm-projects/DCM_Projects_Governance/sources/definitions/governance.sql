/*=============================================================================
  governance.sql — Tags and security/governance policies, managed by DCM.

  Demonstrates the DCM governance object types: DEFINE TAG + ATTACH TAG,
  DEFINE MASKING POLICY + ATTACH MASKING POLICY, DEFINE ROW ACCESS POLICY
  + ATTACH ROW ACCESS POLICY, DEFINE NETWORK RULE / NETWORK POLICY, and
  DEFINE AUTHENTICATION POLICY.

  Note (current DCM limits): DCM attaches the masking policy to the EMAIL
  column and the row access policy to the CUSTOMER table. The remaining
  associations are NOT yet DCM-native and are applied in
  scripts/02_post_deploy.sql:
    - associating a masking policy with a tag (tag-based masking)
    - activating the network and authentication policies (ALTER USER)
=============================================================================*/

----------------------------------------------------------------------
-- Tags
----------------------------------------------------------------------
DEFINE TAG DCM_DEMO_5{{env_suffix}}.GOV.PII
    ALLOWED_VALUES 'PII'
    COMMENT = 'Marks columns that contain personally identifiable information';

DEFINE TAG DCM_DEMO_5{{env_suffix}}.GOV.DATA_DOMAIN
    ALLOWED_VALUES 'SALES', 'MARKETING', 'FINANCE', 'HR', 'CUSTOMER'
    COMMENT = 'Business domain that owns the object';

----------------------------------------------------------------------
-- Masking policy (attached to the EMAIL column at the end of this file)
----------------------------------------------------------------------
DEFINE MASKING POLICY DCM_DEMO_5{{env_suffix}}.GOV.EMAIL_MASK
    AS (VAL STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DCM_DEVELOPER') THEN VAL
        ELSE REGEXP_REPLACE(VAL, '.+\\@', '*****@')
    END
    COMMENT = 'Masks the local-part of an email for non-privileged roles';

----------------------------------------------------------------------
-- Network rule + policy (activated on a demo service user in post-deploy)
----------------------------------------------------------------------
DEFINE NETWORK RULE DCM_DEMO_5{{env_suffix}}.GOV.ALLOW_ALL_IPS
    MODE = INGRESS
    TYPE = IPV4
    VALUE_LIST = ('0.0.0.0/0')
    COMMENT = 'Demo-only rule that allows all IPv4 addresses';

DEFINE NETWORK POLICY DCM_DEMO_5{{env_suffix}}_POLICY
    ALLOWED_NETWORK_RULE_LIST = ('DCM_DEMO_5{{env_suffix}}.GOV.ALLOW_ALL_IPS')
    COMMENT = 'Demo-only network policy - activated only on the demo service user';

----------------------------------------------------------------------
-- Authentication policy (PAT settings; set on the demo service user in post-deploy)
----------------------------------------------------------------------
DEFINE AUTHENTICATION POLICY DCM_DEMO_5{{env_suffix}}.GOV.PAT_POLICY
    AUTHENTICATION_METHODS = ('PROGRAMMATIC_ACCESS_TOKEN')
    PAT_POLICY = (
        DEFAULT_EXPIRY_IN_DAYS = 15,
        MAX_EXPIRY_IN_DAYS = 90,
        NETWORK_POLICY_EVALUATION = ENFORCED_NOT_REQUIRED
    )
    COMMENT = 'Demo-only PAT policy - set only on the demo service user';

----------------------------------------------------------------------
-- Attach tags (native DCM ATTACH TAG - reconciled on every deploy)
----------------------------------------------------------------------
ATTACH TAG DCM_DEMO_5{{env_suffix}}.GOV.PII = 'PII'
    TO TABLE DCM_DEMO_5{{env_suffix}}.RAW.CUSTOMER
        COLUMN EMAIL;

ATTACH TAG DCM_DEMO_5{{env_suffix}}.GOV.DATA_DOMAIN = 'CUSTOMER'
    TO TABLE DCM_DEMO_5{{env_suffix}}.RAW.CUSTOMER;

----------------------------------------------------------------------
-- Attach the masking policy to the EMAIL column (native DCM
-- ATTACH MASKING POLICY - applied with FORCE; removing this statement
-- detaches the policy on the next deploy)
----------------------------------------------------------------------
ATTACH MASKING POLICY DCM_DEMO_5{{env_suffix}}.GOV.EMAIL_MASK
    TO TABLE DCM_DEMO_5{{env_suffix}}.RAW.CUSTOMER
        COLUMN EMAIL;

----------------------------------------------------------------------
-- Row access policy: row-level filtering by role
----------------------------------------------------------------------
DEFINE ROW ACCESS POLICY DCM_DEMO_5{{env_suffix}}.GOV.CUSTOMER_COUNTRY_FILTER
    AS (COUNTRY VARCHAR) RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DCM_DEVELOPER') THEN TRUE
        WHEN CURRENT_ROLE() = 'DCM_DEMO_5{{env_suffix}}_ANALYST' AND COUNTRY = 'USA' THEN TRUE
        ELSE FALSE
    END
    COMMENT = 'Privileged roles see all rows; the analyst role sees only USA customers';

----------------------------------------------------------------------
-- Attach the row access policy to the CUSTOMER table (native DCM
-- ATTACH ROW ACCESS POLICY; removing this statement detaches the
-- policy on the next deploy)
----------------------------------------------------------------------
ATTACH ROW ACCESS POLICY DCM_DEMO_5{{env_suffix}}.GOV.CUSTOMER_COUNTRY_FILTER
    TO TABLE DCM_DEMO_5{{env_suffix}}.RAW.CUSTOMER ON (COUNTRY);
