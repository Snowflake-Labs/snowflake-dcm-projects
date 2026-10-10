/*=============================================================================
  governance.sql — Tags and security/governance policies, managed by DCM.

  Demonstrates the DCM governance object types: DEFINE TAG + ATTACH TAG,
  DEFINE MASKING POLICY, DEFINE NETWORK RULE / NETWORK POLICY, and
  DEFINE AUTHENTICATION POLICY.

  Note (current DCM limits): attaching a masking policy to a column and
  associating a masking policy with a tag are NOT yet DCM-native, so the
  tag->policy association is done in scripts/02_post_deploy.sql. The network
  and authentication policies are DEFINED here but never activated.
=============================================================================*/

----------------------------------------------------------------------
-- Tags
----------------------------------------------------------------------
DEFINE TAG DCM_DEMO_5{{env_suffix}}.GOV.PII
    ALLOWED_VALUES 'PII';

DEFINE TAG DCM_DEMO_5{{env_suffix}}.GOV.DATA_DOMAIN
    ALLOWED_VALUES 'SALES', 'MARKETING', 'FINANCE', 'HR', 'CUSTOMER';

----------------------------------------------------------------------
-- Masking policy (defined here; associated with the PII tag in post-deploy)
----------------------------------------------------------------------
DEFINE MASKING POLICY DCM_DEMO_5{{env_suffix}}.GOV.EMAIL_MASK
    AS (VAL STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DCM_DEVELOPER') THEN VAL
        ELSE REGEXP_REPLACE(VAL, '.+\\@', '*****@')
    END
    COMMENT = 'Masks the local-part of an email for non-privileged roles';

----------------------------------------------------------------------
-- Network rule + policy (created only; NEVER activated in this demo)
----------------------------------------------------------------------
DEFINE NETWORK RULE DCM_DEMO_5{{env_suffix}}.GOV.ALLOW_ALL_IPS
    MODE = INGRESS
    TYPE = IPV4
    VALUE_LIST = ('0.0.0.0/0');

DEFINE NETWORK POLICY DCM_DEMO_5{{env_suffix}}_POLICY
    ALLOWED_NETWORK_RULE_LIST = ('DCM_DEMO_5{{env_suffix}}.GOV.ALLOW_ALL_IPS')
    COMMENT = 'Demo-only network policy - defined but never activated';

----------------------------------------------------------------------
-- Authentication policy (PAT settings; defined, not attached to a user)
----------------------------------------------------------------------
DEFINE AUTHENTICATION POLICY DCM_DEMO_5{{env_suffix}}.GOV.PAT_POLICY
    AUTHENTICATION_METHODS = ('PROGRAMMATIC_ACCESS_TOKEN')
    PAT_POLICY = (
        DEFAULT_EXPIRY_IN_DAYS = 15,
        MAX_EXPIRY_IN_DAYS = 90,
        NETWORK_POLICY_EVALUATION = ENFORCED_NOT_REQUIRED
    );

----------------------------------------------------------------------
-- Attach tags (native DCM ATTACH TAG - reconciled on every deploy)
----------------------------------------------------------------------
ATTACH TAG DCM_DEMO_5{{env_suffix}}.GOV.PII = 'PII'
    TO TABLE DCM_DEMO_5{{env_suffix}}.RAW.CUSTOMER
        COLUMN EMAIL;

ATTACH TAG DCM_DEMO_5{{env_suffix}}.GOV.DATA_DOMAIN = 'CUSTOMER'
    TO TABLE DCM_DEMO_5{{env_suffix}}.RAW.CUSTOMER;

----------------------------------------------------------------------
-- Row access policy (Early Access): row-level filtering by role.
-- Defined here; attached to the table in scripts/02_post_deploy.sql
-- (attaching a row access policy is not yet a DCM DEFINE capability).
----------------------------------------------------------------------
DEFINE ROW ACCESS POLICY DCM_DEMO_5{{env_suffix}}.GOV.CUSTOMER_COUNTRY_FILTER
    AS (COUNTRY VARCHAR) RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DCM_DEVELOPER') THEN TRUE
        WHEN CURRENT_ROLE() = 'DCM_DEMO_5{{env_suffix}}_ANALYST' AND COUNTRY = 'USA' THEN TRUE
        ELSE FALSE
    END
    COMMENT = 'Privileged roles see all rows; the analyst role sees only USA customers';
