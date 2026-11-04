/*=============================================================================
  03_post_deploy.sql: Run AFTER the first successful deployment.

  Safe to re-run. Seeds PII sample data, makes the two policy associations that are set
  outside DCM (tag-based masking, network and authentication policies on the
  demo user), and verifies masked vs. unmasked access by role.

  The EMAIL column masking policy and the CUSTOMER row access policy are NOT
  applied here: DCM attaches them in governance.sql.

  Every USE ROLE is followed by USE WAREHOUSE. The project's warehouse runs
  these queries; your user's default warehouse is usually not granted to
  these roles, so no warehouse is active right after USE ROLE.
=============================================================================*/

USE ROLE DCM_DEVELOPER;
USE WAREHOUSE DCM_DEMO_5_WH_DEV;
SET USER_NAME = (SELECT CURRENT_USER());

-- 1. Seed sample customer data (with PII). TRUNCATE first, so re-running this
--    script leaves the same five rows instead of adding five more.
TRUNCATE TABLE DCM_DEMO_5_DEV.RAW.CUSTOMER;
INSERT INTO DCM_DEMO_5_DEV.RAW.CUSTOMER
    (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, CITY, COUNTRY)
VALUES
    (1, 'Alice',  'Johnson', 'alice.johnson@example.com', '+1-415-555-1001', 'San Francisco', 'USA'),
    (2, 'Bob',    'Smith',   'bob.smith@example.com',     '+1-212-555-1002', 'New York',      'USA'),
    (3, 'Chloe',  'Martin',  'chloe.martin@example.co.uk','+44-20-555-1003', 'London',        'UK'),
    (4, 'David',  'Nguyen',  'david.nguyen@example.com',  '+1-312-555-1004', 'Chicago',       'USA'),
    (5, 'Elena',  'Rossi',   'elena.rossi@example.it',    '+39-06-555-1005', 'Rome',          'Italy');

-- 2. Tag-based masking: set the masking policy on the PII tag, so any column
--    tagged GOV.PII later is masked too. EMAIL now has the same policy both
--    ways; Snowflake gives the directly attached one precedence. FORCE
--    replaces the policy if an earlier run of this script already set it.
ALTER TAG DCM_DEMO_5_DEV.GOV.PII SET MASKING POLICY DCM_DEMO_5_DEV.GOV.EMAIL_MASK FORCE;

-- 3. Set the network and authentication policies on the demo service user
--    created in 02_guide_setup.sql. Don't set them on your own user or on the
--    account: a network policy at account level can lock you out.
--    A user has at most one authentication policy, so the UNSET clears one
--    left by an earlier run of this script before the SET.
ALTER USER DCM_DEMO_5_DEV_SVC SET NETWORK_POLICY = DCM_DEMO_5_DEV_POLICY;
ALTER USER DCM_DEMO_5_DEV_SVC UNSET AUTHENTICATION POLICY;
ALTER USER DCM_DEMO_5_DEV_SVC SET AUTHENTICATION POLICY DCM_DEMO_5_DEV.GOV.PAT_POLICY;

SELECT POLICY_NAME, POLICY_KIND, REF_ENTITY_NAME
    FROM TABLE(DCM_DEMO_5_DEV.INFORMATION_SCHEMA.POLICY_REFERENCES(
        REF_ENTITY_NAME => 'DCM_DEMO_5_DEV_SVC', REF_ENTITY_DOMAIN => 'USER'));

-- 4. Grant the restricted analyst role to yourself so you can test it
GRANT ROLE DCM_DEMO_5_DEV_ANALYST TO USER IDENTIFIER($USER_NAME);

-- 5. Verify: the privileged role sees every row with clear emails
USE ROLE DCM_DEVELOPER;
USE WAREHOUSE DCM_DEMO_5_WH_DEV;
SELECT CUSTOMER_ID, FIRST_NAME, EMAIL, COUNTRY FROM DCM_DEMO_5_DEV.RAW.CUSTOMER ORDER BY CUSTOMER_ID;

-- 6. Verify: the restricted analyst sees only USA rows, with masked emails
USE ROLE DCM_DEMO_5_DEV_ANALYST;
USE WAREHOUSE DCM_DEMO_5_WH_DEV;
SELECT CUSTOMER_ID, FIRST_NAME, EMAIL, COUNTRY FROM DCM_DEMO_5_DEV.RAW.CUSTOMER ORDER BY CUSTOMER_ID;
