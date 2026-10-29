/*=============================================================================
  02_post_deploy.sql — Run AFTER the first successful deployment.
  Seeds PII sample data, applies the remaining policy associations DCM
  can't express yet (tag-based masking, network/authentication policy
  activation), and verifies masked vs. unmasked access by role.

  The EMAIL column masking policy and CUSTOMER row access policy are NOT
  applied here: DCM attaches them in governance.sql.
=============================================================================*/

USE ROLE dcm_developer;
SET user_name = (SELECT CURRENT_USER());

-- 1. Seed sample customer data (with PII)
INSERT INTO dcm_demo_5_dev.raw.customer
    (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, CITY, COUNTRY)
VALUES
    (1, 'Alice',  'Johnson', 'alice.johnson@example.com', '+1-415-555-1001', 'San Francisco', 'USA'),
    (2, 'Bob',    'Smith',   'bob.smith@example.com',     '+1-212-555-1002', 'New York',      'USA'),
    (3, 'Chloe',  'Martin',  'chloe.martin@example.co.uk','+44-20-555-1003', 'London',        'UK'),
    (4, 'David',  'Nguyen',  'david.nguyen@example.com',  '+1-312-555-1004', 'Chicago',       'USA'),
    (5, 'Elena',  'Rossi',   'elena.rossi@example.it',    '+39-06-555-1005', 'Rome',          'Italy');

-- 2. Tag-based masking: associate the masking policy with the PII tag, so any
--    column tagged GOV.PII in the future is masked too. On EMAIL, the policy
--    that DCM attached directly takes precedence over the tag-based one.
--    (Tag<->policy association is not yet supported in DCM.)
ALTER TAG dcm_demo_5_dev.gov.pii SET MASKING POLICY dcm_demo_5_dev.gov.email_mask;

-- 3. Activate the network and authentication policies on the demo service
--    user created in 01_pre_deploy.sql. Assigning these policies is done with
--    ALTER USER / ALTER ACCOUNT outside DCM. Don't set them on your own user or
--    on the account: a network policy at account level can lock you out.
ALTER USER dcm_demo_5_dev_svc SET NETWORK_POLICY = dcm_demo_5_dev_policy;
ALTER USER dcm_demo_5_dev_svc SET AUTHENTICATION POLICY dcm_demo_5_dev.gov.pat_policy;

SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN USER dcm_demo_5_dev_svc;
SELECT policy_name, policy_kind, ref_entity_name
    FROM TABLE(dcm_demo_5_dev.INFORMATION_SCHEMA.POLICY_REFERENCES(
        REF_ENTITY_NAME => 'dcm_demo_5_dev_svc', REF_ENTITY_DOMAIN => 'user'));

-- 4. Grant the restricted analyst role to yourself so you can test masking
GRANT ROLE dcm_demo_5_dev_analyst TO USER IDENTIFIER($user_name);

-- 5. Verify: privileged role (dcm_developer) sees every row with clear emails
USE ROLE dcm_developer;
SELECT customer_id, first_name, email, country FROM dcm_demo_5_dev.raw.customer ORDER BY customer_id;

-- 6. Verify: restricted analyst sees only USA rows with MASKED emails
USE ROLE dcm_demo_5_dev_analyst;
SELECT customer_id, first_name, email, country FROM dcm_demo_5_dev.raw.customer ORDER BY customer_id;
