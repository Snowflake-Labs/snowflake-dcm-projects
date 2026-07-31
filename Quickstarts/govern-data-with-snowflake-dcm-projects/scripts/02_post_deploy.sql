/*=============================================================================
  02_post_deploy.sql — Run AFTER the first successful deployment.
  Seeds PII sample data, wires tag-based masking (not yet DCM-native), and
  verifies masked vs. unmasked access by role.
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

-- 2. Tag-based masking: associate the masking policy with the PII tag.
--    Any column tagged GOV.PII (EMAIL, via ATTACH TAG in governance.sql) is
--    now masked by EMAIL_MASK for non-privileged roles.
--    (Tag<->policy association is not yet a DCM DEFINE capability.)
ALTER TAG dcm_demo_5_dev.gov.pii SET MASKING POLICY dcm_demo_5_dev.gov.email_mask;

-- 3. Grant the restricted analyst role to yourself so you can test masking
GRANT ROLE dcm_demo_5_dev_analyst TO USER IDENTIFIER($user_name);

-- 4. Verify: privileged role (dcm_developer) sees clear emails
USE ROLE dcm_developer;
SELECT customer_id, first_name, email FROM dcm_demo_5_dev.raw.customer ORDER BY customer_id;

-- 5. Verify: restricted analyst sees MASKED emails
USE ROLE dcm_demo_5_dev_analyst;
SELECT customer_id, first_name, email FROM dcm_demo_5_dev.raw.customer ORDER BY customer_id;
