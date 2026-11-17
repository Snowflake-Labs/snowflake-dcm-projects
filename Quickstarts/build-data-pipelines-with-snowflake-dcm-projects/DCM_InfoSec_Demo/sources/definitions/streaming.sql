-- ### Streaming security pipeline
--
-- The InfoSec team ingests two event streams from the brokerage's trading app
-- with Snowpipe Streaming, and transforms each record in the pipe before it
-- lands. There is no landing table and no separate clean-up job: the pipe is
-- the transformation.
--
-- Both pipes enrich records by joining a shared raw table from the Platform
-- project. A pipe treats a joined table as a key-value lookup, so the table
-- must declare its primary key with RELY (see the Platform project's raw.sql)
-- and the ON clause must match that key exactly.
--
-- Two low-latency dynamic tables sit on top, with a 15 second target lag, so
-- the security views stay within seconds of the streams.

DEFINE SCHEMA DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY
    COMMENT = 'Streaming security events and the views built on them';

-- #### Scalar logic called from the pipes
--
-- A pipe can call SQL and JavaScript scalar UDFs, in the SELECT list or in the
-- WHERE clause.

DEFINE FUNCTION DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.CLASSIFY_SEVERITY(
    EVENT_TYPE VARCHAR, ACCOUNT_STATUS VARCHAR
)
RETURNS VARCHAR
IMMUTABLE
COMMENT = 'Severity of an access event, from its type and the account status'
AS
$$
    CASE
        WHEN ACCOUNT_STATUS = 'INAC' THEN 'HIGH'
        WHEN ACCOUNT_STATUS IS NULL THEN 'HIGH'
        WHEN EVENT_TYPE IN ('auth_failure', 'password_reset') THEN 'MEDIUM'
        ELSE 'LOW'
    END
$$;

DEFINE FUNCTION DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.HASH_PII(V VARCHAR)
RETURNS VARCHAR
IMMUTABLE
COMMENT = 'Salted SHA-256 of a lowercased identifier, so the raw value never lands'
AS
$$
    SHA2(CONCAT('dcm-demo-salt:', LOWER(V)), 256)
$$;

-- #### Stream 1: access events
--
-- Every login, order and API call against a brokerage account. The pipe drops
-- health checks, joins ACCOUNT_STG to attach the owning customer and the
-- account status, and classifies severity. Activity on an inactive account,
-- or on an account the platform has never heard of, lands as HIGH.

DEFINE TABLE DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.ACCESS_EVENTS (
    EVENT_TS        TIMESTAMP_NTZ(9) COMMENT 'When the event happened',
    EVENT_TYPE      VARCHAR          COMMENT 'login, auth_failure, password_reset, order_submit or api_key_use',
    CA_ID           NUMBER(38,0)     COMMENT 'Account the event was made against',
    C_ID            NUMBER(38,0)     COMMENT 'Owning customer, from ACCOUNT_STG; empty if the account is unknown',
    ACCOUNT_STATUS  VARCHAR(4)       COMMENT 'ACTV or INAC, from ACCOUNT_STG; empty if the account is unknown',
    SRC_IP          VARCHAR          COMMENT 'Source IP address of the request',
    SEVERITY        VARCHAR          COMMENT 'HIGH, MEDIUM or LOW, from CLASSIFY_SEVERITY'
)
ERROR_LOGGING = TRUE
COMMENT = 'Access events from the trading app, enriched as they stream in';

DEFINE PIPE DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.ACCESS_EVENTS_PIPE
    COMMENT = 'Streams access events: drops health checks, adds the account owner and status, classifies severity'
AS
COPY INTO DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.ACCESS_EVENTS (
    EVENT_TS, EVENT_TYPE, CA_ID, C_ID, ACCOUNT_STATUS, SRC_IP, SEVERITY
)
FROM (
    SELECT
        E.$1:ts::TIMESTAMP_NTZ,
        E.$1:event_type::VARCHAR,
        E.$1:ca_id::NUMBER,
        A.CA_C_ID,
        A.CA_ST_ID,
        E.$1:src_ip::VARCHAR,
        DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.CLASSIFY_SEVERITY(E.$1:event_type::VARCHAR, A.CA_ST_ID)
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')) E
    LEFT OUTER JOIN DCM_DEMO_2{{env_suffix}}.RAW.ACCOUNT_STG A
        ON E.$1:ca_id::NUMBER = A.CA_ID
    WHERE E.$1:event_type::VARCHAR <> 'health_check'
);

-- #### Stream 2: contact changes, privacy-safe
--
-- Customers changing the email address or phone number on their profile. The
-- raw values must never reach a table: the pipe keeps only customers who gave
-- consent, hashes the email, masks the phone number, and joins CUSTOMER_STG to
-- attach the customer's tier and country.

DEFINE TABLE DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.CONTACT_CHANGES (
    EVENT_TS      TIMESTAMP_NTZ(9) COMMENT 'When the contact details changed',
    C_ID          NUMBER(38,0)     COMMENT 'Customer who changed their contact details',
    EMAIL_HASH    VARCHAR          COMMENT 'Salted SHA-256 of the new email address',
    PHONE_MASKED  VARCHAR          COMMENT 'New phone number, all but the last four digits masked',
    C_TIER        NUMBER(1,0)      COMMENT 'Customer tier, from CUSTOMER_STG',
    C_CTRY        VARCHAR(24)      COMMENT 'Customer country, from CUSTOMER_STG'
)
ERROR_LOGGING = TRUE
COMMENT = 'Contact detail changes of consenting customers, with personal data hashed or masked';

DEFINE PIPE DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.CONTACT_CHANGES_PIPE
    COMMENT = 'Streams contact changes: keeps consenting customers, hashes email, masks phone, adds tier and country'
AS
COPY INTO DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.CONTACT_CHANGES (
    EVENT_TS, C_ID, EMAIL_HASH, PHONE_MASKED, C_TIER, C_CTRY
)
FROM (
    SELECT
        E.$1:ts::TIMESTAMP_NTZ,
        E.$1:c_id::NUMBER,
        DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.HASH_PII(E.$1:email::VARCHAR),
        CONCAT('***-***-', RIGHT(E.$1:phone::VARCHAR, 4)),
        C.C_TIER,
        C.C_CTRY
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')) E
    LEFT OUTER JOIN DCM_DEMO_2{{env_suffix}}.RAW.CUSTOMER_STG C
        ON E.$1:c_id::NUMBER = C.C_ID
    WHERE E.$1:consent::BOOLEAN = TRUE
);

-- #### Low-latency views
--
-- A 15 second target lag keeps both tables within seconds of the streams.
-- Each refresh runs on the InfoSec warehouse, so park them with
-- SCHEDULER = DISABLE when the demo is idle.

DEFINE DYNAMIC TABLE DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.SUSPICIOUS_ACTIVITY
    WAREHOUSE = DCM_DEMO_2_INFOSEC_WH{{env_suffix}}
    TARGET_LAG = '15 seconds'
    REFRESH_MODE = ADAPTIVE
    COMMENT = 'High-severity access events, with when the customer last changed their contact details'
AS
WITH LAST_CONTACT_CHANGE AS (
    SELECT C_ID, MAX(EVENT_TS) AS LAST_CONTACT_CHANGE_TS
    FROM DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.CONTACT_CHANGES
    GROUP BY C_ID
)
SELECT
    E.EVENT_TS,
    E.EVENT_TYPE,
    E.CA_ID,
    E.C_ID,
    E.ACCOUNT_STATUS,
    E.SRC_IP,
    L.LAST_CONTACT_CHANGE_TS
FROM DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.ACCESS_EVENTS E
LEFT JOIN LAST_CONTACT_CHANGE L
    ON L.C_ID = E.C_ID
WHERE E.SEVERITY = 'HIGH';

DEFINE DYNAMIC TABLE DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.ACCOUNT_RISK
    WAREHOUSE = DCM_DEMO_2_INFOSEC_WH{{env_suffix}}
    TARGET_LAG = '15 seconds'
    REFRESH_MODE = ADAPTIVE
    COMMENT = 'Per-account event counts by severity, refreshed every 15 seconds'
AS
SELECT
    CA_ID,
    C_ID,
    COUNT_IF(SEVERITY = 'HIGH')   AS HIGH_EVENTS,
    COUNT_IF(SEVERITY = 'MEDIUM') AS MEDIUM_EVENTS,
    COUNT(*)                      AS ALL_EVENTS,
    MAX(EVENT_TS)                 AS LAST_EVENT_TS
FROM DCM_DEMO_2_INFOSEC{{env_suffix}}.SECURITY.ACCESS_EVENTS
GROUP BY CA_ID, C_ID;
