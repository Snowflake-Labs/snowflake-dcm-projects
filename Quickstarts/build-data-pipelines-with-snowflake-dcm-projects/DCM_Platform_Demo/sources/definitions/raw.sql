-- ### Shared raw layer
--
-- One database and one RAW schema that every team reads from. The three
-- staging tables hold a small brokerage feed: customers, their accounts, and
-- the trades placed on those accounts.
--
-- CHANGE_TRACKING = TRUE lets the Pipeline project's dynamic tables track
-- what changed in these tables between refreshes.

DEFINE DATABASE DCM_DEMO_2{{env_suffix}}
    COMMENT = 'Shared raw layer for the DCM Build Data Pipelines Quickstart';

DEFINE SCHEMA DCM_DEMO_2{{env_suffix}}.RAW
    COMMENT = 'Raw staging tables, the ingestion stage and the load task';

DEFINE TABLE DCM_DEMO_2{{env_suffix}}.RAW.CUSTOMER_STG (
    C_ID          NUMBER(38,0) COMMENT 'Customer identifier',
    C_F_NAME      VARCHAR(20)  COMMENT 'First name',
    C_L_NAME      VARCHAR(25)  COMMENT 'Last name',
    C_TIER        NUMBER(1,0)  COMMENT 'Customer tier, 1 to 3',
    C_DOB         DATE         COMMENT 'Date of birth',
    C_CITY        VARCHAR(25)  COMMENT 'City',
    C_STATE_PROV  VARCHAR(20)  COMMENT 'State or province',
    C_CTRY        VARCHAR(24)  COMMENT 'Country'
)
CHANGE_TRACKING = TRUE
COMMENT = 'Customers, one row per customer';

DEFINE TABLE DCM_DEMO_2{{env_suffix}}.RAW.ACCOUNT_STG (
    CA_ID      NUMBER(38,0) COMMENT 'Customer account identifier',
    CA_C_ID    NUMBER(38,0) COMMENT 'Owning customer identifier',
    CA_NAME    VARCHAR(50)  COMMENT 'Account name',
    CA_TAX_ST  NUMBER(1,0)  COMMENT 'Tax status: 0, 1 or 2',
    CA_ST_ID   VARCHAR(4)   COMMENT 'Status: ACTV or INAC'
)
CHANGE_TRACKING = TRUE
COMMENT = 'Brokerage accounts, each owned by one customer';

DEFINE TABLE DCM_DEMO_2{{env_suffix}}.RAW.TRADE_STG (
    T_ID           NUMBER(15,0)     COMMENT 'Trade identifier',
    T_DTS          TIMESTAMP_NTZ(9) COMMENT 'Date and time of the trade',
    T_ST_ID        VARCHAR(4)       COMMENT 'Status: CMPT (completed) or CNCL (cancelled)',
    T_TT_ID        VARCHAR(3)       COMMENT 'Trade type: TMB (market buy) or TMS (market sell)',
    T_S_SYMB       VARCHAR(15)      COMMENT 'Security symbol',
    T_QTY          NUMBER(6,0)      COMMENT 'Quantity of securities traded',
    T_TRADE_PRICE  NUMBER(8,2)      COMMENT 'Unit price the security traded at',
    T_CHRG         NUMBER(10,2)     COMMENT 'Fee charged for placing the trade',
    T_COMM         NUMBER(10,2)     COMMENT 'Commission earned on the trade',
    T_CA_ID        NUMBER(38,0)     COMMENT 'Customer account identifier'
)
CHANGE_TRACKING = TRUE
COMMENT = 'Trades, one row per trade';
