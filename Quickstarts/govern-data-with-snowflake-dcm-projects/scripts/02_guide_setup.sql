/*=============================================================================
  02_guide_setup.sql: Govern Data with Snowflake DCM Projects

  Run once, as ACCOUNTADMIN, AFTER 01_pre_deploy.sql and BEFORE the first
  plan. It is idempotent.

  Creates the one object this guide needs that a DCM project cannot own: the
  demo service user that 03_post_deploy.sql sets the network and
  authentication policies on. Never set the demo policies on your own user or
  on the account.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Demo service user
--
-- Users are account-level objects outside any DCM Project. Owning the user is
-- what lets DCM_DEVELOPER set its network policy.
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS DCM_DEMO_5_DEV_SVC
    TYPE = SERVICE
    COMMENT = 'Demo service user for the Govern Data with DCM Projects quickstart; target of the demo network and authentication policies';
GRANT OWNERSHIP ON USER DCM_DEMO_5_DEV_SVC TO ROLE DCM_DEVELOPER COPY CURRENT GRANTS;
