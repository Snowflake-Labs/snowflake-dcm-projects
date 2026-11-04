/*=============================================================================
  02_guide_setup.sql: DCM Projects for Tasks

  Run once, AFTER 01_pre_deploy.sql and BEFORE the first plan. It is
  idempotent.

  Creates the one object this guide needs that a DCM project cannot own: the
  email notification integration the TASK_ERROR_RATE alert sends to. Plan
  validates the reference, so it must exist before the first plan.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Email notification integration
--
-- A DCM project cannot define notification integrations, so it is created
-- here, as DCM_DEVELOPER (CREATE INTEGRATION comes from 01_pre_deploy.sql).
-- The role then owns it, and ownership covers the USAGE the alert needs at
-- send time. With no ALLOWED_RECIPIENTS it can email any verified email
-- address of a user in this account.
----------------------------------------------------------------------
USE ROLE DCM_DEVELOPER;

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS DCM_DEMO_4_EMAIL_INT
    TYPE = EMAIL
    ENABLED = TRUE
    COMMENT = 'Email integration for the DCM Projects for Tasks alert';

----------------------------------------------------------------------
-- 2. Value for the manifest
--
-- Set alert_email in DCM_Projects_Tasks/manifest.yml to the verified email
-- address of your Snowflake user.
----------------------------------------------------------------------
SELECT CURRENT_USER() AS USER_NAME;
