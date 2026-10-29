/*=============================================================================
  03_cleanup.sql — Run when you're done and want to tear this guide down

  THE PORTFOLIO CLEANUP TEMPLATE. Every guide in this series shares one account,
  one container database for project objects and one developer role, so a cleanup script that drops
  the shared objects takes the other guides' projects with it. This script drops
  only what this guide owns and leaves everything shared in place, commented out
  with the reason.

  To reuse this file in a sibling guide, change these four identifiers and
  nothing else:

    project FQN        dcm_demo.projects.dcm_gov_project_dev
    managed database   dcm_demo_5_dev
    warehouse          dcm_demo_5_wh_dev
    developer role     dcm_developer

  The commented-out section below is deliberately identical across guides.
=============================================================================*/

USE ROLE dcm_developer;

----------------------------------------------------------------------
-- 1. Check what is in the shared schema before you remove anything
--
-- Run these first. If the schema holds projects belonging to other guides,
-- leave the shared objects in section 2 alone — which is the default here.
----------------------------------------------------------------------
SHOW DCM PROJECTS IN SCHEMA dcm_demo.projects;
SHOW GRANTS TO ROLE dcm_developer;

----------------------------------------------------------------------
-- 2. Remove the associations applied outside DCM
--
-- 02_post_deploy.sql made these with ALTER statements because DCM can't
-- express them yet, so PURGE doesn't know about them. Remove them first: a
-- network policy can't be dropped while it's assigned to a user, and PURGE
-- fails on a masking policy that is still associated with a tag.
----------------------------------------------------------------------
ALTER USER IF EXISTS dcm_demo_5_dev_svc UNSET NETWORK_POLICY;
ALTER USER IF EXISTS dcm_demo_5_dev_svc UNSET AUTHENTICATION POLICY;
ALTER TAG IF EXISTS dcm_demo_5_dev.gov.pii UNSET MASKING POLICY dcm_demo_5_dev.gov.email_mask;

----------------------------------------------------------------------
-- 2a. Purge everything this project manages
--
-- PURGE drops every object the project created — the database, its schemas,
-- the table, the tag, the policies, the network rule and policy, the
-- warehouse, the analyst role and all of their data. It also detaches the
-- masking and row access policies attached with ATTACH MASKING POLICY and
-- ATTACH ROW ACCESS POLICY. This is irreversible.
--
-- PURGE leaves the project object itself behind, so drop that too.
--
-- IF PURGE FAILS: PURGE is not transactional, so some objects may already be
-- gone. The known cause here is a masking policy still set on a tag — re-run
-- the ALTER TAG statement in section 2, then re-run PURGE.
----------------------------------------------------------------------
EXECUTE DCM PROJECT dcm_demo.projects.dcm_gov_project_dev PURGE;

DROP DCM PROJECT IF EXISTS dcm_demo.projects.dcm_gov_project_dev;

----------------------------------------------------------------------
-- 2b. Drop the demo service user this guide created
--
-- It lives outside the project, so PURGE does not touch it. Only this guide
-- uses it, so it is removed here rather than left with the shared objects.
----------------------------------------------------------------------
DROP USER IF EXISTS dcm_demo_5_dev_svc;

----------------------------------------------------------------------
-- 3. Everything shared is left alone, on purpose
--
-- All of these are used by the other guides in this series and may be in use
-- elsewhere on your account. Removing them is your call rather than this
-- script's. Uncomment only what you are certain nothing else needs, and only
-- after reading the output of section 1.
--
--   dcm_demo               shared container database. It holds the project objects of
--                          EVERY DCM guide you have run, not just this one.
--                          Dropping it destroys those projects too.
--   dcm_demo.projects      the schema that holds the project objects — same problem, narrower.
--   dcm_developer          shared role. Every guide in this series uses it, and
--                          it may own objects you created outside these guides;
--                          dropping a role that owns objects orphans them.
----------------------------------------------------------------------
-- DROP SCHEMA IF EXISTS dcm_demo.projects;
-- DROP DATABASE IF EXISTS dcm_demo;
-- USE ROLE ACCOUNTADMIN;
-- DROP ROLE IF EXISTS dcm_developer;

-- The account parameter enabled in 01_pre_deploy.sql is left on. It is
-- account-wide, harmless, and needed by the other guides, but this is how you
-- would turn it back off:
-- ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'DISABLED';

----------------------------------------------------------------------
-- 4. Verify what went and what remains
----------------------------------------------------------------------
SHOW DATABASES LIKE 'dcm_demo_5_dev';
SHOW WAREHOUSES LIKE 'dcm_demo_5_wh_dev';
SHOW DCM PROJECTS IN SCHEMA dcm_demo.projects;
