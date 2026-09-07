/*=============================================================================
  03_cleanup.sql — Run when you're done and want to tear this guide down

  THE PORTFOLIO CLEANUP TEMPLATE. Every guide in this series shares one account,
  one registry database and one developer role, so a cleanup script that drops
  the shared objects takes the other guides' projects with it. This script drops
  only what this guide owns and leaves everything shared in place, commented out
  with the reason.

  To reuse this file in a sibling guide, change these four identifiers and
  nothing else:

    project FQN        dcm_demo.projects.dcm_project_dev
    managed database   dcm_demo_1_dev
    warehouse          dcm_demo_1_wh_dev
    developer role     dcm_developer

  The commented-out section below is deliberately identical across guides.
=============================================================================*/

USE ROLE dcm_developer;

----------------------------------------------------------------------
-- 1. Check what is in the shared registry before you remove anything
--
-- Run these first. If the registry holds projects belonging to other guides,
-- leave the shared objects in section 2 alone — which is the default here.
----------------------------------------------------------------------
SHOW DCM PROJECTS IN SCHEMA dcm_demo.projects;
SHOW GRANTS TO ROLE dcm_developer;

----------------------------------------------------------------------
-- 2. Purge everything this project manages
--
-- PURGE drops every object the project created — the database, its schemas,
-- tables, the dynamic table, the semantic view, the warehouse, the roles and
-- all of their data. This is irreversible.
--
-- PURGE leaves the project object itself behind, so drop that too.
--
-- IF PURGE FAILS: the project's objects are still live and may still consume
-- credits. Suspend anything scheduled before you investigate:
--
--   ALTER DYNAMIC TABLE dcm_demo_1_dev.analytics.enriched_order_details SUSPEND;
--   ALTER WAREHOUSE IF EXISTS dcm_demo_1_wh_dev SUSPEND;
--
-- The known cause is a data metric function attachment: a project holding a DMF
-- attachment can fail to purge. This guide attaches none, so you should not hit
-- it here — but the sibling guides do. The workaround is to drop the table the
-- DMF is attached to, which removes the attachment with it, then re-run PURGE.
----------------------------------------------------------------------
EXECUTE DCM PROJECT dcm_demo.projects.dcm_project_dev PURGE;

DROP DCM PROJECT IF EXISTS dcm_demo.projects.dcm_project_dev;

----------------------------------------------------------------------
-- 3. Everything shared is left alone, on purpose
--
-- All of these are used by the other guides in this series and may be in use
-- elsewhere on your account. Removing them is your call rather than this
-- script's. Uncomment only what you are certain nothing else needs, and only
-- after reading the output of section 1.
--
--   dcm_demo               registry database. It holds the project objects of
--                          EVERY DCM guide you have run, not just this one.
--                          Dropping it destroys those projects too.
--   dcm_demo.projects      the registry schema itself — same problem, narrower.
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
-- ALTER ACCOUNT UNSET FEATURE_RBAC_INHERITED_GRANTS;

----------------------------------------------------------------------
-- 4. Verify what went and what remains
----------------------------------------------------------------------
SHOW DATABASES LIKE 'dcm_demo_1_dev';
SHOW WAREHOUSES LIKE 'dcm_demo_1_wh_dev';
SHOW DCM PROJECTS IN SCHEMA dcm_demo.projects;
