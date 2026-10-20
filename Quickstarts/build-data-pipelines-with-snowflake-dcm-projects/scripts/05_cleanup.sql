/*=============================================================================
  05_cleanup.sql — Run when you're done and want to tear this guide down

  Follows the portfolio cleanup template. Every guide in this series shares one
  account, one container database for project objects and one developer role,
  so a cleanup script that drops the shared objects takes the other guides'
  projects with it. This script drops only what this guide owns and leaves
  everything shared in place, commented out with the reason.

  This guide owns two projects:

    platform project   dcm_demo.projects.dcm_platform_dev
    pipeline project   dcm_demo_2_finance_dev.projects.finance_pipeline
    managed databases  dcm_demo_2_dev, dcm_demo_2_finance_dev,
                       dcm_demo_2_marketing_dev
    developer role     dcm_developer (shared)

  The commented-out section below is deliberately identical across guides.
=============================================================================*/

USE ROLE dcm_developer;

----------------------------------------------------------------------
-- 1. Check what is in the shared schema before you remove anything
--
-- Run these first. If the schema holds projects belonging to other guides,
-- leave the shared objects in section 3 alone — which is the default here.
----------------------------------------------------------------------
SHOW DCM PROJECTS IN SCHEMA dcm_demo.projects;
SHOW GRANTS TO ROLE dcm_developer;

----------------------------------------------------------------------
-- 2. Purge everything both projects manage
--
-- PURGE drops every object a project created — databases, schemas, tables,
-- dynamic tables, views, warehouses, roles, grants and all of their data.
-- This is irreversible.
--
-- Order matters. Purge the Pipeline project first, while its host database
-- still exists. The Platform purge then drops the databases, warehouses and
-- team roles — including dcm_demo_2_finance_dev, which takes the Pipeline
-- project object with it.
--
-- IF PURGE FAILS: the project's objects are still live and may still consume
-- credits. Suspend the warehouses before you investigate. The known cause is a
-- data metric function attachment: a project holding a DMF attachment can fail
-- to purge, and this guide DOES attach DMFs (see expectations.sql). The
-- workaround is to drop the table the DMF is attached to, which removes the
-- attachment with it, then re-run PURGE.
----------------------------------------------------------------------
USE ROLE dcm_demo_2_finance_dev_admin;
EXECUTE DCM PROJECT dcm_demo_2_finance_dev.projects.finance_pipeline PURGE;

USE ROLE dcm_developer;
EXECUTE DCM PROJECT dcm_demo.projects.dcm_platform_dev PURGE;

-- PURGE leaves the project object itself behind. The Pipeline project object
-- went with its database; the Platform one lives in the shared schema.
DROP DCM PROJECT IF EXISTS dcm_demo.projects.dcm_platform_dev;

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
SHOW DATABASES LIKE 'dcm_demo_2%';
SHOW WAREHOUSES LIKE 'dcm_demo_2%';
SHOW DCM PROJECTS IN SCHEMA dcm_demo.projects;
