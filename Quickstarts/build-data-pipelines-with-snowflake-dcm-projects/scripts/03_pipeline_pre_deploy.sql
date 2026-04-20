-- =============================================================================
-- Script 3: Pipeline Pre-Deploy
-- Run this BEFORE deploying the Pipeline project
-- =============================================================================

-- The Pipeline project lives in the Finance team's database,
-- which was created by the Platform deployment.
-- Use the Finance team's admin role to create the DCM Project object.

USE ROLE dcm_demo_2_finance_dev_admin;

CREATE DCM PROJECT IF NOT EXISTS dcm_demo_2_finance_dev.projects.finance_pipeline
    COMMENT = 'for DCM Pipeline Demo - Build Data Pipelines Quickstart';
