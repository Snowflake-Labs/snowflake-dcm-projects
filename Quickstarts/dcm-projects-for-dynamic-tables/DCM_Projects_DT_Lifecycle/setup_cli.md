# DCM Projects for Dynamic Tables — CLI Setup

If you prefer using the Snowflake CLI (`snow`) instead of the Snowsight Workspaces UI, you can run all DCM operations from the command line.

## Prerequisites

- Install the [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/installation/installation)
- Configure a connection in `~/.snowflake/connections.toml`

## Commands

### Create the DCM Project Object

```bash
snow sql -q "
USE ROLE dcm_developer;
CREATE DATABASE IF NOT EXISTS dcm_demo;
CREATE SCHEMA IF NOT EXISTS dcm_demo.projects;
CREATE OR REPLACE DCM PROJECT dcm_demo.projects.dcm_project_dev
    COMMENT = 'for testing DCM Projects with Dynamic Tables';
"
```

### Plan

```bash
snow dcm plan \
  --project-path Quickstarts/DCM_DynamicTables_Quickstart \
  --target DCM_DEV
```

### Deploy

```bash
snow dcm deploy \
  --project-path Quickstarts/DCM_DynamicTables_Quickstart \
  --target DCM_DEV \
  --alias "Initial pipeline deployment"
```

### Refresh All Dynamic Tables

```bash
snow dcm refresh \
  --project-path Quickstarts/DCM_DynamicTables_Quickstart \
  --target DCM_DEV \
  --all
```

### Test Data Quality Expectations

```bash
snow dcm test \
  --project-path Quickstarts/DCM_DynamicTables_Quickstart \
  --target DCM_DEV \
  --all
```

### Preview Deployment State

```bash
snow dcm preview \
  --project-path Quickstarts/DCM_DynamicTables_Quickstart \
  --target DCM_DEV
```

### List Projects

```bash
snow dcm list --schema DCM_DEMO.PROJECTS
```

### Describe Project

```bash
snow dcm describe --project DCM_DEMO.PROJECTS.DCM_PROJECT_DEV
```

### List Deployment History

```bash
snow dcm list-deployments --project DCM_DEMO.PROJECTS.DCM_PROJECT_DEV
```

### Drop Project

```bash
snow dcm drop --project DCM_DEMO.PROJECTS.DCM_PROJECT_DEV
```
