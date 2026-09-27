# Snowflake DCM Projects - Quickstarts & Samples


⚠️ This repository includes demo content and code for preview features. 
It is not officially supported by Snowflake. 
Breaking changes may occur at any time. 
Use at your own risk.



Documentation: https://docs.snowflake.com/en/user-guide/dcm-projects/dcm-projects-overview 

---

How to use this demo content:

### Option A: In Snowsight Workspaces ###
(recommended for starters)


1. Navigate to your Snowsight Workspace
2. Create a new Workspace from Git repository
3. insert URL `https://github.com/snowflake-labs/snowflake-dcm-projects`
4. select an API Integration for github (create one if needed)
5. select "public repository"
6. Navigate to a quickstart or sample project below and follow its instructions


### Option B: in your local IDE ###
(if you are already familiar with snowflake-CLI)

1. Install or update Snowflake CLI and check the chosen guide or sample README for version and early-access requirements
2. connect to your Snowflake account and check with `snow connection test`
3. clone this dcm-quickstart repository `git clone https://github.com/snowflake-labs/snowflake-dcm-projects`
4. Navigate to a quickstart or sample project below and follow its instructions

---

## **Quickstarts**

| Folder | Guide | Description |
|:-------|:------|:------------|
| `Quickstarts/get-started-snowflake-dcm-projects/DCM_Projects_Get_Started` | [Get Started with Snowflake DCM Projects](https://www.snowflake.com/en/developers/guides/get-started-snowflake-dcm-projects/) | DCM fundamentals — define infrastructure as code, Jinja templating, plan & deploy |
| `Quickstarts/build-data-pipelines-with-snowflake-dcm-projects/DCM_Platform_Demo` + `DCM_Pipeline_Demo` | [Build Data Pipelines with Snowflake DCM Projects](https://www.snowflake.com/en/developers/guides/build-data-pipelines-with-snowflake-dcm-projects/) | Multi-project pipelines, medallion architecture, per-team infrastructure |
| `Quickstarts/dcm-projects-for-dynamic-tables/DCM_Projects_DT_Lifecycle` | [DCM Projects for Dynamic Tables](https://www.snowflake.com/en/developers/guides/dcm-projects-for-dynamic-tables/) | Dynamic table lifecycle — schema evolution & immutability constraints |
| `Quickstarts/dcm-projects-for-tasks/DCM_Projects_Tasks` | [DCM Projects for Tasks](https://www.snowflake.com/en/developers/guides/dcm-projects-for-tasks/) | Task graphs — finalizer, DMF quality gate, serverless alert, DEFINE PROCEDURE |

## **Standalone sample projects**

These samples provide broader feature examples rather than step-by-step quickstarts. Each project packages its own definitions and assets; account-level setup and runtime prerequisites are still required.

| Project | Coverage |
|:--------|:---------|
| [Full DCM demo](sample-projects/dcm-full-demo-project/README.md) | GA and Public Preview object types, grant families, and attachments on project-managed objects. |
| [Early-access DCM demo](sample-projects/dcm-early-access-project/README.md) | The public baseline plus Private Preview masking-policy attachments, dbt projects, Task Graph Objects, and generic-string/password secrets. Requires the corresponding early-access capabilities. |

Both projects include a three-page Streamlit app and separate notebook and executable Python Code Bundles. Their manifests provide `DCM_DEV`, `DCM_PROD_US`, and `DCM_PROD_EU` targets, using DEV and PROD configurations and distinct names for the two samples.

To minimize setup dependencies, the samples omit streams on pre-existing external tables, masking attachments on pre-existing Iceberg tables, and secret variants requiring external authentication integrations. Account and runtime prerequisites still apply as documented in each sample README.

Before using a sample:

* Replace the manifest's account, user, and owner-role placeholders and review the project's runtime prerequisites.
* Review the manual [project-owner setup SQL](sample-projects/dcm_project_owner_privileges_setup.sql). It creates shared setup resources, enables inherited grants, and grants broad administrator privileges. Run only the appropriate statements in the intended account; DCM does not run this file automatically.
* Follow the sample README's CLI requirements. The documented `compile` command requires an early-access CLI; the quickstart CLI baseline is not sufficient for every sample feature.
* Review `snow dcm plan` output before deploying. PLAN does not execute the app, Code Bundles, or dbt models. Deployment can start existing task and alert schedules and incur compute costs.
