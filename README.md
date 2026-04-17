# Snowflake DCM Projects - Quickstarts & Samples


⚠️ This repository contains demo content and code for preview features. 
It is not officially supported by Snowflake. 
Breaking changes may occur at any time. 
Use at your own risk.


DCM Projects is currently in Public Preview. 

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
6. Open the Quickstarts/DCM_Project_Quickstart_1/setup.ipynb notebook file
7. Connect the notebook to a compute pool so you can run the setup commands step by step


### Option B: in your local IDE ###
(if you are already familiar with snowflake-CLI)

1. install or update snowflake-cli to ensure you have version 3.16 or higher
2. connect to your Snowflake account and check with `snow connection test`
3. clone this dcm-quickstart repository `git clone https://github.com/snowflake-labs/snowflake-dcm-projects`
4. Navigate to the quickstart folder for your guide and follow the instructions

---

## Quickstarts

| Folder | Guide | Description |
|:-------|:------|:------------|
| `DCM_Project_Quickstart_1` | [Get Started with Snowflake DCM Projects](https://www.snowflake.com/en/developers/guides/get-started-snowflake-dcm-projects/) | DCM fundamentals — define infrastructure as code, Jinja templating, plan & deploy |
| `DCM_Project_Quickstart_2` | [Build Data Pipelines with Snowflake DCM Projects](https://www.snowflake.com/en/developers/guides/build-data-pipelines-with-snowflake-dcm-projects/) | Multi-project pipelines, medallion architecture, per-team infrastructure |
| `DCM_Projects_DT_Lifecycle` | [DCM Projects for Dynamic Tables](https://www.snowflake.com/en/developers/guides/dcm-projects-for-dynamic-tables/) | Dynamic table lifecycle — schema evolution & immutability constraints |
