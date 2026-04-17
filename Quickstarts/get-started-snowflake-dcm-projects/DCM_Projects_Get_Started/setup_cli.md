# Snowflake DCM Projects (PrPr) - Quickstart 1 (snowCLI)

⚠️ Note: This project will only work if your account is already enrolled in the Private Preview of Snowflake DCM Projects.

---

This is a demo DCM Project for you to explore some of the core capabilities.

If you are new to DCM Projects we recommend to start with the Workspace UI in Snowflake to familarize yourself with the concept. Then switch over to your local IDE to run the same project with snowCLI commands.


### 1. Install the snowCLI with the latest DCM commands from this tag:

```pipx install git+https://github.com/snowflakedb/snowflake-cli.git --force```


### 2. Confirm your connection to your Snowflake account

`snow connection test`

Check that you have a default role with privileges to create a DCM Project



### 3. Clone the Snowflake-labs repo

- `git clone https://github.com/Snowflake-Labs/snowflake_dcm_projects` 

Navigate to the Quickstart_1 project files:
- `cd ./Quickstarts/DCM_Project_Quickstart_1`

Review the manifest and definition files inside that demo project

### 4. Create a new DCM Project object
Creates the project object with the name specified in the manifest.yml under the "DEV" target
- `snow dcm create --target DEV`

### 5. Plan & Deploy
Always run a DCM Plan before deploying changes to see how these definitions differ from the current state.
If none of these object exist yet, then PLAN will simulate all of them to be created in the right order.
- `snow dcm plan --variable “user=‘MY_USER_NAME’” `

If the plan was successful and the plan output matches the expected changes, then you can deploy those changes to the account.
- `snow dcm deploy --variable “user=‘MY_USER_NAME’” --alias MY_BIG_CHANGE`


### Other available DCM commands:
- `snow dcm list`
- `snow dcm describe`
- `snow dcm list-deployments`
- `snow dcm refresh`
- `snow dcm test`
- `snow dcm preview --target DEV --object DCM_PROJECT_DEV.SERVE.V_DASHBOARD_SALES_BY_CATEGORY_CITY`
- `snow dcm drop`


## Try out the DCM Skills for Cortex Code CLI

- Install Cortex Code CLI (see https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli)
- Start Cortex Code in your terminal
- Ask Cortex Code to add the DCM skill from https://github.com/Snowflake-Labs/snowflake_dcm_projects/cortex_code_skills/dcm
- Once the skill is loaded, you can ask Cortex to create, debug, improve or describe DCM projects 
