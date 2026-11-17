# Full DCM demo project

This standalone Tasty Bytes example demonstrates object types, grant families, and attachments from the [public DCM supported-entities reference](https://docs.snowflake.com/en/user-guide/dcm-projects/dcm-projects-supported-entities). It does not demonstrate every supported variant: streams use project-managed tables, views, and stages rather than pre-existing external tables. See the [repository overview](../../README.md#standalone-sample-projects) to compare the public and early-access samples.

## **Targets separate registration from managed objects**

`manifest.yml` contains portable account placeholders. Each target supplies a project identifier and owner role. The registration database `DCM_DEMO` and schema `PROJECTS` must already exist. The definitions create `DCM_DEMO_1_FULL_DEV` for DEV and `DCM_DEMO_1_FULL_PROD` for PROD. Every account-level object name carries the environment suffix, so DEV and PROD can be deployed side by side on the same account.

Configuration required before validation:

* Account identifiers and pre-existing project registrations for the chosen targets.
* `user_name`, identifying an existing recipient of the sample read role.
* `project_owner_role`, matching the registration's actual owner.
* `compute_pool`, identifying an accessible Streamlit compute pool.
* `assets/notebook_job/code_bundle.yml`, whose compute pool/runtime values are uploaded literally and receive no DCM Jinja substitution.

The low-inventory alert in `monitoring.sql` runs on the project-defined `DCM_DEMO_1_WH{{env_suffix}}` warehouse and ships `SUSPENDED`. It references the existing `dcm_demo_notification` email integration and a placeholder recipient, which need account-specific configuration before the alert is resumed. The public S3 stage and pipe demonstrate object definitions; the `dcm_sample_orders/` path does not promise an available dataset and no files are loaded during PLAN.

## **Validation requires grant authority and account prerequisites**

The [project-owner setup SQL](../dcm_project_owner_privileges_setup.sql) runs in one go on a single account that hosts both DEV and PROD. It enables inherited grants, creates the shared setup resources and the `DCM_DEVELOPER` and `DCM_PROD_DEPLOYER` roles, and grants both roles broad account-level administration privileges. It runs outside DCM.

The project owner needs privileges to create every included object type and to apply tags and DMFs.

Inherited grants and container-level `MANAGE GRANTS` require the account opt-in described in the [inherited-grants documentation](https://docs.snowflake.com/en/user-guide/inherited-grants-intro). The DCM definitions do not change account parameters or bootstrap administrator privileges; the separate manual setup script does.

The existing analytics example calls `SNOWFLAKE.CORTEX.AI_COMPLETE`; model availability and Cortex execution access are runtime prerequisites. Python and Java handlers likewise need their supported runtimes/packages.

From this project directory, using a configured connection:

```bash
snow dcm plan -c MY_CONNECTION --target DCM_DEV --save-output
# After a deployment baseline exists:
snow dcm plan -c MY_CONNECTION --target DCM_DEV --delta --save-output
```

`plan` is the required validation step before deployment. CLI output is saved relative to the command's working directory. PLAN validates object operations but does not execute handlers or test the app and notebook.

## **Deployment includes scheduled work and grant changes**

No deployment is performed by the validation commands above. The `ORDERS_PIPELINE` task graph in `ingest.sql` declares `STARTED` and runs daily: it inserts sample data, loads staged files, and moves new order lines from `INCOMING_ORDERS_STREAM` into the order tables. Deploying it starts scheduled activity. The low-inventory alert checks inventory every 60 minutes once resumed; it and the notebook and Python tasks declare `SUSPENDED`. Every task and alert runs on the project warehouse, so no serverless compute is used. Dynamic tables refresh on their declared schedules, and DMFs can incur monitoring usage.

The network policy is unassigned and allows all IPv4 addresses. It is a syntax demonstration, with no protection effect until assigned. The authentication policy is also unassigned. The `CITY_ROW_FILTER` row access policy is attached to `RAW.CUSTOMER` and limits rows to the cities of the `DCM_DEMO_1_SALES_NA` and `DCM_DEMO_1_SALES_EU` roles, while the project admin role sees all rows. The `SP_UPSERT_FROM_INCOMING` procedure is owned by the lower-privileged `PIPELINE_RUNNER` role, and the `SEMANTIC_READER` database role grants access to the semantic view only. The share in `serve.sql` grants two `RAW` tables and the secure new-versus-returning customers view, and has no consumer accounts; consumers are added outside DCM with `ALTER SHARE ... ADD ACCOUNTS`.

The existing `SQL_post_scripts/insert_sample_data.sql` is an optional manual companion template. DCM does not execute or render it. Its `{{env_suffix}}` placeholder needs replacement with the chosen literal suffix before manual execution. The ingestion root task also supplies sample data; neither path is run by PLAN.

`sources/tests/0_smoke_test.sql` is a minimal DCM test that runs a single query on a branch.

## **Masking attachments and tag propagation use project-managed objects**

`sources/definitions/masking.sql` attaches masking policies to synthetic table, view, and dynamic-table columns, and conditional masking demonstrates `USING`. DCM applies masking attachments with `FORCE`, so deployment can replace a manually attached policy; removal detaches the managed policy without restoring an earlier one. `EMAIL_MASK` in `governance.sql` remains unattached because the sample tables have no email column. `PII_NAME_MASK` in `governance.sql` masks `RAW.CUSTOMER.LAST_NAME` for roles outside the project admin role.

The `SENSITIVITY` tag in `governance.sql` declares `PROPAGATE`, so objects that read `RAW.CUSTOMER.LAST_NAME` inherit it through dependency or data movement. Tag propagation requires Enterprise Edition or higher.

## **Streams use project-managed sources**

`sources/definitions/ingest.sql` includes the pipe and table, view, and directory streams on objects managed by this project. The sample omits streams on pre-existing external tables to avoid that setup dependency.

The Streamlit app uses the deployed database context and pre-installed packages, so it has no dependency file that would trigger a PyPI download or require an external access integration. The Code Bundle notebook demonstrates packaged execution and a runtime argument without reading or changing database data.

## **The app has three pages and two separate bundle examples**

`sources/definitions/dashboard.sql` defines the Streamlit app. `sources/definitions/notebook.sql` defines both Code Bundles and their execution tasks. The serving views and semantic view are defined together in `sources/definitions/serve.sql`.

The Streamlit entrypoint uses `st.navigation` to register:

* Overview.
* Daily sales.
* Category and city.

Page files live under `assets/streamlit/app_pages/`; `data.py` centralizes bounded cached reads. The asset glob includes all page and helper files.

`PYTHON_BUNDLE` uses `assets/python_job/main.py` as its executable entrypoint and warehouse compute with Python 3.11. Its suspended `RUN_PYTHON` task passes the target suffix as `--environment`; the task warehouse supplies its compute context. The existing notebook bundle remains separate and uses a compute pool.

The Python example runs locally without Snowflake or additional packages:

```bash
python3 assets/python_job/main.py --environment local
```

It prints two distinct orders and four units, totaling USD 40.00 from embedded synthetic data. This local check exercises script logic; execution on Snowflake requires a later deployment. Neither bundle writes database data or calls an external service.