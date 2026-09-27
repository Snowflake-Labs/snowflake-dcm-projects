# Full DCM demo project

This standalone Tasty Bytes example includes every object type and grant family in the [public DCM supported-entities reference](https://docs.snowflake.com/en/user-guide/dcm-projects/dcm-projects-supported-entities). The [coverage matrix](../README.md) maps each entity to its file and identifies conditional variants.

## **Targets separate registration from managed objects**

`manifest.yml` contains portable account placeholders. Each target supplies a project identifier and owner role. The registration database `DCM_DEMO` and schema `PROJECTS` must already exist. The definitions create `DCM_DEMO_1_FULL_DEV` for DEV and `DCM_DEMO_1_FULL_PROD` for the PROD targets in their respective accounts.

Configuration required before validation:

* Account identifiers and pre-existing project registrations for the chosen targets.
* `user_name`, identifying an existing recipient of the sample read role.
* `project_owner_role`, matching the registration's actual owner.
* `compute_pool`, identifying an accessible Streamlit compute pool.
* `assets/notebook_job/code_bundle.yml`, whose compute pool/runtime values are uploaded literally and receive no DCM Jinja substitution.

The existing project uses `DCM_WH` for its alert. `monitoring.sql` also references the existing `dcm_demo_notification` email integration and a placeholder recipient. Those need account-specific configuration before the alert can run. The public S3 stage and pipe demonstrate object definitions; the `dcm_sample_orders/` path does not promise an available dataset and no files are loaded during PLAN.

## **Validation requires grant authority and preview prerequisites**

[`../0_dcm_owner_privileges_setup.sql`](../0_dcm_owner_privileges_setup.sql) contains manual administrator grants for the DEV and PROD deployer roles. It separates the shared prerequisites from optional external-resource access. It runs outside DCM and grants broad account-level administration privileges; only the applicable environment section belongs in each target account.

The project owner needs privileges to create every included object type and to apply tags and DMFs. Account-level grants in `grant_examples.sql` also require authority to delegate `EXECUTE TASK` and `EXECUTE DATA METRIC FUNCTION`.

Inherited grants and container-level `MANAGE GRANTS` require the account opt-in described in the [inherited-grants documentation](https://docs.snowflake.com/en/user-guide/inherited-grants-intro). These samples do not change account parameters or bootstrap administrator privileges.

The existing analytics example calls `SNOWFLAKE.CORTEX.AI_COMPLETE`; model availability and Cortex execution access are runtime prerequisites. Python and Java handlers likewise need their supported runtimes/packages.

From this project directory, using a configured connection:

```bash
snow dcm compile -c MY_CONNECTION --target DCM_DEV --save-output
snow dcm plan -c MY_CONNECTION --target DCM_DEV --save-output
# After a deployment baseline exists:
snow dcm plan -c MY_CONNECTION --target DCM_DEV --delta --save-output
```

`compile` is an early-access CLI enhancement. `plan` remains the required validation step before deployment. CLI output is saved relative to the command's working directory. PLAN validates object operations but does not execute handlers or test the app and notebook.

## **Deployment includes scheduled work and grant changes**

No deployment is performed by the validation commands above. The existing ingestion tasks and low-inventory alert declare `STARTED`; deploying them starts scheduled activity. The added notebook task declares `SUSPENDED`. Dynamic tables refresh on their declared schedules, and DMFs can incur monitoring usage.

The network policy is unassigned and allows all IPv4 addresses. It is a syntax demonstration, with no protection effect until assigned. The authentication policy is also unassigned. Masking and row access policies are defined but unattached in this public sample. The share has no consumer accounts.

The existing `SQL_post_scripts/insert_sample_data.sql` is an optional manual companion template. DCM does not execute or render it. Its `{{env_suffix}}` placeholder needs replacement with the chosen literal suffix before manual execution. The ingestion root task also supplies sample data; neither path is run by compile or PLAN.

## **Optional external-table coverage uses an existing source**

Setting `external_table` to an accessible external-table FQN enables the external-table stream in `streams_and_pipe.sql`. An empty value leaves that variant undeclared; table, view, and directory streams are always included.

The Streamlit app uses the deployed database context and pre-installed packages, so it has no dependency file that would trigger a PyPI download or require an external access integration. The Code Bundle notebook demonstrates packaged execution and a runtime argument without reading or changing database data.

## **The app has three pages and two separate bundle examples**

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