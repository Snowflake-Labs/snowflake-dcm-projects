# Early-access DCM demo project

This standalone project includes the public sample baseline plus API, external access, and storage integrations, dbt projects, Task Graph Objects, and generic-string/password secrets from the [DCM early-access reference](https://docs.snowflake.com/en/LIMITEDACCESS/dcm-projects/dcm-projects-early-access). It intentionally omits variants requiring pre-existing Iceberg/external tables or authentication integrations. See the [repository overview](../../README.md#standalone-sample-projects) to compare the public and early-access samples.

## **Early-access targets use separate names**

The [project-owner setup SQL](../dcm_project_owner_privileges_setup.sql) runs in one go on a single account that hosts both DEV and PROD. It enables inherited grants, creates the shared setup resources and the `DCM_DEVELOPER` and `DCM_PROD_DEPLOYER` roles, and grants both roles broad account-level administration privileges. It runs outside DCM.

The DEV target manages `DCM_DEMO_1_EA_DEV`, and the PROD target manages `DCM_DEMO_1_EA_PROD`. Every account-level object name carries the environment suffix, so DEV and PROD can be deployed side by side on the same account. Registration names differ from the public sample. Each target requires its own pre-existing DCM project under `DCM_DEMO.PROJECTS`.

Portable placeholders requiring configuration:

* Account identifiers and the registration owner roles.
* An existing `user_name` for the read-role grant.
* An accessible Streamlit `compute_pool`.
* The literal compute pool/runtime in `assets/notebook_job/code_bundle.yml`.
* Existing alert infrastructure: `dcm_demo_notification` and a registered email recipient in `monitoring.sql`, needed before the suspended alert is resumed.

The public baseline's grant authority and inherited-grants opt-in requirements apply here too. The DCM definitions do not enable account features or provision administrator privileges; the separate manual setup script enables inherited grants and grants deployer privileges. The account must additionally have the private-preview capabilities enabled.

## **Runtime secrets stay outside the project files**

The manifest declares:

* `BUILD_NUMBER`, a non-sensitive environment variable included in a comment.
* `DEMO_API_KEY`, the generic-string secret value.
* `DEMO_PASSWORD`, the password secret value.

Real values must remain outside version control. Every sensitive SQL property uses `_snow.env_secret()`, so it renders as a secret reference. The sample contains no function that returns a secret's value.

From this directory, after configuring targets:

```bash
snow dcm plan -c MY_CONNECTION --target DCM_DEV --save-output
# With a deployment baseline:
snow dcm plan -c MY_CONNECTION --target DCM_DEV --delta --save-output
```

The sample demonstrates generic-string and password secrets only. OAuth2 and cloud-provider-token variants are omitted because they require separately configured integrations.

`include_secrets` defaults to `true`. For partial validation without runtime values, `-D "include_secrets=false"` omits `early_access_secrets.sql` statements. Such a run does not validate secrets or runtime environment value supply. This override is intended for validation of a new registration: deploying it against a project that already manages secrets would remove those definitions and plan their deletion.

## **Integrations and dbt configuration have explicit boundaries**

`sources/definitions/early_access_integrations.sql` defines one API, one external access, and one storage integration. Integrations are account-level objects, so each name carries `{{env_suffix}}` to keep DEV and PROD apart, and the project owner needs `CREATE INTEGRATION`.

* **API integration:** `DCM_DEMO_1_GITHUB_API` allows Git HTTPS access to `https://github.com`. No project object references it, because DCM does not define Git repositories.
* **External access integration:** `SP_CHECK_EXAMPLE_API` references it and calls `https://example.com` only when the procedure is executed. Nothing in the project calls it.
* **Storage integration:** `S3_INTEGRATION_STAGE` references it. The AWS role ARN and bucket are placeholders; listing or loading from the stage fails until a real bucket and IAM trust policy are configured.

Masking attachments, tag propagation, and the row access policy come from the public baseline. Streams use project-managed objects. Examples requiring pre-existing Iceberg or external tables are omitted to minimize setup dependencies.

`sources/dbt/tasty_dbt/profiles.yml` is independent of DCM templating. Its DEV and PROD databases and warehouses match the manifest's shipped suffixes. Changes to those names or owner roles need matching profile changes. The dbt sources resolve from `target.database`. DCM-managed objects have no dependencies on dbt-produced models.

PLAN does not compile or execute dbt models. Deployment compiles the dbt project. The suspended `DBT_RUN` and `DBT_TEST` tasks execute the project later. The separate Task Graph Object has exactly one root and defaults its members to suspended.

Both seed-data paths include every menu ID referenced by the supplied order details, including the optional lowercase-city examples. The additional menu names and prices are synthetic demo values. The dbt source relationship test remains enabled to catch unmatched IDs in externally loaded data. For an existing deployment, deploy the updated task definition and run `INSERT_SAMPLE_DATA` to add missing menu entries before rerunning dbt tests; its MENU insert preserves existing IDs. The manual seed script is intended for a fresh load, not an idempotent repair.

## **Validation does not deploy or run scheduled work**

The existing ingestion tasks declare `STARTED`; a later deployment starts those schedules. The low-inventory alert and the notebook, dbt, and Task Graph Object examples are suspended. Dynamic tables and DMFs use their declared refresh/monitoring schedules. The analytics baseline also requires Cortex access for its existing `AI_COMPLETE` call.

The network/authentication policies remain unassigned, and the share has no consumer accounts. The network policy allows all IPv4 traffic if assigned. The external stage and manual pipe are syntax examples without a guaranteed input dataset.

`SQL_post_scripts/insert_sample_data.sql` is a manual companion template, outside DCM rendering. Its suffix placeholder requires literal replacement before use. The ingestion task provides a second sample-data path; neither runs during validation.

The Streamlit app uses its deployed database context and pre-installed packages. The Code Bundle notebook demonstrates packaged execution without database access. Assets receive no DCM templating. PLAN cannot establish handler or application runtime correctness.

The early-access CLI also offers `snow dcm dependencies` for a deployment dependency diagram and `snow dcm init` for interactive target/registration setup. They introduce no additional managed entity types.

## **The app has three pages and two separate bundle examples**

`sources/definitions/dashboard.sql` defines the Streamlit app. `sources/definitions/notebook.sql` defines both Code Bundles and their execution tasks. The serving views and semantic view are defined together in `sources/definitions/serve.sql`; the pipe and streams are in `sources/definitions/ingest.sql`.

The Streamlit entrypoint uses `st.navigation` to register:

* Overview.
* Daily sales.
* Category and city.

`assets/streamlit/data.py` provides bounded cached reads; the asset glob includes every page and helper file under `assets/streamlit/app_pages/`.

`PYTHON_BUNDLE` packages `assets/python_job/main.py` separately from the notebook bundle. It runs on warehouse compute with Python 3.11. The suspended `RUN_PYTHON` task supplies the target suffix and warehouse context. The script summarizes embedded synthetic order lines using only the standard library, with no database writes or network requests.

```bash
python3 assets/python_job/main.py --environment local
```

The local output contains two distinct orders and four units, totaling USD 40.00. Snowflake execution requires a later deployment; the existing notebook bundle continues to use compute-pool runtime.