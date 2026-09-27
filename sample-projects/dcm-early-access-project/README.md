# Early-access DCM demo project

This standalone project includes the full public baseline plus the entities in the [DCM early-access reference](https://docs.snowflake.com/en/LIMITEDACCESS/dcm-projects/dcm-projects-early-access). See the [repository overview](../../README.md#standalone-sample-projects) to compare the public and early-access samples.

## **Early-access targets use separate names**

The [project-owner setup SQL](../dcm_project_owner_privileges_setup.sql) creates shared setup resources, enables inherited grants, and contains manual administrator grants for the DEV and PROD deployer roles. It runs outside DCM and grants broad account-level administration privileges; review the shared setup and run only the applicable role section in each target account.

The DEV target manages `DCM_DEMO_1_EA_DEV`, and the PROD targets manage `DCM_DEMO_1_EA_PROD` in their respective accounts. Registration names differ from the public sample. Each target requires its own pre-existing DCM project under `DCM_DEMO.PROJECTS`.

Portable placeholders requiring configuration:

* Account identifiers and the registration owner roles.
* An existing `user_name` for the read-role grant.
* An accessible Streamlit `compute_pool`.
* The literal compute pool/runtime in `assets/notebook_job/code_bundle.yml`.
* Existing alert infrastructure: `DCM_WH`, `dcm_demo_notification`, and a registered email recipient in `monitoring.sql`.

The public baseline's grant authority and inherited-grants opt-in requirements apply here too. The owner needs authority to delegate account-level task and DMF execution privileges. This project does not enable account features or provision administrator privileges. The account must additionally have the private-preview capabilities enabled.

## **Runtime secrets stay outside the project files**

The manifest declares:

* `BUILD_NUMBER`, a non-sensitive environment variable included in a comment.
* `DEMO_API_KEY`, the generic-string secret value.
* `DEMO_PASSWORD`, the password secret value.
* `DEMO_OAUTH_REFRESH_TOKEN`, required when the OAuth integration example is enabled.

Values can be supplied through the invoking shell or an ignored `.env` file. `.env.example` contains only non-sensitive configuration. Real values must remain outside version control. Every sensitive SQL property uses `_snow.env_secret()`, so it renders as a secret reference. The sample contains no function that returns a secret's value.

From this directory, after configuring targets and supplying values:

```bash
snow dcm compile -c MY_CONNECTION --target DCM_DEV --save-output
snow dcm plan -c MY_CONNECTION --target DCM_DEV --save-output
# With a deployment baseline:
snow dcm plan -c MY_CONNECTION --target DCM_DEV --delta --save-output
```

An early-access CLI is needed for `compile` and the documented CLI enhancements. CLI 3.24 or later supports environment value supply. `--env-file .env` is available for PLAN on builds exposing that option; shell variables avoid depending on that flag. No secret values belong in `--variable` arguments.

Optional secret variants:

* `oauth_integration`: existing API authentication integration for the OAuth secret, with the refresh token supplied separately.
* `cloud_token_integration`: existing API authentication integration for the cloud-provider token secret.

An empty integration name excludes its variant. Integrations are configured outside DCM. The four secret types shown match the supplied DCM early-access page.

`include_secrets` defaults to `true`. For partial validation without runtime values, `-D "include_secrets=false"` omits `early_access_secrets.sql` statements. Such a run does not validate secrets or runtime environment value supply. This override is intended for validation of a new registration: deploying it against a project that already manages secrets would remove those definitions and plan their deletion.

## **Masking attachments and dbt configuration have explicit boundaries**

The sample attaches masking policies to synthetic table, view, and dynamic-table columns. Conditional masking demonstrates `USING`. DCM applies masking attachments with `FORCE`, so deployment can replace a manually attached policy; removal detaches the managed policy without restoring an earlier one. The row access policy remains definition-only.

An optional existing Iceberg target is enabled with `iceberg_table` and `iceberg_column` (a varchar column, default `CITY`). No Iceberg table is created by the sample. The public baseline also offers an optional external-table stream through `external_table`.

`sources/dbt/tasty_dbt/profiles.yml` is independent of DCM templating. Its DEV and PROD databases and warehouses match the manifest's shipped suffixes. Changes to those names or owner roles need matching profile changes. The dbt sources resolve from `target.database`. DCM-managed objects have no dependencies on dbt-produced models.

PLAN does not compile or execute dbt models. Deployment compiles the dbt project. The suspended `DBT_RUN` and `DBT_TEST` tasks execute the project later. The separate Task Graph Object has exactly one root and defaults its members to suspended.

Both seed-data paths include every menu ID referenced by the supplied order details, including the optional lowercase-city examples. The additional menu names and prices are synthetic demo values. The dbt source relationship test remains enabled to catch unmatched IDs in externally loaded data. For an existing deployment, deploy the updated task definition and run `INSERT_SAMPLE_DATA` to add missing menu entries before rerunning dbt tests; its MENU insert preserves existing IDs. The manual seed script is intended for a fresh load, not an idempotent repair.

## **Validation does not deploy or run scheduled work**

The existing ingestion tasks and alert declare `STARTED`; a later deployment starts those schedules. New notebook, dbt, and Task Graph Object examples are suspended. Dynamic tables and DMFs use their declared refresh/monitoring schedules. The analytics baseline also requires Cortex access for its existing `AI_COMPLETE` call.

The network/authentication policies remain unassigned, and the share has no consumer accounts. The network policy allows all IPv4 traffic if assigned. The external stage and manual pipe are syntax examples without a guaranteed input dataset.

`SQL_post_scripts/insert_sample_data.sql` is a manual companion template, outside DCM rendering. Its suffix placeholder requires literal replacement before use. The ingestion task provides a second sample-data path; neither runs during validation.

The Streamlit app uses its deployed database context and pre-installed packages. The Code Bundle notebook demonstrates packaged execution without database access. Assets receive no DCM templating. Compile and PLAN cannot establish handler or application runtime correctness.

The early-access CLI also offers `snow dcm dependencies` for a deployment dependency diagram and `snow dcm init` for interactive target/registration setup. They introduce no additional managed entity types.

## **The app has three pages and two separate bundle examples**

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