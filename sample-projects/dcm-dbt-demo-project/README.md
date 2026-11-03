# DCM Projects and dbt: a combined pipeline

This demo shows one way to combine DCM Projects and dbt in Snowflake. dbt models and transforms the data. DCM Projects manages everything around the transformations:

* Databases, schemas, and warehouses
* Roles and grants, including inherited grants on tables that dbt creates
* Tags, masking policies, and row access policies, attached to dbt tables
* The ingestion layer: stage, file format, four raw tables, and Snowpipe
* Data quality expectations
* Consumers of the dbt output: a Dynamic Table, a view, a semantic view, and a Streamlit app
* The task graph that loads data and runs dbt

The companion blog post explains the reasoning behind this split.

## Project layout

```text
5b_DCM_and_dbt_Demo/
├── manifest.yml                        DCM targets, variables, and the Streamlit asset
├── setup/00_admin_setup.sql            One-time setup by an administrator
├── sources/definitions/
│   ├── 01_infrastructure.sql           Phase 1: database, schemas (including STAGING and ANALYTICS for dbt), warehouse
│   ├── 02_raw_and_ingestion.sql        Phase 1: stage, raw tables, pipes, DMFs
│   ├── 03_access_and_governance.sql    Phase 1: roles, grants, tags, policies
│   ├── 04_governance_attachments.sql   Phase 2: tags and policies on dbt tables
│   ├── 05_consumers.sql                Phase 2: Dynamic Table, view, semantic view, Streamlit
│   └── 06_orchestration.sql            Phase 2: task graph that loads data and runs dbt
├── dbt/demo_pipeline/                  The dbt project (deployed with snow dbt deploy)
├── streamlit/revenue_app/              Streamlit app source, imported as a project asset
└── sample_data/                        CSV files for the landing stage
```

The phase 2 files are wrapped in `{% if not initial_deployment %}`. The variable defaults to `false` and is only set to `true` for the very first deployment of an environment, before dbt has created its tables.

## The dbt models

The dbt project follows the usual staging, intermediate, and mart layers. Staging and intermediate models go into the `STAGING` schema, which consumers can't read; marts go into `ANALYTICS`.

| Layer | Model | Materialization |
|---|---|---|
| staging | `STG_CUSTOMERS` | incremental, `merge` |
| staging | `STG_ORDERS`, `STG_ORDER_ITEMS`, `STG_PRODUCTS` | `dynamic_table` |
| intermediate | `INT_ORDER_ITEMS_ENRICHED` | `dynamic_table` |
| marts | `DIM_CUSTOMERS` | incremental, `merge`, enforced contract |
| marts | `DIM_PRODUCTS`, `FCT_ORDERS`, `CUSTOMER_ORDER_SUMMARY` | `dynamic_table` |

None of the models replaces its table on a regular run. Other parts of the project:

* `dbt_project.yml` sets the Dynamic Table defaults and the schema and tag per layer.
* `sources.yml` declares the four DCM raw tables with freshness thresholds.
* `selectors.yml` defines the `incremental_models` and `dynamic_tables` selectors that the task graph uses.
* The macros `incremental_watermark` (null-safe watermark with the `reprocess_days` variable) and `cents_to_currency`.

Every Dynamic Table uses `target_lag = 'DOWNSTREAM'` and `initialize = 'ON_SCHEDULE'`, so none of them refreshes on its own. A Dynamic Table can also refresh on a target lag such as `'1 hour'`, pulling from upstream as needed. This demo refreshes on a schedule instead: the task `REFRESH_DYNAMIC_TABLES` refreshes the last tables in the chain (`SERVE.DAILY_REVENUE`, `CUSTOMER_ORDER_SUMMARY`, `DIM_PRODUCTS`), and Snowflake refreshes everything upstream of them in the same operation.

## Setup

Requirements: Snowflake CLI 3.17 or later and a role that can run the statements in `setup/00_admin_setup.sql`.

1. **Run the admin setup.**  
   Set your user name in `setup/00_admin_setup.sql` and run it once. It creates the `DCM_DBT_DEMO_DEPLOYER` role and the DCM project objects for DEV and PROD, and enables inherited grants for the account (`FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED'`).

2. **Deploy phase 1.**  
   ```bash
   snow dcm deploy --target DEV -D "initial_deployment=true" --alias phase-1
   ```

3. **Load the sample data.**  
   ```bash
   for t in customers orders order_items products; do
     snow stage copy sample_data/$t/ @DCM_DBT_DEMO_DEV.RAW.LANDING/$t/
   done
   ```
   The first task graph run refreshes the pipes. This step is optional for the first dbt run: dbt also builds its models on top of empty raw tables.

4. **Deploy and run the dbt project.**  
   ```bash
   snow dbt deploy DCM_DBT_DEMO_DEV.ORCHESTRATION.DEMO_PIPELINE --source dbt/demo_pipeline --dbt-version 1.12.3 --default-target dev
   snow dbt execute DCM_DBT_DEMO_DEV.ORCHESTRATION.DEMO_PIPELINE run --target dev
   ```
   Use `run`, not `build`: the Dynamic Tables are created with `initialize = 'ON_SCHEDULE'` and stay empty until their first refresh, so dbt tests on them fail with `not initialized`. The task graph runs the tests after the refresh.

5. **Deploy phase 2.**  
   ```bash
   snow dcm plan --target DEV
   snow dcm deploy --target DEV --alias phase-2
   ```

6. **Run the task graph once** to load the data and fill the Dynamic Tables. Snowpipe loads asynchronously, so if the tables are still empty afterwards, run it a second time:
   ```sql
   execute task graph DCM_DBT_DEMO_DEV.ORCHESTRATION.DAILY_PIPELINE;
   ```

From now on every DCM deployment is a regular full deployment without the flag, and the task graph `ORCHESTRATION.DAILY_PIPELINE` runs the pipeline:

```text
LOAD_RAW ──┬── CHECK_FRESHNESS (dbt source freshness)                   ──┬── APPLY_DYNAMIC_TABLES (dbt run --selector dynamic_tables)
           └── BUILD_CUSTOMERS (dbt build --selector incremental_models) ──┘            │
                                                                            REFRESH_DYNAMIC_TABLES
                                                                                         │
                                                                            TEST_DYNAMIC_TABLES (dbt test --selector dynamic_tables)
```

```sql
execute task graph DCM_DBT_DEMO_DEV.ORCHESTRATION.DAILY_PIPELINE;
```

Run step 2 with `initial_deployment=true` only for a new environment. On an environment that already has its consumers, the flag removes them, and DCM PLAN lists every one of them as a `DROP`.

## Try it

* **Masking and row access:**  
  `use role DCM_DBT_DEMO_DEV_ANALYST;` and query `SERVE.CUSTOMER_DIRECTORY` (masked emails), or `SERVE.DAILY_REVENUE` and `ANALYTICS.CUSTOMER_ORDER_SUMMARY` (EMEA rows only). The `STAGING` schema isn't visible to the analyst.
* **Semantic view:**  
  `select * from semantic_view(DCM_DBT_DEMO_DEV.SERVE.SALES dimensions CUSTOMERS.REGION metrics ORDERS.TOTAL_REVENUE);`
* **dbt runs do not cause drift:**  
  Run dbt a few times, then run `snow dcm plan --target DEV`. It reports no changes.
* **A breaking dbt change:**  
  Rename `REGION` in `FCT_ORDERS.sql` (and in `CUSTOMER_ORDER_SUMMARY.sql`, which reads it), redeploy the dbt project, and run `dbt run -s FCT_ORDERS+ --full-refresh` (a regular run reports `PASS` but skips SQL-only changes to a `dynamic_table` model). `SERVE.DAILY_REVENUE` fails to refresh, and `snow dcm plan` fails and points to `05_consumers.sql`. Update the column in `05_consumers.sql` and `04_governance_attachments.sql` and deploy again. Don't run the task graph before the DCM deployment: a refreshed table without its row access policy shows the analyst every region.
* **A new column in an incremental model:**  
  Add a column to `STG_CUSTOMERS.sql`, `DIM_CUSTOMERS.sql`, and the contract in `schema.yml`. `on_schema_change = 'append_new_columns'` adds it to the existing tables, so policies and tags stay in place. Existing rows stay `NULL` until a run with `--vars '{"reprocess_days": 60}'`. A new column with personal data needs its `PII` tag and mask in `04_governance_attachments.sql`.
* **A new raw column:**  
  Add it to the raw table in `02_raw_and_ingestion.sql` and deploy. The pipes match columns by name, so files with and without the new column both load. A pipe's `COPY` statement can't be changed later, and a replacement pipe loads every staged file again.

## Clean up

```bash
snow sql -q "drop dbt project DCM_DBT_DEMO_DEV.ORCHESTRATION.DEMO_PIPELINE;"
snow dcm purge --target DEV
```

`purge` drops every object the project manages, including the database that holds the dbt tables.
