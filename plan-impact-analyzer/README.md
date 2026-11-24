# DCM PLAN downstream impact check (experimental)

> ⚠️ **Experimental, use at your own risk.**  
> This stored procedure is a community sample and is not a supported Snowflake feature. Its output can contain false positives and can miss impacted objects. Review the results before you rely on them.

`CHECK_DOWNSTREAM_IMPACT` reads the `plan_result.json` file of a DCM PLAN and lists the objects **outside your DCM project** that read directly from the tables and views the plan changes. A DCM PLAN shows what changes inside the project; this procedure adds a view of the consumers the plan does not cover, such as views, Dynamic Tables, and tables owned by other teams or other DCM projects.

The procedure uses Snowflake lineage (`SNOWFLAKE.CORE.GET_LINEAGE`) and the definitions of the consuming views and Dynamic Tables. It runs with caller's rights, so it only reports consumers that your current role can see.

<br>

# **Setup**

## **1. Create the procedure**

Open `check_downstream_impact.sql`, replace the database and schema in the `create schema` and `create or replace procedure` statements with a location of your choice, and run the script. The examples below use `MY_DB.TOOLS`.

The role that calls the procedure needs:

* `USAGE` on the procedure, its database, and its schema.
* Read access to the plan file location (see the next section).
* The privileges your DCM project already requires to run `SHOW ENTITIES IN DCM PROJECT`.

## **2. Create a plan file**

Run DCM PLAN with `--save-output`:

```bash
snow dcm plan --target DEV --save-output
```

The CLI writes the result to `out/plan_result.json` in your project directory. In Snowsight Workspaces, running PLAN on a DCM project writes the same file into the `out/` folder of the project in your workspace.

<br>

# **Run the check**

The procedure takes one argument: the full path to a `plan_result.json` file.

## **Plan file in a workspace**

Pass the full `snow://workspace/...` path to the file:

```sql
call MY_DB.TOOLS.CHECK_DOWNSTREAM_IMPACT(
	'snow://workspace/USER$MY_USER.PUBLIC.MY_WORKSPACE/versions/head/my_dcm_project/out/plan_result.json'
);
```

## **Plan file in a local directory**

The procedure runs inside Snowflake and cannot read files on your machine, so a local path such as `out/plan_result.json` fails. Upload the file to a stage first. Your user stage (`@~`) works without any setup:

```bash
snow stage copy out/plan_result.json @~/dcm_plans/ --overwrite
```

Then pass the stage path:

```sql
call MY_DB.TOOLS.CHECK_DOWNSTREAM_IMPACT('@~/dcm_plans/plan_result.json');
```

(`snow stage copy` uploads the file uncompressed, which the procedure needs. If you use a SQL `PUT` command instead, add `auto_compress = false`, otherwise the file is stored as `plan_result.json.gz` and cannot be read.)

If the path is wrong or your role cannot read the file, the call fails with `File does not exist or not authorized`.

<br>

# **Read the output**

The procedure returns one row per changed object and impacted object:

| Column | Content |
| --- | --- |
| `CHANGED_OBJECT` | Table or view changed by the plan. For column changes, the column name is appended, for example `SALES.RAW.ORDERS.STATUS`. |
| `IMPACTED_OBJECT` | Fully qualified name of the downstream object outside the project. |
| `ISSUE` | Severity, type of change, the affected object type and column where known, how the dependency was detected, and the expected effect. Several issues for the same pair are separated by ` \| `. |

Example:

| CHANGED_OBJECT | IMPACTED_OBJECT | ISSUE |
| --- | --- | --- |
| `SALES.RAW.CUSTOMERS.EMAIL` | `MARKETING.REPORTS.V_MAILING_LIST` | `BREAKING: DROP COLUMN affects VIEW column EMAIL (detected via column lineage). Consumer column is derived from the dropped column.` |
| `SALES.RAW.ORDERS.STATUS` | `BI.REPORTS.V_SHIPPED_ORDERS` | `LIKELY_BREAKING: DROP COLUMN affects VIEW (detected via column name found in definition). Column name appears in the consumer definition (e.g. WHERE/JOIN) but not in column lineage.` |

Severities:

* `BREAKING`: the consumer is expected to fail on its next query or refresh.
* `LIKELY_BREAKING`: the column name appears in the consumer's definition, typically in a `WHERE` or `JOIN`. The match is by name, so a column with the same name from another table in the same query produces a false positive.
* `WARNING`: the consumer received data by copy (CTAS, `INSERT ... SELECT`, `COPY`). Its existing data stays intact, but the process that loads it fails.

When nothing is impacted, the procedure returns a single row with `ISSUE` set to `No impacted downstream objects found outside the project.`

<br>

# **What the check covers**

| Planned change | Reported consumers |
| --- | --- |
| Drop a table or view | Views, Dynamic Tables, and materialized views that read from it, and tables loaded from it |
| Drop a column | Consumers that select the column or derive a column from it, consumers that use it only in `WHERE` or `JOIN` clauses, and `SELECT *` views |
| Add a column | `SELECT *` views on the table (their column list is fixed when the view is created, so a changed column count breaks them until they are recreated) |
| Change a column's data type to a non-widening type | Consumers derived from the column, reported as `WARNING` |

Changes that do not break consumers are not reported:

* Widening a type, for example `NUMBER(12,2)` to `NUMBER(14,2)` or `VARCHAR(100)` to `VARCHAR(200)`.
* Adding a column, for consumers that list their columns explicitly, and for `SELECT *` Dynamic Tables.
* Consumers that are part of the same DCM project, because the PLAN itself covers them.

<br>

# **What the check does not cover**

* **Only direct consumers:**  
  Objects that read from an impacted object (two or more hops away) are not listed.
* **Schema and database drops:**  
  The plan lists only the dropped container, so consumers of the tables and views inside it are not found.
* **Column renames:**  
  This scenario is untested. A rename may show up as a dropped and an added column, or not at all.
* **Other consumer types:**  
  Stored procedures, UDFs, semantic views, streams, shares, and tools outside Snowflake (BI tools, dbt) are not checked.
* **The process that loads a copied table:**  
  For tables filled by a task or a pipeline, the procedure reports the target table, not the task that will fail.
* **Objects your role cannot see:**  
  Lineage and definitions are read with the caller's privileges, so consumers hidden from your role are missing from the result.
* **Changes that DCM PLAN rejects:**  
  For example, narrowing a column type fails at PLAN, so no plan file exists to check.

<br>

# **Extending the check**

Ideas for adapting the procedure to your environment:

* Run it in CI after `snow dcm plan --save-output` and fail the pipeline when any `BREAKING` row is returned.
* Add the owner or contact tag of each impacted object to the output, so the right team is notified before deployment.
* Expand dropped schemas into their tables and views before the lineage lookup to cover schema drops.
