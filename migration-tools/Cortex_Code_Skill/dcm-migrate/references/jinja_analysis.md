# Jinja Templating Analysis for Migrated Definitions

Reference for Step 8 of the DCM Migrate skill. Provides detection heuristics and transformation patterns for analyzing migrated definitions.

## Analysis Strategy

Run analysis AFTER successful adoption (PLAN shows zero changes, DEPLOY completed). Templating changes must be re-validated with a fresh PLAN + DEPLOY cycle.

## 1. Literal Value Parameterization

### Detection Heuristics

Scan all `.sql` files in `sources/definitions/` for literal values that appear in 3+ locations:

| Pattern | What to Look For | Suggested Variable |
|---------|------------------|--------------------|
| Database name | `DEFINE ... <DB_NAME>.SCHEMA.OBJECT` | `{{ db_name }}` or `{{ db_name }}{{env_suffix}}` |
| Warehouse references | `WAREHOUSE = <WH_NAME>` in tasks/dynamic tables | `{{ wh_name }}` or `{{ wh_base }}{{env_suffix}}` |
| Role names | `TO ROLE <ROLE_NAME>` or `TO DATABASE ROLE <DB>.<ROLE>` | `{{ admin_role }}`, `{{ reader_role }}` |
| Retention periods | `DATA_RETENTION_TIME_IN_DAYS = <N>` | `{{ retention_days }}` |
| Warehouse sizes | `WAREHOUSE_SIZE = '<SIZE>'` | `{{ wh_size }}` |
| Target lag | `TARGET_LAG = '<VALUE>'` | `{{ target_lag }}` |

### Transformation Example

Before:
```sql
DEFINE DYNAMIC TABLE ANALYTICS_PROD.SERVE.DAILY_METRICS
    TARGET_LAG = '1 hour'
    WAREHOUSE = ANALYTICS_WH_PROD
AS SELECT ...;
```

After (with `env_suffix` pattern):
```sql
DEFINE DYNAMIC TABLE ANALYTICS{{env_suffix}}.SERVE.DAILY_METRICS
    TARGET_LAG = '{{ target_lag | default("1 hour") }}'
    WAREHOUSE = ANALYTICS_WH{{env_suffix}}
AS SELECT ...;
```

### Manifest Configuration (v2)

```yaml
manifest_version: 2
type: DCM_PROJECT
default_target: 'DEV'

targets:
  DEV:
    project_name: 'ANALYTICS_DEV.PROJECTS.MY_PROJECT_DEV'
    project_owner: DCM_DEVELOPER
    templating_config: 'DEV'
  PROD:
    project_name: 'ANALYTICS.PROJECTS.MY_PROJECT'
    project_owner: DCM_PROD_DEPLOYER
    templating_config: 'PROD'

templating:
  defaults:
    env_suffix: '_DEV'
    target_lag: '1 hour'
    wh_size: 'XSMALL'
  configurations:
    DEV:
      env_suffix: '_DEV'
      target_lag: 'DOWNSTREAM'
    PROD:
      env_suffix: ''
      target_lag: '1 hour'
      wh_size: 'LARGE'
```

## 2. Structural Repetition (Macro Candidates)

### Detection Heuristics

Compare DEFINE blocks across files. Flag as macro candidates when:

- **3+ DEFINE blocks** share identical column structure but differ only in names
- **3+ GRANT blocks** follow the same role→schema→privileges pattern
- **3+ DEFINE SCHEMA blocks** share the same properties (retention, etc.)

### Scoring

| Signal | Weight |
|--------|--------|
| Identical column set (same names, types, order) across N tables | High — strong macro candidate |
| Same GRANT pattern repeated for N schemas/roles | High — strong macro candidate |
| Similar but not identical structures (e.g., shared prefix columns + unique columns) | Medium — partial macro, may not be worth it |
| Only 2 repetitions | Low — probably not worth a macro |

### Transformation Example: Repeated Grant Pattern

Before:
```sql
GRANT USAGE ON SCHEMA DB.RAW TO DATABASE ROLE DB.ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA DB.RAW TO DATABASE ROLE DB.ANALYST;

GRANT USAGE ON SCHEMA DB.SERVE TO DATABASE ROLE DB.ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA DB.SERVE TO DATABASE ROLE DB.ANALYST;

GRANT USAGE ON SCHEMA DB.ANALYTICS TO DATABASE ROLE DB.ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA DB.ANALYTICS TO DATABASE ROLE DB.ANALYST;
```

After (inline macro):
```sql
{% macro _schema_read_grants(db, schema, role) %}
GRANT USAGE ON SCHEMA {{db}}.{{schema}} TO DATABASE ROLE {{db}}.{{role}};
GRANT SELECT ON ALL TABLES IN SCHEMA {{db}}.{{schema}} TO DATABASE ROLE {{db}}.{{role}};
{% endmacro %}

{% for schema in ['RAW', 'SERVE', 'ANALYTICS'] %}
{{ _schema_read_grants(db_name, schema, 'ANALYST') }}
{% endfor %}
```

### When to Use Global Macros (sources/macros/)

Use global macros when:
- The same macro is needed in 2+ definition files
- The pattern is general enough to apply across schemas or projects

Use inline macros (prefixed with `_`) when:
- The macro is only used within a single file
- The pattern is specific to one context

## 3. Environment-Specific Logic (Conditional Candidates)

### Detection Heuristics

Flag values that typically vary between environments:

| Pattern | Dev Value | Prod Value | Jinja Pattern |
|---------|-----------|------------|---------------|
| Warehouse size | XSMALL | LARGE/XLARGE | `{{ wh_size }}` |
| Data retention | 1 day | 90 days | `{{ retention_days }}` |
| Task schedule | Disabled or long interval | Short interval | `{% if env != 'DEV' %}SCHEDULE = '...'{% endif %}` |
| Auto-suspend | 60 seconds | 300 seconds | `{{ auto_suspend }}` |

### When NOT to Templatize

- Object names that are the same across environments (only the database prefix changes)
- Column definitions — these must be identical across environments
- View/dynamic table SQL bodies — diverging logic across environments is a maintenance risk; use the same SQL everywhere

## Presentation Format

Present findings to the user as a categorized report:

```
=== Jinja Templating Analysis ===

1. VARIABLES — Parameterize literal values
   Priority: HIGH
   - "ANALYTICS_PROD" (database name) → {{ db_name }}{{env_suffix}}
     Found in: 42 definitions across 6 files
   - "ANALYTICS_WH_PROD" (warehouse) → {{ wh_base }}{{env_suffix}}
     Found in: 8 definitions (tasks + dynamic tables)

2. MACROS — Reduce structural repetition
   Priority: MEDIUM
   - Grant pattern (USAGE + SELECT per schema) repeated 5x → inline macro
   - Audit columns (CREATED_AT, UPDATED_AT, CREATED_BY) on 12 tables → column macro

3. CONDITIONALS — Environment-specific values
   Priority: LOW (apply only if multi-env is planned)
   - WAREHOUSE_SIZE = 'XLARGE' on 2 warehouses → {{ wh_size }}
   - DATA_RETENTION_TIME_IN_DAYS = 90 on 4 schemas → {{ retention_days }}

Recommended next step: Apply VARIABLES first (highest impact, lowest risk),
then MACROS if the user wants to simplify ongoing maintenance.
```

## Re-validation After Templating

After applying any Jinja changes:
1. Run `snow dcm raw-analyze` to check for Jinja syntax errors
2. Run `snow dcm plan --save-output` to verify the rendered output still produces zero changes
3. If PLAN shows differences, the Jinja transformation altered the effective SQL — revert and fix
4. Only DEPLOY after PLAN confirms zero changes
