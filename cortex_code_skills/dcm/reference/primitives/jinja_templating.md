# Jinja Templating in DCM

DCM definitions support Jinja2 templating for dynamic SQL generation. Variable values come from the `templating` section in `manifest.yml`.

## Core Syntax

| Feature | Syntax | Example |
|---------|--------|---------|
| Variables | `{{ variable_name }}` | `{{ wh_size }}` |
| Loops | `{% for item in list %}...{% endfor %}` | `{% for t in teams %}...{% endfor %}` |
| Conditionals | `{% if cond %}...{% endif %}` | `{% if env == 'PROD' %}...{% endif %}` |
| Filters | `{{ value \| filter }}` | `{{ team \| upper }}`, `{{ x \| default('XSMALL') }}` |
| Comments | `{# comment #}` | `{# not rendered #}` |

```sql
DEFINE DATABASE MY_PROJECT_{{env}};

DEFINE WAREHOUSE MY_PROJECT_WH
WITH WAREHOUSE_SIZE = '{{wh_size}}';

{% for team in teams %}
    DEFINE SCHEMA MY_DB.{{ team | upper }};
{% endfor %}

{% if enable_sandbox %}
    DEFINE SCHEMA MY_DB.SANDBOX;
{% endif %}
```

## Macros

### Inline Macros (Scoped to File)

Inline macros are only accessible within the file where they are defined. Prefix with `_` to signal local scope.

```sql
{% macro _create_schema_grants(db, schema, role) %}
    GRANT USAGE ON SCHEMA {{db}}.{{schema}} TO DATABASE ROLE {{db}}.{{role}};
    GRANT SELECT ON ALL TABLES IN SCHEMA {{db}}.{{schema}} TO DATABASE ROLE {{db}}.{{role}};
{% endmacro %}

{{ _create_schema_grants('SALES_DB', 'RAW', 'ANALYST') }}
{{ _create_schema_grants('SALES_DB', 'ANALYTICS', 'ANALYST') }}
```

### Global Macros (in `sources/macros/`)

Place macro files in `sources/macros/` to make them accessible from all definition files. Same syntax as inline macros, but without the `_` prefix convention.

SQL comments containing Jinja are rendered then treated as SQL comments: `-- Database: {{ db_name }}`.

## Dictionaries

Dictionaries are defined as list items with named properties in `manifest.yml` configurations:

```yaml
templating:
  configurations:
    PROD:
      teams:
        - name: "Marketing"
          wh_size: "MEDIUM"
          data_retention_days: 14
        - name: "Finance"
          wh_size: "X-LARGE"
          data_retention_days: 90
```

Access properties with dot notation inside loops:

```sql
{% for team in teams %}
    DEFINE SCHEMA MY_DB.{{ team.name | upper }}
        DATA_RETENTION_TIME_IN_DAYS = {{ team.data_retention_days }};
{% endfor %}
```

Dictionaries cannot be overridden at runtime via the `--variable` flag.

## Variable Resolution Hierarchy

Variables resolve in this order (later overrides earlier):

1. `templating.defaults` -- base values
2. `templating.configurations.<name>` -- configuration-specific overrides
3. Runtime `--variable` flag -- CLI execution-time overrides

## Unsupported Features

- `IMPORT` from external sources
- `EXTENDS` keyword
- `INCLUDE` keyword

## Collaboration Best Practice

In shared development environments, use unique suffixes to avoid object name conflicts:

```bash
snow dcm deploy --variable "db='DEV_JS'" -c myconnection --target DEV
snow dcm deploy --variable "db='DEV_TICKET_1234'" -c myconnection --target DEV
```

## Examples

### Loop: Schemas and Roles per Team

```sql
{% for team in teams %}
    DEFINE SCHEMA ANALYTICS_DB.{{ team | upper }};
    DEFINE DATABASE ROLE ANALYTICS_DB.{{ team | upper }}_READ;

    GRANT USAGE ON SCHEMA ANALYTICS_DB.{{ team | upper }}
        TO DATABASE ROLE ANALYTICS_DB.{{ team | upper }}_READ;
    GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS_DB.{{ team | upper }}
        TO DATABASE ROLE ANALYTICS_DB.{{ team | upper }}_READ;
{% endfor %}
```

### Macro: Reusable Team Infrastructure

```sql
{% macro create_team_infra(db, team, owner_role) %}
    DEFINE SCHEMA {{db}}.{{ team | upper }};
    DEFINE DATABASE ROLE {{db}}.{{ team | upper }}_DEVELOPER;
    DEFINE DATABASE ROLE {{db}}.{{ team | upper }}_READER;

    GRANT USAGE ON DATABASE {{db}} TO DATABASE ROLE {{db}}.{{ team | upper }}_READER;
    GRANT USAGE ON SCHEMA {{db}}.{{ team | upper }}
        TO DATABASE ROLE {{db}}.{{ team | upper }}_READER;
    GRANT SELECT ON ALL TABLES IN SCHEMA {{db}}.{{ team | upper }}
        TO DATABASE ROLE {{db}}.{{ team | upper }}_READER;
    GRANT CREATE TABLE, CREATE VIEW ON SCHEMA {{db}}.{{ team | upper }}
        TO DATABASE ROLE {{db}}.{{ team | upper }}_DEVELOPER;
    GRANT DATABASE ROLE {{db}}.{{ team | upper }}_READER
        TO DATABASE ROLE {{db}}.{{ team | upper }}_DEVELOPER;
    GRANT DATABASE ROLE {{db}}.{{ team | upper }}_DEVELOPER
        TO ROLE {{owner_role}};
{% endmacro %}

{% for team in teams %}
    {{ create_team_infra('ANALYTICS_DB', team, project_owner_role) }}
{% endfor %}
```

### Dictionary: Multi-Tenant Provisioning

Given `teams` defined as a list of dictionaries in `manifest.yml` (each with `name`, `wh_size`, `data_retention_days`, and `needs_sandbox` properties):

```sql
{% for team in teams %}
{% set team_name = team.name | upper %}

DEFINE SCHEMA CORP_DB.{{team_name}}
    DATA_RETENTION_TIME_IN_DAYS = {{ team.data_retention_days }};

DEFINE WAREHOUSE {{team_name}}_WH
WITH WAREHOUSE_SIZE = '{{ team.wh_size }}';

{% if team.needs_sandbox | default(false) %}
DEFINE SCHEMA CORP_DB.{{team_name}}_SANDBOX
    DATA_RETENTION_TIME_IN_DAYS = 1;
{% endif %}
{% endfor %}
```

### Combined: Variables, Loops, Conditionals, and Macros

```sql
{% macro _team_schema(db, team) %}
    DEFINE SCHEMA {{db}}.{{ team | upper }};
    DEFINE DATABASE ROLE {{db}}.{{ team | upper }}_READ;
    GRANT USAGE ON SCHEMA {{db}}.{{ team | upper }}
        TO DATABASE ROLE {{db}}.{{ team | upper }}_READ;
{% endmacro %}

DEFINE DATABASE PROJECT_DB_{{env_suffix}};

{% for team in teams %}
    {{ _team_schema('PROJECT_DB_' ~ env_suffix, team) }}
    {% if team == 'Finance' %}
    DEFINE TABLE PROJECT_DB_{{env_suffix}}.FINANCE.AUDIT_LOG (
        EVENT_ID NUMBER NOT NULL,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    DATA_RETENTION_TIME_IN_DAYS = 365;
    {% endif %}
{% endfor %}
```
