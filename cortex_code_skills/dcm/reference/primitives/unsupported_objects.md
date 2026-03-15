# Unsupported Object Types in DCM

Objects not supported by DEFINE must be managed with companion SQL scripts.

## Supported DEFINE Types

See the **Supported Entities** table in [`syntax_overview.md`](../syntax_overview.md#supported-entities) for the canonical list of object types that can be managed with DEFINE.

Everything else requires imperative SQL in companion scripts.

## Companion Scripts

These files are **optional** — only needed if your project uses object types not supported by DEFINE. Place them at the **project root** alongside `manifest.yml` (NOT in `sources/definitions/`, NOT referenced in manifest).

### `pre_deploy.sql` — Runs before `snow dcm plan`

Objects referenced by DEFINE statements that the planner validates at plan time. These must exist before `snow dcm plan` runs:
- Integrations (requires ACCOUNTADMIN)
- Network rules and policies (requires SECURITYADMIN)
- Shares (requires CREATE SHARE privilege)

Begin the file with `USE ROLE ACCOUNTADMIN;` or run with `snow sql -f pre_deploy.sql --role ACCOUNTADMIN`.

### `post_deploy.sql` — Runs after `snow dcm deploy`

Objects that depend on DEFINE'd entities and must be created after `snow dcm deploy`:
- Streams
- Alerts
- File formats
- External stages
- Semantic views

## Examples

```sql
-- pre_deploy.sql
USE ROLE ACCOUNTADMIN;
CREATE API INTEGRATION IF NOT EXISTS my_api_integration
    API_PROVIDER = aws_api_gateway
    API_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/my_role'
    API_ALLOWED_PREFIXES = ('https://my-api.example.com')
    ENABLED = TRUE;
```

```sql
-- post_deploy.sql
CREATE OR REPLACE STREAM my_stream ON TABLE my_db.my_schema.my_table;
CREATE OR REPLACE STAGE my_db.my_schema.my_ext_stage
    URL = 's3://my-bucket/path/'
    STORAGE_INTEGRATION = my_storage_integration;
```

## Important Notes

- **No Jinja**: Unlike DCM definition files, companion scripts do not support Jinja templating. `snow sql -f` has no Jinja renderer. For multi-environment setups, maintain separate files per target or use shell variable substitution.
- **No dependency management**: Unlike DEFINE'd objects, companion script objects are NOT part of DCM's dependency graph. You must manually ensure correct execution order within these files.
- **Idempotency**: Use `CREATE IF NOT EXISTS` for stable objects (integrations, network policies). Use `CREATE OR REPLACE` for evolving objects (external stages, file formats).

## Execution Order

```
pre_deploy.sql → snow dcm plan → confirm → snow dcm deploy → post_deploy.sql → post_deployment_grants.sql
```
