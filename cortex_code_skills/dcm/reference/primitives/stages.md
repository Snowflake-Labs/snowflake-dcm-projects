# Stages in DCM

## Syntax

```sql
DEFINE STAGE database_name.schema_name.stage_name
    [DIRECTORY = (ENABLE = TRUE)]
    [FILE_FORMAT = (TYPE = 'format' ...)]
    [ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')]
    [COPY_OPTIONS = (ON_ERROR = 'option' ...)]
    [COMMENT = 'description'];
```

This syntax applies to **internal stages only**. External stages (S3, Azure, GCS) are not supported via `DEFINE` and must be placed in `post_deploy.sql` instead. See `primitives/unsupported_objects.md` for details.

## Supported Changes

- `DIRECTORY` table settings (enable/disable)
- `COMMENT`

## Immutable

- `ENCRYPTION` type cannot be changed after creation. The stage must be dropped and recreated to change encryption.

## Decision Guide: Internal vs External Stage

If the stage definition includes a `URL` parameter (e.g., `URL = 's3://bucket/path'`), it is an **external stage** and cannot use `DEFINE STAGE`. Place external stage definitions in `post_deploy.sql` instead.

| Stage Type | Has URL? | DCM Approach |
|------------|----------|--------------|
| Internal | No | `DEFINE STAGE` |
| External (S3/Azure/GCS) | Yes | `post_deploy.sql` |

## Examples

### Basic Example

```sql
DEFINE STAGE FINANCE_DB.RAW.UPLOAD_STAGE
    COMMENT = 'Internal stage for file uploads';
```

### With Directory Table and File Format

```sql
DEFINE STAGE FINANCE_DB.RAW.CSV_LANDING
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1)
    COMMENT = 'CSV landing stage with directory tracking';
```

### With Jinja Templating

```sql
DEFINE STAGE ETL_DB{{env_suffix}}.RAW.DATA_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Encrypted upload stage for {{env_suffix}} environment';
```

### File Formats and External Stages

For file formats and external stages, see `primitives/unsupported_objects.md`. File formats are stateless — `CREATE OR REPLACE` in `post_deploy.sql` is safe.
