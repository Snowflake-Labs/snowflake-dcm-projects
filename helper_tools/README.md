## DDL to DCM Procedure
Migrates existing Snowflake objects into SQL files with DEFINE statements.

**Supported object types:**
- Schemas
- Tables
- Views
- Dynamic Tables
- Tasks
- SQL Functions
- SQL Procedures

**Features**:
- Expands short names to fully-qualified names in object bodies
- Optionally groups all objects of the same schema + type into a single SQL file
- Reports errors and skipped objects in the result set

**Usage**:
```
   CALL GENERATE_DEFINITIONS(
       'MY_DATABASE',                -- database to scan
       NULL,                         -- schema allow-list (NULL = all schemas)
       '@MY_DB.PUBLIC.STG/src',      -- target stage or workspace path
       TRUE                          -- group by type (FALSE = one file per object)
   );
```
