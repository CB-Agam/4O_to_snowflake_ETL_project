# 4D to Snowflake ETL

This project loads mapped data directly from 4D into Snowflake using the Excel workbook as mapping metadata.

## Documentation

See [Raw-layer extraction from 4D to Snowflake](docs/RAW_LAYER_4D_TO_SNOWFLAKE.md)
for the implemented end-to-end extraction and load flow, connection
configuration, full and incremental modes, target-table preparation, and
`ETL_PIPELINE_LOG` behavior. The diagram's editable vector source is
[RAW_LAYER_DATA_FLOW.svg](docs/RAW_LAYER_DATA_FLOW.svg).

The illustrated Microsoft Word version is available at
[RAW_LAYER_4D_TO_SNOWFLAKE.docx](docs/RAW_LAYER_4D_TO_SNOWFLAKE.docx).

## Project structure

- config/
- connections/
- scripts/
- logs/
- tests/

## Local environment

1. Copy .env.example to .env
2. Fill in the real values for your 4D and Snowflake environment
   - `FOURD_QUERY_TIMEOUT_SECONDS` is optional; it defaults to `1800` (30 minutes).
     A 4D query that exceeds this limit will be cancelled by the ODBC driver.
3. Install requirements:

```powershell
py -3.12 -m pip install -r requirements.txt
```

4. Run connection verification:

```powershell
py -3.12 scripts\verify_connections.py
```

5. Load one mapped table (for example, `ACCOUNT_CONTACTS`):

```powershell
py -3.12 scripts\run_initial_etl.py --table ACCOUNT_CONTACTS
```

Load multiple mapped tables in one run (one Snowflake and 4D connection, with a
single run ID in the audit log):

```powershell
py -3.12 scripts\run_initial_etl.py --tables ACCOUNTS_AGING_ITEM Reports Account_Contacts
```

The runner selects the worksheet matching the target table name. You can pass
`--sheet "Worksheet Name"` if the worksheet uses a different name. The ETL
inserts rows; check the Snowflake targets before rerunning to avoid duplicates.

Incremental mode is opt-in and maintains a current change-batch table plus an
append-only `_H` history table. Review [incremental loading documentation](docs/INCREMENTAL_LOADING.md)
and confirm the configured Snowflake schema is a development target before the
first run:

```powershell
py -3.12 scripts\run_initial_etl.py --table CLAIMS --incremental
```

Pipeline output is shown in the console and saved to `logs/etl_pipeline.log`.
Errors include full tracebacks; the log rotates at 5 MB and keeps up to five
backups. Treat logs as potentially sensitive and do not share them without
review.

Export a CSV report with one row per table and ETL run, including the table's
current row count, the first start time, last end time, and elapsed seconds
recorded in `ETL_PIPELINE_LOG`. The audit log table itself is excluded:

```powershell
py -3.12 scripts\export_table_load_summary.py
```

The default target is `DW_AGAM_DEV.UW_RAW_STG`; the CSV is saved under `logs/`.
Use `--database`, `--schema`, or `--output` to override these defaults. Row
counts are current counts at report time, so the same table count may appear
for multiple historical runs.

The ETL runner also creates and writes step-level audit records to
`<SNOWFLAKE_DATABASE>.<SNOWFLAKE_SCHEMA>.ETL_PIPELINE_LOG`. The table contains
a run ID, a per-run step number, table, step, status, message, full error
details, and event timestamp. Run IDs are unique per ETL invocation; order
events within a run by `RUN_STEP_NO`. The table migration removes the former
global `S_NO` column. The Snowflake log records steps after Snowflake is
connected and the log table is available; earlier failures are retained in the
local file log. Audit timestamps are recorded in UTC using 24-hour time.
Because the columns use `TIMESTAMP_NTZ`, UTC is a storage convention rather
than timezone metadata; the ETL Snowflake session is set to UTC.

For mapped source columns with 4D type `CLOB`, the initial ETL widens bounded
Snowflake `VARCHAR(n)` targets to unbounded `VARCHAR` in new table definitions
and alters existing target columns before loading. The Snowflake role must
have permission to alter those target tables.

The initial ETL excludes a target column named `STATUS` from workbook mappings
and drops an existing `STATUS` column from each selected Snowflake target table.
Dropping the column also removes its existing data.
`CREATED_TIMESTAMP` and `UPDATED_TIMESTAMP` are managed by Snowflake: new tables
create them as `NOT NULL DEFAULT CURRENT_TIMESTAMP()`, and existing tables have
nulls backfilled. Inserts explicitly use `CURRENT_TIMESTAMP()` for both fields,
so they are populated for every inserted row without relying on altering
defaults on existing tables.

## Important notes

- Never commit .env
- Never hard-code credentials in source files
- Use environment variables or a secrets manager in production
