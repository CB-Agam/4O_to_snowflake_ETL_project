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
   - `FOURD_QUERY_TIMEOUT_SECONDS` defaults to `1800` (30 minutes). Set it to
     `0` to disable the client-side 4D query timeout; the ODBC driver or server
     may still impose its own limits.
   - `SNOWFLAKE_NETWORK_TIMEOUT_SECONDS` defaults to `0` (no client-side
     timeout for Snowflake requests). Set a positive value to enable a timeout.
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

Full loads extract and stage rows in batches of 50,000 by default. Adjust the
batch size when needed:

```powershell
py -3.12 scripts\run_initial_etl.py --table Alerts --batch-size 50000
```

## Single-file Parquet end-to-end ETL

Run the complete export, raw load, and typed transform flow with one command.
The existing `run_initial_etl.py` command remains unchanged.

```powershell
.\env_etl\Scripts\python.exe scripts\run_parquet_etl.py `
  --table "User_Defined_Form_Builds" `
  --fetch-size 50000 `
  --export-dir exports
```

For multiple tables, pass their mapping worksheet/table names. Processing
continues after an individual table fails, and the command exits with code 1
if any table failed:

```powershell
.\env_etl\Scripts\python.exe scripts\run_parquet_etl.py `
  --tables "Account Contacts" "User_Defined_Form_Builds" `
  --fetch-size 50000 `
  --export-dir exports
```

The runner uses one 4D connection, one Snowflake connection, and one run ID
for all selected tables. Each table gets one completed Parquet file, is loaded
into its `<TARGET_TABLE>_RAW` VARCHAR landing table, and then has valid rows
published to the mapped target. Raw rows stay available for correction;
invalid mapped values are recorded in `ETL_DATA_QUALITY_ERRORS`. Existing
target snapshots are preserved under generated `<TARGET_TABLE>_HIST_<ID>`
names. Per-table extraction, load, transform, row counts, file details, and
errors are recorded in `ETL_PIPELINE_LOG`; load and transform details are also
tracked in `ETL_PARQUET_FILE_LOADS`.

The runner uses `SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA` from `.env`; pass
`--database` and `--schema` to override them. The mapping worksheet is inferred
from each table name. For a single table with a differently named worksheet,
pass `--sheet "Worksheet Name"`.

To run each stage separately (for example, when resuming from an already
completed export), use the commands below.

The individual commands together export one complete table from 4D to one
local Parquet file, upload and copy it into a Snowflake raw landing table, and
transform valid values into the mapped production table.
The Snowflake command writes to `<TARGET_TABLE>_RAW`, where mapped data fields
are `VARCHAR` so source values remain available for validation and cleansing.
The transform command publishes a full snapshot of valid rows and records
conversion failures in `ETL_DATA_QUALITY_ERRORS`. If a target already exists,
the previous snapshot is preserved under a generated `<TARGET_TABLE>_HIST_<ID>`
name.

For `User_Defined_Form_Builds`, run from the repository root in PowerShell:

```powershell
$runId = [guid]::NewGuid().ToString()
.\env_etl\Scripts\python.exe scripts\export_table_to_parquet.py `
  --table "User_Defined_Form_Builds" `
  --sheet "User_Defined_Form_Builds" `
  --fetch-size 50000 `
  --export-dir exports `
  --run-id $runId

$parquetFile = "exports\USER_DEFINED_FORM_BUILDS\$runId\user_defined_form_builds.parquet"
.\env_etl\Scripts\python.exe scripts\load_parquet_to_snowflake.py `
  --table "User_Defined_Form_Builds" `
  --sheet "User_Defined_Form_Builds" `
  --file $parquetFile

.\env_etl\Scripts\python.exe scripts\transform_parquet_raw.py `
  --table "User_Defined_Form_Builds" `
  --sheet "User_Defined_Form_Builds" `
  --file $parquetFile
```

The load command uses `SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA` from `.env`
unless `--database` and `--schema` are supplied. It creates/reuses an internal
stage, a `USER_DEFINED_FORM_BUILDS_RAW` landing table, and the
`ETL_PARQUET_FILE_LOADS` file ledger. It verifies Parquet metadata and COPY row
counts, records the file SHA-256 to prevent reloading the same successful file,
and keeps raw date/numeric values as text for subsequent validation. The
transform command uses safe Snowflake conversions, loads only rows whose
mapped values convert successfully, and writes invalid columns with their
original row values to `ETL_DATA_QUALITY_ERRORS`. Raw landing rows remain
available for correction and reprocessing.

Load multiple mapped tables in one run (one Snowflake and 4D connection, with a
single run ID in the audit log):

```powershell
py -3.12 scripts\run_initial_etl.py --tables ACCOUNTS_AGING_ITEM Reports Account_Contacts
```

The runner selects the worksheet matching the target table name. You can pass
`--sheet "Worksheet Name"` if the worksheet uses a different name. The ETL
appends rows to the target; check the Snowflake targets before rerunning to
avoid duplicates.

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

The ETL runner also creates and writes step-level audit records to
`<SNOWFLAKE_DATABASE>.<SNOWFLAKE_SCHEMA>.ETL_PIPELINE_LOG`. The table contains
a run ID, a per-run step number, table, step, status, message, full error
details, and event timestamp. Run IDs are unique per ETL invocation; order
events within a run by `RUN_STEP_NO`. The table migration removes the former
global `S_NO` column. The Snowflake log records steps after Snowflake is
connected and the log table is available; earlier failures are retained in the
local file log.

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
