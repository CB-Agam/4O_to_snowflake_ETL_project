# 4D to Snowflake ETL

This project is being built incrementally. The current milestone focuses only on proving that Python can connect successfully to both the 4D database and Snowflake.

## Current milestone

The first step is to validate the two required connections before implementing extraction, transformation, staging, history, or production refresh logic.

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

The runner selects the worksheet matching the target table name. You can pass
`--sheet "Worksheet Name"` if the worksheet uses a different name. The ETL
inserts rows; check the Snowflake target before rerunning to avoid duplicates.

Pipeline output is shown in the console and saved to `logs/etl_pipeline.log`.
Errors include full tracebacks; the log rotates at 5 MB and keeps up to five
backups. Treat logs as potentially sensitive and do not share them without
review.

The ETL runner also creates and writes step-level audit records to
`<SNOWFLAKE_DATABASE>.<SNOWFLAKE_SCHEMA>.ETL_PIPELINE_LOG`. The table contains
an identity sequence number, run ID, table, step, status, message, full error
details, and event timestamp. The Snowflake log records steps after Snowflake
is connected and the log table is available; earlier failures are retained in
the local file log.

## Important notes

- Never commit .env
- Never hard-code credentials in source files
- Use environment variables or a secrets manager in production
