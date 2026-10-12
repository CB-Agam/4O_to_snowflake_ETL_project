# 4D to Snowflake ETL

This project contains a direct 4D ODBC loader and a local-file/SFTP-to-Snowpipe loader.

## Daily full-table 4D snapshot

The daily command uses the selected worksheet in the Excel mapping workbook to read every mapped row from 4D, write a JSON array to `LOCAL_EXPORT_DIR`, load and validate that file in Snowflake, and then publish the new table snapshot. It does not require a separate 4D JSON export process.

1. Configure the 4D ODBC and Snowflake settings in `.env`, and set `LOCAL_EXPORT_DIR` to a writable local folder.
2. Run one table manually first:

   ```powershell
   python scripts\run_daily_etl.py --table "Accounts Aging Item"
   ```

   The command reads the worksheet named `Accounts Aging Item` from `4D_to_Snowflake (1-50 tables mapping).xlsx` by default. It uses its source table/column entries to query 4D and its target columns/types to map and create the Snowflake table. The full 4D result is written to a uniquely named JSON file; the file becomes visible with a `.json` extension only after the export has completed.

3. The command stages the transformed JSON in Snowflake and runs synchronous `COPY INTO` into a temporary load table. After Snowflake confirms the loaded row count matches the 4D export, the prior current table is preserved as a dated `<TABLE>_HS_YYYYMMDD_HHMMSS_<BATCH>` snapshot and the new load becomes `<TABLE>`. The first load has no previous snapshot to archive. Batch status and row counts are written to `<database>.<schema>.ETL_BATCH_AUDIT`.

This daily workflow uses direct `COPY INTO`, rather than Snowpipe, so it can wait for load completion and safely publish/archive snapshots in the same run. Snowpipe remains available for already-created JSON exports, but it only queues files asynchronously and does not extract data from 4D.

### Schedule it with Windows Task Scheduler

After the manual command succeeds, create a daily task:

1. Open **Task Scheduler** and choose **Create Basic Task**; set a daily trigger.
2. Set **Program/script** to the Python executable in the active virtual environment, for example `C:\python\env\Scripts\python.exe`.
3. Set **Add arguments** to `scripts\run_daily_etl.py --table "Accounts Aging Item"`.
4. Set **Start in** to the project folder, for example `C:\python\4O_to_snowflake_ETL_project`.
5. In the task properties, enable **Run task as soon as possible after a scheduled start is missed** if desired. The PC must be powered on and able to reach both 4D and Snowflake when the task runs.

If Snowflake uses `externalbrowser`, the task may need the Windows user logged in to complete authentication. For unattended scheduled runs, configure Snowflake key-pair authentication and register the public key on the Snowflake user. The Snowflake role needs permissions for the target table, internal stage, clone/history tables, `COPY INTO`, and `ETL_BATCH_AUDIT`.

## Existing JSON exports to Snowpipe

Use this optional path only when a separate process has already created a JSON array of source-column keyed records in `LOCAL_EXPORT_DIR`:

```powershell
python scripts\run_snowpipe.py --table "Accounts Aging Item"
```

This command searches the folder for matching JSON files, maps them using the workbook, uploads them to an internal stage, and queues Snowpipe loads asynchronously. It does not query 4D and does not perform the daily history-table swap. The optional `--source sftp` mode reads the same kind of pre-created export from SFTP.

## Direct 4D ODBC loader

`scripts\run_initial_etl.py` is the older direct insert loader. Use `scripts\run_daily_etl.py` for the full-extract JSON-and-snapshot workflow. The optional `FOURD_QUERY_TIMEOUT_SECONDS` defaults to `1800` seconds.

## Important notes

- Never commit .env
- Never hard-code credentials in source files
- Use environment variables or a secrets manager in production
- Snowpipe ingests files; it cannot connect to or extract records from 4D.
- The separate `scripts\verify_connections.py` command tests direct 4D and Snowflake connections.
