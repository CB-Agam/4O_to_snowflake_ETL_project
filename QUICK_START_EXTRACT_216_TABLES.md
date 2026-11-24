# Quick Start: Extract All 216 Tables to Parquet

## Run Complete Extraction (Will Process All 216 Tables)

```powershell
cd C:\python\python\4O_to_snowflake_ETL_project
python scripts\extract_table_to_csv.py --table all --export-dir exports --format parquet
```

## What This Does

1. **Discovers** all 216 accessible tables from 4D via ODBC metadata
2. **Checks** each table's `exports\<TABLE_NAME>\` folder for existing Parquet files
3. **Skips** tables that:
   - Already have a Parquet file (file exists in `exports\<TABLE_NAME>\<RUN_ID>\*.parquet`)
   - Return no row count from 4D (graceful skip, not extracted)
4. **Extracts** new tables to local Parquet files in parallel (3 workers by default)
5. **Reconnects to 4D** automatically if any table extraction fails, then continues
6. **Logs everything** to Snowflake `ETL_PIPELINE_LOG` table

## Expected Behavior

**Console output will show:**
- Skipped tables (already exported, no row count)
- Failed tables (connection errors, etc.) — script will reconnect and continue
- Successful extractions (table name, row count, file path)
- Final summary: "Extraction run complete: X tables processed"

**Snowflake audit table will record:**
- RUN_ID: unique identifier for this run
- Each table with status: `SKIPPED`, `FAILED`, or `SUCCESS`
- For success: row count, file path, extraction timing

## Options

### Skip File Logging (Log Only to Snowflake)
```powershell
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging
```

### Single Table Extraction
```powershell
python scripts\extract_table_to_csv.py --table "Account Contacts" --export-dir exports --format parquet
```

### Custom Workers (1-3, default 3)
```powershell
python scripts\extract_table_to_csv.py --table all --format parquet --workers 2
```

### Custom Fetch Size (default: auto-calculated)
```powershell
python scripts\extract_table_to_csv.py --table all --format parquet --fetch-size 25000
```

## Monitor Progress

**In Snowflake (while run is in progress or after):**
```sql
-- Check latest extraction run status
SELECT TABLE_NAME, STATUS, MESSAGE
FROM ETL_PIPELINE_LOG
WHERE RUN_ID = (
    SELECT RUN_ID FROM ETL_PIPELINE_LOG
    WHERE STEP = 'PARQUET_EXTRACTION'
    ORDER BY TIME_STAMP DESC LIMIT 1
)
AND STEP = 'PARQUET_EXTRACTION'
ORDER BY START_TIME DESC
LIMIT 50;

-- Count by status
SELECT STATUS, COUNT(*) as COUNT
FROM ETL_PIPELINE_LOG
WHERE RUN_ID = (
    SELECT RUN_ID FROM ETL_PIPELINE_LOG
    WHERE STEP = 'PARQUET_EXTRACTION'
    ORDER BY TIME_STAMP DESC LIMIT 1
)
AND STEP = 'PARQUET_EXTRACTION'
GROUP BY STATUS;
```

## After Extraction Completes

**Check exports folder for new Parquet files:**
```powershell
Get-ChildItem exports -Recurse -Filter "*.parquet" | Measure-Object
```

**Next step - Upload to Snowflake:**
```powershell
python scripts\upload_parquet_exports.py --export-dir exports --archive-dir uploaded_exports
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Script stops after ~20 tables | Fixed in this update. If still happening, check that you're running the latest code. |
| "Skipping: Parquet already exists" | Delete the file from `exports\<TABLE_NAME>\<RUN_ID>\` folder if you want to re-extract. |
| "4D did not return a row count" | Table may be inaccessible. Check 4D permissions and table visibility. |
| Connection timeout | Increase `FOURD_QUERY_TIMEOUT_SECONDS` in `.env` (default 1800 seconds / 30 minutes). |
