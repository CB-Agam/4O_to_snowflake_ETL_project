# Extraction Script Fixes - Complete Extraction of 216 Tables

## Problem
The extraction script was stopping after ~20 tables instead of processing all 216 accessible tables from 4D.

**Root Causes:**
1. **Connection state corruption:** When a table extraction failed (e.g., `Acc_User_Passwords` with ODBC error `08004`), the 4D connection state became unstable
2. **No connection recovery:** The script didn't reconnect after errors, causing subsequent tables to fail silently
3. **Silent loop exit:** When critical exceptions occurred, the script would exit the processing loop without completing all tables
4. **File logging overhead:** Optional file logging could accumulate and slow processing

## Solutions Implemented

### 1. Connection Recovery After Failures
**File:** `scripts/extract_table_to_csv.py`

Added reconnection logic in the extraction exception handler:
```python
except Exception as exc:
    # ... log error ...
    exit_code = 1
    try:
        close_4d_connection(source_connection)
        source_connection = connect_to_4d()
        logger.info("Reconnected to 4D after extraction failure")
    except Exception as reconnect_exc:
        logger.exception("Failed to reconnect to 4D after extraction error")
        raise reconnect_exc
    continue  # Continue with next table
```

**Impact:** Tables after a failed extraction now proceed instead of silently stopping.

### 2. Explicit Loop Completion Logging
**File:** `scripts/extract_table_to_csv.py`

Added visibility into how many tables were prepared and processed:
```python
print(f"Prepared {len(selected_tables)} tables for extraction")
logger.info("Prepared %d tables for extraction", len(selected_tables))
# ... process all tables ...
logger.info("All %d tables processed", len(selected_tables))
print(f"Extraction run complete: {len(selected_tables)} tables processed")
```

**Impact:** You can now see if the script truly processed all discovered tables or stopped early.

### 3. Optional Snowflake-Only Logging
**Files:** `config/logging_config.py`, `scripts/extract_table_to_csv.py`

- Added `disable_file_logging` parameter to `configure_logging()`
- Added `--no-file-logging` command-line flag to extraction script

**Usage:**
```powershell
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging
```

**Impact:** Logging goes only to Snowflake `ETL_PIPELINE_LOG` table and console, skipping file I/O.

## Running the Fixed Extraction

### Standard mode (logs to file and Snowflake):
```powershell
python scripts\extract_table_to_csv.py --table all --export-dir exports --format parquet
```

### Snowflake-only mode (no file logs):
```powershell
python scripts\extract_table_to_csv.py --table all --export-dir exports --format parquet --no-file-logging
```

## Behavior After Fix

The extraction now:
1. ✅ Discovers all 216 tables from 4D
2. ✅ Skips tables already in `exports\<TABLE_NAME>\` folder
3. ✅ Skips tables where 4D returns no row count (graceful skip)
4. ✅ **Reconnects and continues** when a table fails with an error
5. ✅ Processes all remaining tables even if one fails
6. ✅ Logs everything to Snowflake `ETL_PIPELINE_LOG` audit table with RUN_ID and step status
7. ✅ Reports final count of tables processed

## Monitoring Progress

Check Snowflake audit table to verify complete run:
```sql
SELECT TABLE_NAME, STATUS, COUNT(*) as COUNT
FROM ETL_PIPELINE_LOG
WHERE RUN_ID = (
    SELECT RUN_ID FROM ETL_PIPELINE_LOG
    WHERE STEP = 'PARQUET_EXTRACTION'
    ORDER BY TIME_STAMP DESC LIMIT 1
)
AND STEP = 'PARQUET_EXTRACTION'
GROUP BY TABLE_NAME, STATUS
ORDER BY STATUS DESC;
```

Expected result: All 216 rows should be recorded with status `SKIPPED`, `SUCCESS`, or `FAILED`.

## Troubleshooting

**If extraction still stops early:**
1. Check console output for "Reconnected to 4D" messages
2. Review Snowflake audit table for failed tables
3. Run the diagnostic to confirm 4D still exposes 216 tables:
   ```powershell
   python -c "from scripts.extract_table_to_csv import _discover_tables; from connections.fourd_connection import connect_to_4d, close_4d_connection; c=connect_to_4d(); t=_discover_tables(c); print('Accessible TABLE count:', len(t)); close_4d_connection(c)"
   ```
4. Contact 4D administrator if tables are intermittently unavailable or if connection errors persist
