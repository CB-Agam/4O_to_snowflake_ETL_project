# ETL Pipeline Status Recording - Final Solution

## Problem Statement

Your ETL extraction script was stopping unexpectedly while processing 216 tables, leaving orphaned records in the ETL_PIPELINE_LOG audit table with:
- **STATUS = RUNNING** (never updated)
- **END_TIME = null** (never set)  
- **ERROR_MESSAGE = empty** (never recorded)

This made it impossible to debug why the script stopped.

## Solution Overview

This fix implements two key improvements:

### 1. Skip Tables Feature (Already Implemented ✓)
- Added `TABLES_TO_SKIP` set to avoid processing problematic tables
- Currently skips tables that cause ODBC errors (Acc_User_Passwords, Application, etc.)
- Script continues to next table instead of crashing
- Skipped tables logged as "SKIPPED" status in audit table

### 2. Orphaned RUNNING Status Fix (Just Implemented ✓)
- Tracks the last table being processed
- When script crashes, updates the last RUNNING record to STOPPED
- Records complete error details and stack trace
- Prevents orphaned RUNNING records

## Changes Made

### File: `scripts/extract_table_to_csv.py`

**Change 1: Track Last Table Step (Lines 316-318)**
```python
processed_count = 0
last_table_step = None      # Track step ID of last table
last_table_name = None      # Track name of last table
for table_name, target_table, mapping_rows in selected_tables:
    ...
    last_table_step = table_step     # Update tracking
    last_table_name = table_name     # Update tracking
```

**Change 2: Update RUNNING to STOPPED on Crash (Lines 476-490)**
When script crashes, the outer exception handler now:
1. ✓ Updates the last table record from RUNNING to STOPPED
2. ✓ Sets END_TIME to current time
3. ✓ Records the error message and full stack trace
4. ✓ Records process-level stop reason

## Expected Behavior

### Before Fix
When script stops at MASTERFILE table:
```
ETL_PIPELINE_LOG Record:
  TABLE_NAME: MASTERFILE
  STATUS: RUNNING        [Problem: stuck in this state]
  END_TIME: null         [Problem: not set]
  ERROR_MESSAGE: (empty) [Problem: no error details]
```

### After Fix
When script stops at MASTERFILE table:
```
ETL_PIPELINE_LOG Record 1 (Table level):
  TABLE_NAME: MASTERFILE
  STATUS: STOPPED        [Fixed: properly updated]
  END_TIME: 2026-11-25 12:16:00  [Fixed: set to crash time]
  ERROR_MESSAGE: (full Python traceback)  [Fixed: includes error]
  MESSAGE: "Table MASTERFILE extraction stopped unexpectedly. Reason: [error details]"

ETL_PIPELINE_LOG Record 2 (Process level):
  TABLE_NAME: EXTRACTION_PROCESS_all
  STATUS: STOPPED
  MESSAGE: "Extraction stopped after processing 143 tables. Last table: MASTERFILE..."
  ERROR_MESSAGE: (full Python traceback)
```

## How to Use

### Run Normal Extraction
```bash
cd C:\ETL\snowglake to 4D
.\.venv\Scripts\Activate.ps1
python scripts\extract_table_to_csv.py --table all --format parquet
```

The script will:
1. ✓ Automatically skip problematic tables
2. ✓ Continue extracting remaining tables
3. ✓ Log progress for each table
4. ✓ Record all results in Snowflake ETL_PIPELINE_LOG

### Monitor Execution
```sql
-- View last 5 processing records
SELECT 
    RUN_STEP_NO, TABLE_NAME, STATUS, 
    MESSAGE, START_TIME, END_TIME
FROM "DW_AGAM_DEV"."UW_RAW_STG"."ETL_PIPELINE_LOG"
ORDER BY RUN_STEP_NO DESC
LIMIT 5;

-- Find all STOPPED records
SELECT 
    RUN_ID, TABLE_NAME, STATUS, 
    MESSAGE, ERROR_MESSAGE, START_TIME, END_TIME
FROM "DW_AGAM_DEV"."UW_RAW_STG"."ETL_PIPELINE_LOG"
WHERE STATUS = 'STOPPED'
ORDER BY TIME_STAMP DESC
LIMIT 10;

-- Find why last extraction stopped
SELECT 
    TABLE_NAME, STATUS, MESSAGE, ERROR_MESSAGE, END_TIME
FROM "DW_AGAM_DEV"."UW_RAW_STG"."ETL_PIPELINE_LOG"
WHERE STATUS = 'STOPPED'
ORDER BY TIME_STAMP DESC
LIMIT 1;
```

## Testing

### Verify Syntax
```bash
python -m py_compile scripts\extract_table_to_csv.py
# Output: [OK] No syntax errors
```

### Run Test Suite
```bash
python test_orphaned_running_fix.py
# Output: ALL TESTS PASSED [OK]
```

### Verify Fix in Snowflake
After running extraction, check:
1. ✓ No RUNNING records with null END_TIME
2. ✓ All STOPPED records have ERROR_MESSAGE populated
3. ✓ Last table record shows exact stop reason
4. ✓ Process-level record shows total tables processed

## Troubleshooting

### Script Still Stops?
1. Check ETL_PIPELINE_LOG for the STOPPED record
2. Read ERROR_MESSAGE field for the actual error
3. Add table to TABLES_TO_SKIP if it's a known issue
4. Contact support with the error message

### RUNNING Record Still Appears?
This should NOT happen with the fix. If it does:
1. Run the test: `python test_orphaned_running_fix.py`
2. Verify you're using the updated script
3. Check if script actually crashed (check Windows error logs)
4. Restart script and try again

### Need to Add More Skip Tables?
Edit `scripts/extract_table_to_csv.py` lines 31-33:
```python
TABLES_TO_SKIP = {
    "Acc_User_Passwords",      # Existing
    "Application",              # Existing
    "YourProblematicTable",    # Add new tables here
}
```

## Deployment Checklist

- [x] Code updated: `scripts/extract_table_to_csv.py`
- [x] Syntax validated: `py_compile` passes
- [x] Tests created and passing: `test_orphaned_running_fix.py` - ALL TESTS PASSED
- [x] Documentation created: This file
- [x] Ready for production: YES

## Implementation Status

| Component | Status | Notes |
|-----------|--------|-------|
| Skip Tables Feature | ✓ COMPLETE | Already working, can add more tables as needed |
| Last Table Tracking | ✓ COMPLETE | Added to exception handler |
| RUNNING to STOPPED Update | ✓ COMPLETE | Updates via audit.finish_step() |
| Error Message Recording | ✓ COMPLETE | Full traceback recorded |
| END_TIME Setting | ✓ COMPLETE | Set to current time on crash |
| Testing | ✓ COMPLETE | All tests pass |
| Documentation | ✓ COMPLETE | Complete documentation provided |

## Files

### Code Files
- **scripts/extract_table_to_csv.py** - Main extraction script (UPDATED)
- **etl/audit.py** - Audit logging (unchanged)
- **etl/parallel_parquet_export.py** - Parquet export (unchanged)

### Documentation Files
- **FIX_SUMMARY.md** - Technical summary of changes
- **ORPHANED_RUNNING_STATUS_FIX.md** - Detailed technical documentation
- **README_ETL_STATUS_RECORDING.md** - This file

### Test Files
- **test_orphaned_running_fix.py** - Verification test suite (ALL TESTS PASSED)

## Next Steps

1. **Immediate**: Review this documentation
2. **Deploy**: Pull the updated `scripts/extract_table_to_csv.py`
3. **Validate**: Run extraction and verify ETL_PIPELINE_LOG records
4. **Monitor**: Check Snowflake for proper status recordings
5. **Archive**: Save these documentation files for future reference

## Support

If script stops unexpectedly:
1. Check ETL_PIPELINE_LOG for STOPPED record
2. Read the MESSAGE and ERROR_MESSAGE fields
3. If it's a known table, add to TABLES_TO_SKIP
4. If it's a new error, investigate the ERROR_MESSAGE traceback
5. Restart extraction with `python scripts\extract_table_to_csv.py --table all --format parquet`

---

**Solution Status**: ✓ COMPLETE AND TESTED  
**Last Updated**: 2026-11-25  
**Test Results**: ALL TESTS PASSED [OK]  
**Ready for Deployment**: YES
