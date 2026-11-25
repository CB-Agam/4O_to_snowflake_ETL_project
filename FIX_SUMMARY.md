# Fix Summary: Orphaned RUNNING Status in ETL_PIPELINE_LOG

## Issue Reported
When the extraction script stops unexpectedly, the last table being processed remains in the ETL_PIPELINE_LOG with:
- **STATUS**: `RUNNING` (not updated)
- **END_TIME**: `null` (not set)
- **ERROR_MESSAGE**: empty (not recorded)

**Example from screenshot:**
```
TABLE_NAME: *ASTERFILE
STATUS: RUNNING  <-- SHOULD BE STOPPED or FAILED
START_TIME: 2026-11-25 12:15:52.875
END_TIME: null   <-- SHOULD BE SET
MESSAGE: (empty or incomplete)
ERROR_MESSAGE: (empty)
```

This makes it impossible to know:
- ✗ What caused the script to stop
- ✗ When exactly it stopped
- ✗ What was the last table being processed

## Root Cause Analysis

**Problem Flow:**
1. Script starts processing a table (e.g., MASTERFILE)
2. Creates audit record: `table_step = audit.record(..., STATUS="RUNNING")`
3. Table processing fails (ODBC error, connection timeout, etc.)
4. Exception occurs and caught by outer exception handler
5. Script creates a NEW audit record with STATUS="STOPPED"
6. **BUT**: Original `table_step` record is left with STATUS="RUNNING"
7. Script exits without updating the original RUNNING record

**Result:** Two incomplete records:
- Original table record: STATUS=RUNNING (orphaned, never finalized)
- Process-level record: STATUS=STOPPED (created but doesn't reference the table)

## Solution Implemented

### File Changed: `scripts/extract_table_to_csv.py`

**Change 1: Track Last Table (Lines 316-318)**
```python
processed_count = 0
last_table_step = None      # NEW: Track step number of last table
last_table_name = None      # NEW: Track name of last table

for table_name, target_table, mapping_rows in selected_tables:
    ...
    # Track last table step in case of crash
    last_table_step = table_step     # NEW: Save step number
    last_table_name = table_name     # NEW: Save table name
```

**Change 2: Update RUNNING to STOPPED on Crash (Lines 476-490)**
```python
except Exception as outer_exc:
    # Update the last RUNNING table record to STOPPED if there is one
    if last_table_step is not None:
        try:
            stop_message = f"Table {last_table_name} extraction stopped unexpectedly. Reason: {str(outer_exc)}"
            audit.finish_step(
                last_table_step,              # Update the RUNNING record
                "STOPPED",                    # Set status to STOPPED
                stop_message,                 # Include detailed reason
                error_message=traceback.format_exc(),  # Include full traceback
            )
            logger.info("Updated last table record status to STOPPED: %s", last_table_name)
            print(f"Updated table {last_table_name} status to STOPPED in audit table")
        except Exception as audit_exc:
            logger.exception("Could not update last table step in audit table: %s", audit_exc)
    
    # Also record process-level stop reason
    try:
        stop_reason = f"Extraction stopped after processing {processed_count} tables. Last table: {last_table_name}. Reason: {str(outer_exc)}"
        audit.record(
            step_name, "STOPPED", f"EXTRACTION_PROCESS_{args.table}",
            stop_reason,
            error_message=traceback.format_exc(),
        )
        logger.info("Recorded extraction stop reason to audit table")
```

## Result After Fix

Now when script stops unexpectedly:

**Table-level record (last table being processed):**
```
TABLE_NAME: MASTERFILE
STATUS: STOPPED  [FIXED] (was RUNNING)
START_TIME: 2026-11-25 12:15:52.875
END_TIME: 2026-11-25 12:16:00.123  [FIXED] (was null)
MESSAGE: "Table MASTERFILE extraction stopped unexpectedly. Reason: [error details]"
ERROR_MESSAGE: [Full Python traceback]  [FIXED] (was empty)
```

**Process-level record:**
```
TABLE_NAME: EXTRACTION_PROCESS_all
STATUS: STOPPED
START_TIME: 2026-11-25 12:15:00.000
END_TIME: 2026-11-25 12:16:00.123
MESSAGE: "Extraction stopped after processing 143 tables. Last table: MASTERFILE. Reason: [error]"
ERROR_MESSAGE: [Full Python traceback]
```

## Verification

### 1. Code Syntax
```bash
python -m py_compile scripts\extract_table_to_csv.py
# Output: No errors (syntax valid)
```

### 2. Test Suite
```bash
python test_orphaned_running_fix.py
# Output: ALL TESTS PASSED [OK]
```

### 3. Query Snowflake After Script Stops
```sql
-- Check the last 2 records
SELECT 
    RUN_STEP_NO,
    TABLE_NAME,
    STATUS,
    MESSAGE,
    ERROR_MESSAGE,
    START_TIME,
    END_TIME
FROM "DW_AGAM_DEV"."UW_RAW_STG"."ETL_PIPELINE_LOG"
ORDER BY RUN_STEP_NO DESC
LIMIT 2;
```

**Expected Result:**
- Last record: TABLE_NAME = last-table-name, STATUS = STOPPED
- Previous record: TABLE_NAME = EXTRACTION_PROCESS_*, STATUS = STOPPED
- NO orphaned RUNNING records

## Benefits

1. **Complete visibility**: Always know which table caused the stop
2. **Full error details**: Complete traceback recorded in audit table
3. **Proper status transitions**: No RUNNING entries left behind
4. **Debugging aid**: Can identify exact point where extraction failed
5. **Data quality**: Can determine which tables were successfully extracted before failure
6. **Audit trail**: Complete history in Snowflake for compliance

## Impact Assessment

- **Breaking changes**: None
- **Backward compatibility**: Fully compatible
- **Performance impact**: Negligible (minimal additional code)
- **Data loss risk**: None
- **Deployment risk**: Very low (defensive coding only)

## Deployment Instructions

1. Pull latest code from repository
2. Verify syntax: `python -m py_compile scripts\extract_table_to_csv.py`
3. Run extraction: `python scripts\extract_table_to_csv.py --table all --format parquet`
4. Check ETL_PIPELINE_LOG for STOPPED status and error details
5. No additional configuration needed

## Files Modified

- **scripts/extract_table_to_csv.py** (2 changes)
  - Lines 316-318: Track last_table_step and last_table_name
  - Lines 476-490: Update RUNNING to STOPPED in exception handler

## Files Created (Documentation Only)

- **ORPHANED_RUNNING_STATUS_FIX.md** - Detailed technical documentation
- **FIX_SUMMARY.md** - This file
- **test_orphaned_running_fix.py** - Verification test suite

## Next Steps

1. Deploy the updated `extract_table_to_csv.py`
2. Run extraction and verify STOPPED records are properly recorded
3. Monitor Snowflake ETL_PIPELINE_LOG for complete audit trail
4. Refer to this documentation if script stops in future

---

**Status**: READY FOR PRODUCTION  
**Last Updated**: 2026-11-25  
**Test Result**: ALL TESTS PASSED
