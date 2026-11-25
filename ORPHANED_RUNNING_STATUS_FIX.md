# Fix: Orphaned RUNNING Status in ETL_PIPELINE_LOG

## Problem
When the extraction script crashed or stopped unexpectedly, the last table being processed would remain in the ETL_PIPELINE_LOG with:
- **STATUS**: `RUNNING` (never updated)
- **END_TIME**: `null` (never set)
- **ERROR_MESSAGE**: empty (never recorded)

This made it impossible to know why the script stopped.

## Root Cause
When an exception occurred during table processing:
1. The `table_step` audit entry was created with STATUS = "RUNNING"
2. If an error occurred (e.g., ODBC error, connection failure), the exception handler would catch it
3. The exception handler would create a NEW audit record with STATUS = "STOPPED"
4. But the original `table_step` record was left with STATUS = "RUNNING" because we lost the reference to it in the outer exception handler

Result: Two records for the same failure, with orphaned RUNNING entries.

## Solution Implemented

### Changes to `scripts/extract_table_to_csv.py`

**1. Track the last table step (Lines 316-318)**
```python
processed_count = 0
last_table_step = None      # NEW: Track the step number of last table
last_table_name = None      # NEW: Track the name of last table
for table_name, target_table, mapping_rows in selected_tables:
    ...
    # Track last table step in case of crash
    last_table_step = table_step     # NEW: Save step number
    last_table_name = table_name     # NEW: Save table name
```

**2. Update RUNNING record to STOPPED in exception handler (Lines 476-490)**
```python
except Exception as outer_exc:
    ...
    # Update the last RUNNING table record to STOPPED if there is one
    if last_table_step is not None:
        try:
            stop_message = f"Table {last_table_name} extraction stopped unexpectedly. Reason: {str(outer_exc)}"
            audit.finish_step(
                last_table_step,              # Update the RUNNING record
                "STOPPED",                    # Set status to STOPPED
                stop_message,                 # Include stop reason
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

Now when the script stops unexpectedly:

**Table-level record (last table being processed):**
- STATUS: `STOPPED` ✅ (updated from RUNNING)
- MESSAGE: `"Table MASTERFILE extraction stopped unexpectedly. Reason: [error details]"`
- ERROR_MESSAGE: Full Python traceback ✅
- END_TIME: Set to current time ✅

**Process-level record:**
- STATUS: `STOPPED`
- MESSAGE: `"Extraction stopped after processing 143 tables. Last table: MASTERFILE. Reason: [error]"`
- Includes full traceback

## How to Verify

After running the script and it stops:

```sql
-- Check the last 2 records in ETL_PIPELINE_LOG
SELECT 
    RUN_STEP_NO,
    TABLE_NAME,
    STATUS,
    MESSAGE,
    ERROR_MESSAGE,
    START_TIME,
    END_TIME
FROM "DW_AGAM_DEV"."UW_RAW_STG"."ETL_PIPELINE_LOG"
WHERE RUN_ID = 'your-run-id'
ORDER BY RUN_STEP_NO DESC
LIMIT 2;
```

You should see:
1. **Last row**: Table with STATUS = `STOPPED` and ERROR_MESSAGE populated
2. **Second-to-last row**: Process record with STATUS = `STOPPED`

No orphaned `RUNNING` records! ✅

## Benefits

1. **Complete visibility**: Always know which table caused the stop
2. **Full error details**: Complete traceback recorded in audit table
3. **Proper status transitions**: No RUNNING entries left behind
4. **Debugging aid**: Can identify exactly when and why extraction stopped
5. **Data quality**: Can determine which tables succeeded before the stop

## Testing Recommendation

To test this fix:
```bash
# This should trigger an error on a problematic table
python scripts\extract_table_to_csv.py --table all --format parquet

# Check Snowflake
# - Last table record should have STATUS = STOPPED (not RUNNING)
# - ERROR_MESSAGE should contain full traceback
# - END_TIME should be populated
```
