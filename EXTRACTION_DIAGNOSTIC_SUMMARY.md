# Extraction Loop Issue - Diagnostic Summary

## What Changed
We've added **detailed diagnostic logging** to identify exactly where and why the extraction loop is stopping.

## Files Modified
1. **scripts/extract_table_to_csv.py**
   - Added line-by-line tracking: `[N/216] Processing table: <name>`
   - Added error details when outer exception is caught
   - Shows exactly how many tables were processed before stopping

## How to Use

### Quick Start (Windows PowerShell)
```powershell
cd "C:\ETL\snowglake to 4D"
powershell -ExecutionPolicy Bypass -File run_diagnostic.ps1
```

This will:
1. Check disk space
2. Count accessible tables
3. Run extraction with full diagnostic output
4. Save everything to a timestamped log file

### Manual Run
```powershell
cd "C:\ETL\snowglake to 4D"
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging 2>&1 | Tee-Object -FilePath extraction_debug.txt
```

## What You'll See

### Success Case
```
[1/216] Processing table: Acc_Cheque_Payments
  Skipping Acc_Cheque_Payments: Parquet already exists...
[2/216] Processing table: Access Privileges
  Skipping Access Privileges: Parquet already exists...
...
[216/216] Processing table: Last Table
  Extraction completed: format=parquet table=LAST_TABLE rows=1234...
Extraction run complete: 216 of 216 tables processed
```

### Failure Case (What We're Trying to Find)
```
[1/216] Processing table: Acc_Cheque_Payments
  ...
[20/216] Processing table: Application
  Skipping Application: 4D did not return a row count...
ERROR: Extraction stopped unexpectedly: <ACTUAL ERROR MESSAGE HERE>
Processed 20 tables before stopping
```

## Information to Collect

### 1. The Diagnostic Output
Save the full output to share with support:
```powershell
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging 2>&1 > diagnostic_full.txt 2>&1
```

### 2. Disk Space Status
```powershell
# Check C: drive free space
Get-Volume | Select-Object DriveLetter, Size, SizeRemaining
```

### 3. Table List (First 30 and Last 30 from 4D)
```powershell
python -c "from scripts.extract_table_to_csv import _discover_tables; from connections.fourd_connection import connect_to_4d, close_4d_connection; c=connect_to_4d(); t=_discover_tables(c); print(f'Total: {len(t)}\n'); [print(f'{i+1}. {name}') for i, name in enumerate(t)]; close_4d_connection(c)" > table_list.txt 2>&1
```

### 4. Your Excel Sheet
Please share your Excel sheet with:
- Column 1: Table Name
- Column 2: Row Count
- Totals at bottom

This will help us understand which tables should be extracted vs skipped.

## Possible Causes (by error message)

| Error Contains | Likely Cause |
|---|---|
| disk, space, full, write | **Disk space exhausted** |
| timeout, connection, refused | **Network/4D server issue** |
| memory, out of memory | **Memory exhaustion** |
| table name | **Specific table causing crash** |
| reconnect | **4D connection unstable** |
| (no error message) | **Script hanging, Ctrl+C, or task killed** |

## Troubleshooting Steps

### If you see a disk space error:
```powershell
# Clean up old exports (after backing them up!)
Remove-Item C:\ETL\snowglake to 4D\exports\* -Recurse -Force

# Then run extraction again
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging
```

### If you see a timeout/connection error:
```powershell
# Run with single worker (slower but more stable)
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging --workers 1
```

### If the script seems to hang (no output for 5+ minutes):
- Press Ctrl+C to stop
- Run again with `--workers 1` and `--fetch-size 100`
- Check if 4D server is responding: `ping 172.16.0.141`

## Expected Output After Git Pull

After `git pull`, you should see:
1. The file `run_diagnostic.ps1` available
2. The file `DEBUG_EXTRACTION_ISSUE.md` available  
3. When running extraction, each table should show `[N/216]` prefix

## Next Steps

1. Run the diagnostic script
2. Share the output file and any ERROR messages
3. Share your Excel sheet with row counts
4. Share disk space information
5. We'll identify and fix the root cause

---

**Last Updated:** 2026-11-24  
**Repository:** CB-Agam/4O_to_snowflake_ETL_project
