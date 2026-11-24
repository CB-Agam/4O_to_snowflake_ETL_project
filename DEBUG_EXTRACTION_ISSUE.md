# Debugging the Extraction Loop Exit Issue

## Problem Statement
The extraction script runs for ~19-20 tables (216 total), then stops silently without processing the remaining tables.

## What We Know
- 216 tables are accessible in 4D database
- Some tables are skipped: `Parquet files already exist` or `No row count returned by 4D`
- Some tables fail: `Acc_User_Passwords` with ODBC error (recovers with reconnection)
- After ~19-20 tables processed, the script stops with **NO completion message**

## Root Cause Investigation

The issue is that **when an exception escapes the main table processing loop, it's caught by the outer exception handler**, which logs the error but doesn't print it to stdout. This makes it appear as if the script "just stopped".

**Latest diagnostic:** We've added detailed logging to identify WHICH table causes the exit.

## How to Run the Diagnostic

### Step 1: Pull the latest code
```powershell
cd C:\ETL\snowglake to 4D
git pull
```

### Step 2: Run extraction with new diagnostic output
```powershell
cd C:\ETL\snowglake to 4D
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging 2>&1 | Tee-Object -FilePath extraction_diagnostic.txt
```

This will create `extraction_diagnostic.txt` with the full output.

### Step 3: Look for these patterns in the output

The new code will now print:
```
[1/216] Processing table: Acc_Cheque_Payments
[2/216] Processing table: Access Privileges
[3/216] Processing table: Account Accumulations
...
[20/216] Processing table: Application
ERROR: Extraction stopped unexpectedly: <ACTUAL ERROR HERE>
Processed 20 tables before stopping
```

**IMPORTANT:** Look for the ERROR message! This tells us WHY the loop exited.

## Common Causes & Solutions

### Cause 1: Disk Space Full
**Error message might contain:** "disk", "space", "drive", "full"
**Solution:** 
```powershell
# Check disk space
Get-Volume | Where-Object {$_.DriveLetter -eq 'C'} | Select-Object SizeRemaining, Size
dir C:\python\python\4O_to_snowflake_ETL_project\exports
```

### Cause 2: Network Connection Timeout
**Error message might contain:** "timeout", "connection", "refused", "network"
**Solution:**
- Verify 4D server is reachable: `ping 172.16.0.141`
- Check if 4D server port 19812 is accessible
- Try increasing timeout in `config/settings.py`

### Cause 3: Memory Exhaustion
**Error message might contain:** "memory", "MemoryError", "out of memory"
**Solution:**
- Run with fewer parallel workers: `--workers 1`
- Reduce fetch size: `--fetch-size 100`

### Cause 4: Specific Table Issue
**Error message contains the table name that's failing**
**Solution:**
- Skip that table or investigate its data structure
- Check if it has corrupted data or unusual schema

## If Error Message is Not Shown

If you still don't see an ERROR message after "Processed N tables", then:

1. **The script might be hanging (not crashing)**
   - Wait 10+ minutes to see if it's just slow
   - Or run with `--workers 1` to process tables sequentially

2. **The script might be exiting due to Ctrl+C or external interrupt**
   - Check if someone ran Ctrl+C
   - Check Windows Task Manager if process is still running

3. **Check the logs** (if file logging is enabled):
   ```powershell
   # Find the most recent log file
   ls C:\ETL\snowglake to 4D\logs\* | Sort-Object LastWriteTime -Descending | Select-Object -First 1
   # View it
   Get-Content (ls C:\ETL\snowglake to 4D\logs\* | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
   ```

## What to Share With Support

1. **The extraction_diagnostic.txt output** (from Step 2)
2. **Your Excel sheet** with:
   - Table names
   - Row count for each table (0 means skip)
   - Total number of tables in the sheet
3. **Disk space info:**
   ```powershell
   Get-Volume | Format-Table
   ```
4. **Which table is in position 20** (after "Application")?
   - Run: `python -c "from scripts.extract_table_to_csv import _discover_tables; from connections.fourd_connection import connect_to_4d, close_4d_connection; c=connect_to_4d(); t=_discover_tables(c); print(f'\n'.join(f'{i+1}. {name}' for i,name in enumerate(t[:30])))" 2>&1 | head -40`

## Alternative: Run With Reduced Parallelism

If the issue is related to concurrency:

```powershell
# Run with 1 worker (sequential, slower but more stable)
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging --workers 1
```

## Alternative: Run With Logging Disabled

If file logging is causing issues:

```powershell
# Already using this in the diagnostic command
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging
```

---

## Next Steps

1. **Run the diagnostic command** (Step 2 above)
2. **Share the error message** that appears
3. **Share your Excel sheet** with table row counts
4. **Share disk space info** (command above)

With this information, we can identify the root cause and fix it properly.
