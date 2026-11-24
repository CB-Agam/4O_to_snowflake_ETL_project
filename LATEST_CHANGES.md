# Latest Changes - Extraction Loop Diagnostic Fix

## Summary
We've added **comprehensive diagnostic logging** to identify exactly where the extraction loop is stopping and why.

## Changes Made

### 1. Enhanced Error Reporting
**File:** `scripts/extract_table_to_csv.py`

**What Changed:**
- Added table-by-table progress tracking: `[N/216] Processing table: <name>`
- When an error occurs, it now prints to stdout (not just logs)
- Shows exactly how many tables were processed before the error

**Example Output:**
```
[1/216] Processing table: Acc_Cheque_Payments
  Skipping Acc_Cheque_Payments: Parquet already exists...
[2/216] Processing table: Access Privileges
  ...
[20/216] Processing table: Application
  Skipping Application: 4D did not return a row count...
[21/216] Processing table: (next table)
ERROR: Extraction stopped unexpectedly: (actual error message here)
Processed 21 tables before stopping
```

### 2. New Diagnostic Tools
Created three new files to help with troubleshooting:

#### `run_diagnostic.ps1` (PowerShell Script)
One-command diagnostic that:
- Checks disk space
- Counts accessible tables from 4D
- Runs extraction with full diagnostic output
- Saves everything to a timestamped file

**Usage:**
```powershell
powershell -ExecutionPolicy Bypass -File run_diagnostic.ps1
```

#### `DEBUG_EXTRACTION_ISSUE.md` (Detailed Guide)
Complete debugging guide with:
- Problem statement
- Root cause analysis
- Common causes & solutions
- Troubleshooting steps
- Alternative run options

#### `EXTRACTION_DIAGNOSTIC_SUMMARY.md` (Quick Reference)
Quick reference card with:
- What changed
- How to use
- Expected output
- Information to collect
- Possible causes by error message

#### `scripts/verify_diagnostic_fix.py` (Verification Script)
Run this after `git pull` to confirm all fixes are in place:

**Usage:**
```powershell
python scripts/verify_diagnostic_fix.py
```

**Verifies:**
- All diagnostic logging code is present
- All supporting files exist
- Python syntax is valid

## How to Use

### Step 1: Get Latest Code
```powershell
cd C:\ETL\snowglake to 4D
git pull
```

### Step 2: Verify Deployment
```powershell
python scripts/verify_diagnostic_fix.py
```

This should output:
```
STATUS: All checks passed - Latest fixes are in place!
```

### Step 3: Run Diagnostics
```powershell
powershell -ExecutionPolicy Bypass -File run_diagnostic.ps1
```

This will create a timestamped file like `extraction_diagnostic_2026-11-24_12-34-56.txt` with the full output.

### Step 4: Share Information
Gather and share:
1. The diagnostic output file
2. Your Excel sheet with table row counts
3. Any ERROR messages shown
4. Disk space info (from diagnostic)

## What We're Looking For

When you run the diagnostic, we need to know:

1. **Which table (N of 216) causes the loop to exit?**
   - This tells us if it's a specific table or a timing/resource issue

2. **What is the exact error message?**
   - Disk space? Memory? Timeout? Specific table error?

3. **How many tables were processed before stopping?**
   - If it's always 20, that's a pattern we need to understand

4. **Your Excel sheet with row counts**
   - Which tables should be extracted?
   - Which tables have 0 rows and should be skipped?

## Expected Behavior

### Success Case: All 216 Tables Process
```
[1/216] Processing table: Acc_Cheque_Payments
...
[216/216] Processing table: Last Table Name
Extraction completed: ...
Extraction run complete: 216 of 216 tables processed
```

### Failure Case: Stops Mid-Way
```
[1/216] Processing table: Acc_Cheque_Payments
...
[20/216] Processing table: Application
ERROR: Extraction stopped unexpectedly: (error details)
Processed 20 tables before stopping
```

## If You Don't See the Error Message

The diagnostic tools will help identify if:

1. **Script is hanging** (waiting for 4D or Snowflake response)
   - Wait 10+ minutes to see if it's just slow
   - Or use `--workers 1` for sequential processing

2. **Disk space is full**
   - Check the diagnostic output for free space
   - Clean up old exports if needed

3. **Network connectivity issue**
   - Ping 4D server: `ping 172.16.0.141`
   - Check if port 19812 is open

4. **Memory exhaustion**
   - Run with fewer workers: `--workers 1`
   - Run with smaller fetch size: `--fetch-size 100`

## Commands for Manual Troubleshooting

### Run with Sequential Processing (Slower but More Stable)
```powershell
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging --workers 1
```

### Save Full Output to File
```powershell
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging 2>&1 | Tee-Object -FilePath debug_output.txt
```

### List All Accessible Tables from 4D
```powershell
python -c "from scripts.extract_table_to_csv import _discover_tables; from connections.fourd_connection import connect_to_4d, close_4d_connection; c=connect_to_4d(); t=_discover_tables(c); [print(f'{i+1}. {name}') for i,name in enumerate(t)]; close_4d_connection(c)" 2>&1
```

### Check Disk Space
```powershell
Get-Volume | Select-Object DriveLetter, Size, SizeRemaining
```

## Questions?

If you need help:
1. Run the diagnostic script: `powershell -ExecutionPolicy Bypass -File run_diagnostic.ps1`
2. Share the output file
3. Share your Excel sheet
4. Share any error messages

The diagnostic output will give us everything we need to identify the root cause.

---

**Last Updated:** 2026-11-24  
**Repository:** CB-Agam/4O_to_snowflake_ETL_project  
**Branch:** complete-extraction-fr-all-table
