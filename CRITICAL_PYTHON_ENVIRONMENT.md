# CRITICAL: Extraction Requires Correct Python Environment

## The Problem

You have **TWO COPIES** of the 4O_to_snowflake_ETL_project:

1. **C:\python\python\4O_to_snowflake_ETL_project** (Your working copy with `.venv`)
   - Has virtual environment with `pyarrow` installed
   - Has all required packages
   - **THIS IS WHERE YOU SHOULD RUN EXTRACTION FROM**

2. **C:\ETL\snowglake to 4D** (Git repository)
   - Has the latest diagnostic tools
   - Does NOT have `.venv` or pyarrow installed
   - Diagnostic tools are here but can't run without pyarrow

## Why Extraction Failed

When you ran the diagnostic earlier, it worked because you were in the `.venv` virtualenv at:
```
(.venv) C:\python\python\4O_to_snowflake_ETL_project>
```

This virtualenv has `pyarrow` installed, so extraction worked partially (up to table 7).

However, when tests ran from the wrong Python environment, they failed with:
```
ModuleNotFoundError: No module named 'pyarrow'
```

## The Solution

### Option 1: Run from Your Virtualenv (RECOMMENDED)

```powershell
# Navigate to your project directory
cd C:\python\python\4O_to_snowflake_ETL_project

# Activate virtualenv (should already be active)
.\.venv\Scripts\Activate.ps1

# Run extraction from HERE
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging
```

### Option 2: Use the Provided Script

```powershell
# Navigate to the git repo
cd C:\ETL\snowglake to 4D

# Run the new wrapper script that handles virtualenv
powershell -ExecutionPolicy Bypass -File run_extraction_in_venv.ps1 -ProjectPath "C:\python\python\4O_to_snowflake_ETL_project"
```

## Why Two Copies Exist

You have:
1. **Working copy** at `C:\python\python\4O_to_snowflake_ETL_project`
   - Your development/testing location
   - Has dependencies installed in `.venv`
   - Has your actual work

2. **Git repository** at `C:\ETL\snowglake to 4D`
   - Tracking repository
   - Latest diagnostic tools
   - Shared with team

**You should:**
- Run extraction from your working copy (`C:\python\python\4O_to_snowflake_ETL_project`)
- Sync diagnostic tools from git repo to your working copy
- Do `git pull` regularly to get updates

## Why Extraction Stopped After Table 7

From your diagnostic run, the extraction was working but processing tables slowly:
- Table 1-6: Processed successfully
- Table 7 (Background Images): Started processing but...
- **Unknown reason**: Script stopped without completion message

The issue is NOT the diagnostic tools - it's something happening during extraction of table 7 (or shortly after).

**Possible causes:**
1. Table 7 is very large (many rows) → Taking a long time
2. Memory issue triggered at table 7
3. Network timeout
4. Disk space running low (31.65 GB free is getting tight)
5. Specific error with table 7's schema/data

## What to Do Now

1. **Sync diagnostic tools to your working copy:**
   ```powershell
   cd C:\python\python\4O_to_snowflake_ETL_project
   git pull C:\ETL\snowglake to 4D complete-extraction-fr-all-table
   ```

2. **Check if extraction output file exists from earlier run:**
   ```powershell
   cd C:\python\python\4O_to_snowflake_ETL_project
   ls extraction_diagnostic_*.txt
   dir /s | grep extraction_diagnostic
   ```

   If found, check the end of the file:
   ```powershell
   Get-Content extraction_diagnostic_*.txt | Select-Object -Last 50
   ```

3. **Run extraction again from your virtualenv:**
   ```powershell
   cd C:\python\python\4O_to_snowflake_ETL_project
   .\.venv\Scripts\Activate.ps1
   python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging 2>&1 | Tee-Object -FilePath extraction_$(Get-Date -Format 'yyyy-MM-dd_HHmmss').txt
   ```

4. **Wait for completion (15-30 minutes)**

5. **Check the output file:**
   ```powershell
   # Last 100 lines
   Get-Content extraction_*.txt | Select-Object -Last 100
   
   # Look for ERROR messages
   Get-Content extraction_*.txt | Select-String "ERROR"
   
   # Look for completion message
   Get-Content extraction_*.txt | Select-String "Extraction run complete"
   ```

6. **Share results:**
   - The extraction output file
   - Any ERROR messages found
   - Your Excel sheet with table row counts
   - Disk space: `Get-Volume | Select-Object DriveLetter, Size, SizeRemaining`

## Important Notes

- **Do NOT delete either copy** - you need both
- **Always run extraction from your .venv** - that's where dependencies are
- **Diagnostic tools are in the git repo** - sync them regularly with git pull
- **Keep disk space free** - you have only 31.65 GB free, need at least 50+ GB for parquet files

## Questions

If you get stuck:
1. Which directory are you currently in? (Check the prompt)
2. Is `.venv` active? (Check if prompt shows `(.venv)`)
3. Can you run: `python -c "import pyarrow; print('OK')"` without errors?

---

**Key takeaway:** Always run extraction from `C:\python\python\4O_to_snowflake_ETL_project` with `.venv` activated!
