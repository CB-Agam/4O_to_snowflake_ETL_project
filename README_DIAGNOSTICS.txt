================================================================================
EXTRACTION LOOP ISSUE - DIAGNOSTIC TOOLS
================================================================================

PROBLEM:
  Extraction script stops after ~20 tables (out of 216 total)
  without processing remaining tables and without error message.

SOLUTION:
  We've added comprehensive diagnostic logging to identify exactly WHERE
  and WHY the script is stopping.

================================================================================
QUICK START
================================================================================

1. Update your code:
   cd C:\ETL\snowglake to 4D
   git pull

2. Verify fixes are in place:
   python scripts/verify_diagnostic_fix.py

3. Run diagnostic:
   powershell -ExecutionPolicy Bypass -File run_diagnostic.ps1

4. Share the output file with the team

================================================================================
NEW FILES ADDED
================================================================================

Documentation:
  - README_DIAGNOSTICS.txt (this file)
  - LATEST_CHANGES.md (what changed and how to use)
  - DEBUG_EXTRACTION_ISSUE.md (detailed debugging guide)
  - EXTRACTION_DIAGNOSTIC_SUMMARY.md (quick reference)

Tools:
  - run_diagnostic.ps1 (one-command diagnostic for Windows)
  - scripts/verify_diagnostic_fix.py (verify all fixes are in place)

Changes:
  - scripts/extract_table_to_csv.py (added progress tracking and error details)

================================================================================
HOW IT WORKS
================================================================================

The diagnostic will show output like:

  [1/216] Processing table: Acc_Cheque_Payments
  [2/216] Processing table: Access Privileges
  ...
  [20/216] Processing table: Application
  ERROR: Extraction stopped unexpectedly: (error details here)
  Processed 20 tables before stopping

The ERROR line is KEY - it tells us exactly what went wrong.

================================================================================
INFORMATION WE NEED
================================================================================

Please run the diagnostic and share:

1. The diagnostic output file (timestamped, e.g., extraction_diagnostic_2026-11-24_12-34-56.txt)
   - Look for: ERROR: Extraction stopped unexpectedly: ...
   - Look for: Which table number (N of 216) was being processed

2. Your Excel sheet with table row counts
   - Column 1: Table name
   - Column 2: Row count (0 means skip, >0 means extract)
   - Total number of tables in your data

3. Disk space information
   - From the diagnostic output: C: Drive Free Space
   - Or manual check: Get-Volume | Select-Object DriveLetter, Size, SizeRemaining

4. Any specific errors you see
   - Disk full? Network timeout? Memory error?
   - 4D connection failed? Specific table causing crash?

================================================================================
MANUAL TROUBLESHOOTING
================================================================================

If diagnostic doesn't show the error, try:

Option 1: Run with single worker (slower but more stable)
  python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging --workers 1

Option 2: Save output to file
  python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging > output.txt 2>&1

Option 3: Check if 4D server is reachable
  ping 172.16.0.141

Option 4: Check disk space
  Get-Volume | Select-Object DriveLetter, Size, SizeRemaining

Option 5: Check if any files got corrupted
  dir C:\ETL\snowglake to 4D\scripts\

================================================================================
EXPECTED RESULTS
================================================================================

SUCCESS: Script processes all 216 tables
  - Final line: "Extraction run complete: 216 of 216 tables processed"
  - All tables either extracted or skipped with reason

FAILURE: Script stops mid-way
  - Final lines show: ERROR: ... 
  - Shows which table (N of 216) caused the exit
  - Shows exactly what error message was

HANGING: Script seems frozen (no output for 5+ minutes)
  - Press Ctrl+C
  - Try with --workers 1
  - Check if 4D server is responding: ping 172.16.0.141

================================================================================
QUESTIONS?
================================================================================

1. Run: python scripts/verify_diagnostic_fix.py
2. Run: powershell -ExecutionPolicy Bypass -File run_diagnostic.ps1
3. Share the diagnostic output file
4. Share your Excel sheet with row counts
5. Share any ERROR messages

That's all we need to identify and fix the problem!

================================================================================
REPOSITORY INFO
================================================================================

Repository: CB-Agam/4O_to_snowflake_ETL_project
Branch: complete-extraction-fr-all-table
Latest Commit: Diagnostic tools and documentation added

For detailed information, see:
  - LATEST_CHANGES.md
  - DEBUG_EXTRACTION_ISSUE.md
  - EXTRACTION_DIAGNOSTIC_SUMMARY.md

================================================================================
