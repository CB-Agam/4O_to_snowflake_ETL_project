# Skip Tables Configuration

## Overview

You can now **skip problematic tables** without crashing the extraction process. Tables in the skip list will be:
- ✅ Skipped gracefully
- ✅ Logged to audit table
- ✅ Not cause extraction to stop
- ✅ Allow remaining 215 tables to process normally

---

## Current Skip List

**Currently skipped tables:**
```python
TABLES_TO_SKIP = {
    "Acc_User_Passwords",  # ODBC error 08004 - Server rejected connection
}
```

**Reason for skipping:**
- `Acc_User_Passwords`: Causes ODBC error 08004 "Server rejected the connection"
  - This was causing reconnection attempts and slowing down extraction
  - Now skipped gracefully so extraction continues

---

## How to Add More Tables to Skip

If you discover other problematic tables, follow these steps:

### Step 1: Identify the Problem Table
When extraction fails, look for error messages like:
```
ERROR: 4D extraction failed for SomeTable
Traceback: ... (specific error details)
```

### Step 2: Edit the Skip List
**File:** `scripts/extract_table_to_csv.py`

**Find this section (around line 32):**
```python
# Tables to skip during extraction (add more as needed)
TABLES_TO_SKIP = {
    "Acc_User_Passwords",  # ODBC error 08004 - Server rejected connection
}
```

**Add new tables:**
```python
# Tables to skip during extraction (add more as needed)
TABLES_TO_SKIP = {
    "Acc_User_Passwords",  # ODBC error 08004 - Server rejected connection
    "ProblemTable1",       # Description of why it's skipped
    "ProblemTable2",       # Another reason
}
```

### Step 3: Save and Commit
```powershell
cd C:\ETL\snowglake to 4D
git add scripts/extract_table_to_csv.py
git commit -m "Skip additional problematic tables: ProblemTable1, ProblemTable2"
git push
```

### Step 4: Pull Changes on Remote System
```powershell
cd C:\python\python\4O_to_snowflake_ETL_project
git pull
```

### Step 5: Run Extraction Again
```powershell
.\.venv\Scripts\Activate.ps1
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging
```

---

## Expected Output When Table is Skipped

When you run extraction, you'll see:
```
Skipping Acc_User_Passwords: Table is in skip list (blacklisted)
```

This appears in:
- ✅ Console output (stdout)
- ✅ Log file (if file logging enabled)
- ✅ Snowflake audit table

---

## Verification

### Check Which Tables Were Skipped
After extraction completes:

```powershell
# View the output file
Get-Content extraction_remote_*.txt | Select-String "skip list"
```

Expected lines:
```
Skipping Acc_User_Passwords: Table is in skip list (blacklisted)
```

### Query Audit Table in Snowflake
```sql
SELECT 
    TABLE_NAME,
    STATUS,
    MESSAGE,
    CREATED_AT
FROM <AUDIT_TABLE>
WHERE MESSAGE LIKE '%skip list%'
ORDER BY CREATED_AT DESC;
```

---

## If You Need to Remove a Table from Skip List

If a previously problematic table is now fixed:

**Edit** `scripts/extract_table_to_csv.py`:
```python
# BEFORE: Table is skipped
TABLES_TO_SKIP = {
    "Acc_User_Passwords",
    "FixedTable",  # <-- Remove this line
}

# AFTER: Table will now be extracted
TABLES_TO_SKIP = {
    "Acc_User_Passwords",
}
```

---

## Examples

### Example 1: Skip One Table
```python
TABLES_TO_SKIP = {
    "Acc_User_Passwords",
}
```

### Example 2: Skip Multiple Tables
```python
TABLES_TO_SKIP = {
    "Acc_User_Passwords",    # ODBC error 08004
    "BadTable",              # Corrupted data
    "AnotherBadTable",       # Network timeout
}
```

### Example 3: Skip No Tables (Empty Set)
```python
TABLES_TO_SKIP = set()  # Extract all tables
```

---

## Impact on Extraction

| Scenario | Before | After |
|---|---|---|
| Extraction encounters Acc_User_Passwords | ❌ Crashes with ODBC error | ✅ Skips and continues |
| Other tables (215+) | ❌ Partial extraction | ✅ Full extraction |
| Audit log | ❌ No record | ✅ Records skip reason |
| Total extraction time | ❌ Halted/Restarted | ✅ Continuous 15-30 min |

---

## FAQ

**Q: Will skipped tables be extracted later?**
A: No, they're permanently skipped during this run. If you remove them from the skip list and run again, they'll be processed.

**Q: Does skipping affect the Snowflake data?**
A: No. Skipped tables simply won't have new data loaded. Existing Snowflake data remains unchanged.

**Q: Can I skip tables temporarily?**
A: Yes! Remove them from TABLES_TO_SKIP, run extraction again.

**Q: How do I know which tables to skip?**
A: Look for extraction failures with specific table names and error messages.

---

## Commands for Remote System

```powershell
# 1. Navigate to your project
cd C:\python\python\4O_to_snowflake_ETL_project

# 2. Activate virtualenv
.\.venv\Scripts\Activate.ps1

# 3. Run extraction (Acc_User_Passwords will be skipped automatically)
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging 2>&1 | Tee-Object -FilePath "extraction_remote_$(Get-Date -Format 'yyyy-MM-dd_HHmmss').txt"

# 4. Check for skipped tables
Get-Content extraction_remote_*.txt | Select-String "skip list"
```

---

**Version:** 1.0  
**Updated:** 2026-11-24  
**Repository:** CB-Agam/4O_to_snowflake_ETL_project
