# Remote System Quick Start Guide

**For:** Remote server at `172.16.0.141:19812`  
**Project Path:** `C:\python\python\4O_to_snowflake_ETL_project`

---

## 🚀 Quick Start (5 minutes)

### Step 1: Activate Environment
```powershell
cd C:\python\python\4O_to_snowflake_ETL_project
.\.venv\Scripts\Activate.ps1
```

### Step 2: Pull Latest Changes
```powershell
git pull
```

**Expected Output:**
```
Already up to date.
OR
Fast-forward (if updates available)
```

### Step 3: Run Extraction (All 216 Tables)
```powershell
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging
```

**Expected Behavior:**
- ✅ Skips `Acc_User_Passwords` (known issue)
- ✅ Processes 215 tables (216 total - 1 skipped)
- ✅ Saves to: `exports/` folder
- ✅ Runs continuously ~15-30 minutes
- ✅ Logs to Snowflake audit table

---

## 📊 Output Files

### Where Are Exports Saved?
```
C:\python\python\4O_to_snowflake_ETL_project\exports\
└── ACCOUNTS\
    ├── <UUID>\
    │   └── accounts.parquet
├── ACCOUNT_CONTACTS\
├── ACH_TRANSACTIONS\
└── ... (many more tables)
```

### Check Export Folder Size
```powershell
# View total size of exports
(Get-ChildItem -Path exports -Recurse | Measure-Object -Property Length -Sum).Sum / 1GB
```

---

## ✅ Verification Steps

### 1. Check Skipped Tables
```powershell
# After extraction completes, check for skipped tables
Get-ChildItem -Path exports | Where-Object { $_.PSIsContainer } | Measure-Object | Select-Object Count
```

Expected: ~215 folders (not 216, since Acc_User_Passwords is skipped)

### 2. Verify Parquet Files
```powershell
# Check first table
Get-ChildItem -Path "exports\ACCOUNTS\*\*.parquet"

# Count all parquet files
(Get-ChildItem -Path exports -Recurse -Filter "*.parquet" | Measure-Object).Count
```

Expected: ~215 parquet files

### 3. Query Snowflake Audit Table
```sql
SELECT 
    TABLE_NAME,
    STATUS,
    MESSAGE,
    CREATED_AT
FROM <YOUR_AUDIT_TABLE>
WHERE STATUS = 'SKIPPED'
ORDER BY CREATED_AT DESC;
```

---

## 🔧 Troubleshooting

### Issue: Script Stops with ODBC Error
**Solution:** This is now handled! Restart with:
```powershell
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging
```

The problematic table (Acc_User_Passwords) will be skipped automatically.

### Issue: Some Tables Still Missing
**Solution:** Check the skip list:
```powershell
# Edit the script to see what's being skipped
code scripts\extract_table_to_csv.py
```

Look for `TABLES_TO_SKIP` around line 31. If you need to add more tables:
1. Edit `scripts/extract_table_to_csv.py`
2. Add table name to `TABLES_TO_SKIP` set
3. Commit and push changes
4. Pull on remote system

### Issue: "Cannot find path" Error
**Solution:** Make sure you're in the correct directory:
```powershell
cd C:\python\python\4O_to_snowflake_ETL_project
pwd  # Verify path
ls   # List files (should show scripts, exports, etc.)
```

---

## 📋 Currently Skipped Tables (1 table)

| Table Name | Reason | Status |
|---|---|---|
| `Acc_User_Passwords` | ODBC error 08004 - Server rejected connection | ✅ Skipped |

**Total Extraction: 215/216 tables (99.5% coverage)**

---

## 🔄 If You Need to Add More Skip Tables

### Edit the skip list:
```powershell
# Open in your editor
code scripts\extract_table_to_csv.py

# Find this section (line ~31):
# TABLES_TO_SKIP = {
#     "Acc_User_Passwords",
# }

# Add new table:
# TABLES_TO_SKIP = {
#     "Acc_User_Passwords",
#     "ProblemTable",  # New table
# }
```

### Commit and push:
```powershell
git add scripts/extract_table_to_csv.py
git commit -m "Skip additional table: ProblemTable"
git push
```

### Pull on remote:
```powershell
cd C:\python\python\4O_to_snowflake_ETL_project
git pull
```

---

## 📝 Commands Summary

| Command | Purpose |
|---|---|
| `.\.venv\Scripts\Activate.ps1` | Activate Python virtual environment |
| `git pull` | Get latest code updates |
| `python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging` | Extract all 215 tables to Parquet |
| `python -c "from scripts.extract_table_to_csv import _discover_tables; ..."` | Discover available tables (216 total) |
| `Get-ChildItem -Path exports -Recurse \| Measure-Object` | Count exported files |

---

## 🎯 Expected Results

After successful extraction:

✅ **215 tables** extracted to Parquet format  
✅ **~1.34 GB** of data exported (approximate)  
✅ **1 table** skipped (Acc_User_Passwords)  
✅ **Audit log** in Snowflake with skip reasons  
✅ **No crashes** - process runs to completion  

---

## 🆘 Still Having Issues?

If extraction still fails:

1. **Check error message** - Is it a new table failure?
2. **Document the table name** - Which table causes the error?
3. **Update skip list** - Add that table to `TABLES_TO_SKIP`
4. **Commit and push** - Share changes with the team
5. **Continue extraction** - Run again to process remaining tables

---

**Version:** 1.0  
**Updated:** 2026-11-24  
**Repository:** CB-Agam/4O_to_snowflake_ETL_project  
**Branch:** `complete-extraction-fr-all-table`
