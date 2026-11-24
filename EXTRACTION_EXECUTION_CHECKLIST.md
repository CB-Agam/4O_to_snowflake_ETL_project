# Remote System Extraction Checklist

**Date:** _______________  
**System:** Remote server (172.16.0.141:19812)  
**Project:** 4O to Snowflake ETL  
**Executed By:** _______________

---

## 📋 Pre-Extraction Checklist

### Environment Setup
- [ ] Navigated to: `C:\python\python\4O_to_snowflake_ETL_project`
- [ ] Activated virtual environment: `.\.venv\Scripts\Activate.ps1`
- [ ] Verified Python is active: `python --version` shows Python 3.12.x
- [ ] Pulled latest changes: `git pull` - no merge conflicts

### Storage Verification
- [ ] Checked disk space: `fsutil volume diskfree C:` - at least 50GB free
- [ ] Verified exports folder exists: `C:\python\python\4O_to_snowflake_ETL_project\exports`
- [ ] (Optional) Cleaned old exports if needed to free space

### Configuration Verification
- [ ] Checked Snowflake connection settings in `connections/snowflake_connection.py`
- [ ] Verified 4D server address: `172.16.0.141:19812`
- [ ] Confirmed audit table exists in Snowflake: `<AUDIT_TABLE>`

---

## 🚀 Extraction Execution

### Pre-Run Information
- **Tables to Extract:** 215 (Acc_User_Passwords intentionally skipped)
- **Expected Skip Count:** 1
- **Expected Duration:** 15-30 minutes
- **Export Location:** `exports/` folder (each table in UUID subdirectory)

### Run Command
```powershell
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging
```

- [ ] Copied command exactly as shown
- [ ] Ready to monitor output

### During Extraction
- [ ] Script started successfully (Snowflake connection message appears)
- [ ] 4D connection established: "4D connection successful"
- [ ] Tables are being skipped (not extracted): 14 tables (Acc_Cheque_Payments, etc.)
- [ ] Extraction working (no ERROR messages about unknown tables)
- [ ] Skipping message appears: "Skipping Acc_User_Passwords: Table is in skip list"
- [ ] Script continues to next table after Acc_User_Passwords
- [ ] No timeout errors
- [ ] No out of memory errors
- [ ] Process is running continuously (not stuck)

### Expected Log Output
```
INFO: Skipping Acc_Cheque_Payments: Parquet already exists...
INFO: Skipping Access Privileges: Parquet already exists...
...
INFO: Processing table 3/202: Acc_User_Passwords
INFO: Skipping Acc_User_Passwords: Table is in skip list (blacklisted)
INFO: Processing table 4/202: Account Joint Holders
...
(continues for 215 tables total)
```

---

## ✅ Post-Extraction Verification

### Script Completion
- [ ] Script completed without errors
- [ ] Final message shows completion (or see expected output below)
- [ ] No hanging processes (Ctrl+C not needed)
- [ ] Exit code is 0 (success)

### Export Folder Verification
```powershell
# Count exported parquet files
(Get-ChildItem -Path exports -Recurse -Filter "*.parquet" | Measure-Object).Count
```
- [ ] Result shows approximately **215 parquet files** (not 216)
- [ ] Each file is in its own subfolder with UUID

### Export Folder Size
```powershell
# Check total size
(Get-ChildItem -Path exports -Recurse | Measure-Object -Property Length -Sum).Sum / 1GB
```
- [ ] Size is approximately **1.3-1.5 GB** (or greater if more data)
- [ ] Size has increased from last run (new data was added)

### Sample File Verification
```powershell
# Verify parquet file exists for key table
Get-ChildItem -Path "exports\ACCOUNTS\*\*.parquet" -Recurse
```
- [ ] Can find parquet files for ACCOUNTS table
- [ ] Parquet files are not empty (size > 0 KB)

### Snowflake Audit Log Verification
```sql
-- Count total extraction records
SELECT COUNT(*) as total_records
FROM <AUDIT_TABLE>
WHERE CREATED_AT >= DATE(NOW() - INTERVAL '1 DAY');

-- Check for skip records
SELECT TABLE_NAME, STATUS, MESSAGE
FROM <AUDIT_TABLE>
WHERE MESSAGE LIKE '%skip list%'
ORDER BY CREATED_AT DESC;

-- Expected: 1 record for Acc_User_Passwords with status 'SKIPPED'
```
- [ ] Audit table has records from today's extraction
- [ ] Can see "Acc_User_Passwords" with status "SKIPPED"
- [ ] Skip reason is: "Table is in skip list (blacklisted)"
- [ ] Other tables show appropriate statuses (EXTRACTED, etc.)

---

## 📊 Summary of Results

### Extraction Statistics
| Metric | Expected | Actual |
|---|---|---|
| Total Tables Available | 216 | ___ |
| Skipped (Already Extracted) | 14 | ___ |
| Intentionally Skipped | 1 | ___ |
| Successfully Extracted | 215+ | ___ |
| **Parquet Files Created** | **~215** | **___** |
| Extraction Failures | 0 | ___ |
| Total Duration | 15-30 min | ___ min |

### Success Criteria
- [ ] 215+ parquet files in exports folder
- [ ] No crash with error message
- [ ] Acc_User_Passwords skipped (not extracted)
- [ ] Other problematic tables handled gracefully
- [ ] Audit log shows all skips with reason
- [ ] Process ran to completion without manual intervention

---

## 🔄 If Any Issues Occurred

### Issue: Script Stopped with Error
- [ ] Identified the error message
- [ ] Noted which table caused the error
- [ ] Added table to skip list (if needed)
- [ ] Edited: `scripts/extract_table_to_csv.py` line 31-33
- [ ] Committed changes: `git commit -m "Skip additional table: ..."`
- [ ] Pushed to repo: `git push`
- [ ] Pulled on this system: `git pull`
- [ ] Reran extraction: `python scripts\extract_table_to_csv.py ...`

### Issue: Incomplete Extraction (Missing Parquet Files)
- [ ] Verified script actually completed (check exit message)
- [ ] Counted parquet files again
- [ ] Checked for specific table folder in exports
- [ ] Verified disk space (may need to cleanup old exports)
- [ ] Reviewed error messages in output

### Issue: Slow Extraction / Timeout
- [ ] Monitored network connection to 4D server
- [ ] Checked 4D server availability: Can ping 172.16.0.141
- [ ] Verified Snowflake connection is stable
- [ ] Allowed more time (can run overnight)
- [ ] Reran extraction with patience

---

## 🎯 Final Sign-Off

### Verification Complete
- [ ] All post-extraction checks passed
- [ ] 215+ parquet files successfully exported
- [ ] Snowflake audit log confirms extraction
- [ ] No unexpected errors in logs
- [ ] Data is ready for next pipeline stage

### Next Steps
- [ ] Upload parquet files to Snowflake (if manual upload needed)
- [ ] Run downstream data transformations
- [ ] Notify team of successful extraction
- [ ] Archive extraction logs (if required)
- [ ] Document any new skip tables (if added)

---

## 📝 Notes / Comments

```
_________________________________________________________________

_________________________________________________________________

_________________________________________________________________

_________________________________________________________________
```

---

## 📞 Support Reference

**Documentation:**
- Quick Start: `REMOTE_SYSTEM_QUICK_START.md`
- Configuration: `SKIP_TABLES_CONFIGURATION.md`
- Summary: `IMPLEMENTATION_SUMMARY.md`

**Commands Reminder:**
```powershell
# Activate env and pull latest
.\.venv\Scripts\Activate.ps1
git pull

# Run extraction
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging

# Count results
(Get-ChildItem -Path exports -Recurse -Filter "*.parquet" | Measure-Object).Count

# Check folder size
(Get-ChildItem -Path exports -Recurse | Measure-Object -Property Length -Sum).Sum / 1GB
```

---

**Checklist Version:** 1.0  
**Updated:** 2026-11-24  
**Repository:** CB-Agam/4O_to_snowflake_ETL_project
