#!/usr/bin/env python
"""
Simple diagnostic: Check if extraction loop is exiting prematurely.

This script does NOT require pyarrow, snowflake, or pyodbc.
It only checks connection status and table discovery.

Usage:
  python scripts\check_extraction_status.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

print("\n" + "=" * 80)
print("EXTRACTION STATUS DIAGNOSTIC")
print("=" * 80 + "\n")

# Step 1: Check Python environment
print("[STEP 1] Python Environment")
print(f"  Python version: {sys.version}")
print(f"  Executable: {sys.executable}")
print(f"  Project root: {PROJECT_ROOT}\n")

# Step 2: Check if critical files exist
print("[STEP 2] Critical Files")
files_to_check = [
    "scripts/extract_table_to_csv.py",
    "config/logging_config.py",
    "config/settings.py",
    "etl/parallel_parquet_export.py",
    "connections/fourd_connection.py",
    "connections/snowflake_connection.py",
]

all_files_exist = True
for file in files_to_check:
    path = PROJECT_ROOT / file
    exists = path.exists()
    status = "[OK]" if exists else "[MISSING]"
    print(f"  {status} {file}")
    if not exists:
        all_files_exist = False

if not all_files_exist:
    print("\n[ERROR] Some critical files are missing!")
    sys.exit(1)

print("[OK] All critical files found\n")

# Step 3: Check extract_table_to_csv.py for key fixes
print("[STEP 3] Checking extract_table_to_csv.py for fixes...")

extract_script = PROJECT_ROOT / "scripts/extract_table_to_csv.py"
content = extract_script.read_text()

checks = {
    "Has 'processed_count' variable": "processed_count" in content,
    "Has '--no-file-logging' argument": "--no-file-logging" in content,
    "Completion message present": "processed successfully" in content.lower(),
    "Does NOT re-raise in reconnection": content.count("raise reconnect_exc") == 0,
    "Has try/except around table processing": "except (SourceRowCountUnavailable, Exception) as exc:" in content,
}

for check, result in checks.items():
    status = "[YES]" if result else "[NO]"
    print(f"  {status} {check}")

all_checks_pass = all(checks.values())
if not all_checks_pass:
    print("\n[WARNING] Some fixes may not be in place!")
else:
    print("\n[OK] All critical fixes verified\n")

# Step 4: Summary
print("=" * 80)
print("NEXT STEPS")
print("=" * 80)
print("""
1. Share your Excel sheet with table row counts:
   - Which tables have 0 rows?
   - Which tables have data?
   - Total row count per table?

2. Run extraction with debug output:
   python scripts\\extract_table_to_csv.py --table all --format parquet --no-file-logging

3. Run the diagnostic to list all accessible tables:
   python -c "from scripts.extract_table_to_csv import _discover_tables; from connections.fourd_connection import connect_to_4d, close_4d_connection; c=connect_to_4d(); t=_discover_tables(c); print(f'Total: {len(t)}'); [print(f'{i+1}. {name}') for i,name in enumerate(t)]; close_4d_connection(c)"

4. Check disk space on export folder:
   dir C:\\python\\python\\4O_to_snowflake_ETL_project\\exports

5. If extraction still stops early, capture the full output:
   python scripts\\extract_table_to_csv.py --table all --format parquet --no-file-logging 2>&1 | Tee-Object -FilePath debug_output.txt
   (Then share debug_output.txt)
""")
print("=" * 80 + "\n")

sys.exit(0 if all_checks_pass else 1)
