#!/usr/bin/env python
"""
Verify that all extraction fixes are in place and working.
Run this after git pull to confirm the latest code is deployed.
"""

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent  # Go up to project root, not scripts
SUCCESS = True

print("\n" + "=" * 80)
print("EXTRACTION FIX VERIFICATION")
print("=" * 80 + "\n")

# Check 1: Diagnostic logging
print("[CHECK 1] Table-by-table progress tracking...")
extract_script = PROJECT_ROOT / "scripts" / "extract_table_to_csv.py"
content = extract_script.read_text()

checks = [
    ("Table progress print statement", "[{processed_count}/{len(selected_tables)}]" in content),
    ("Error message on outer exception", "ERROR: Extraction stopped unexpectedly" in content),
    ("Processed count before stopping", "Processed {processed_count} tables before stopping" in content),
    ("Per-table logger.info", "logger.info(\"Processing table %d/%d" in content),
]

for check_name, passed in checks:
    status = "[YES]" if passed else "[NO]"
    print(f"  {status} {check_name}")
    if not passed:
        SUCCESS = False

# Check 2: File existence
print("\n[CHECK 2] Diagnostic files exist...")
files_to_check = [
    "run_diagnostic.ps1",
    "DEBUG_EXTRACTION_ISSUE.md",
    "EXTRACTION_DIAGNOSTIC_SUMMARY.md",
]

for file in files_to_check:
    path = PROJECT_ROOT / file
    exists = path.exists()
    status = "[YES]" if exists else "[NO]"
    print(f"  {status} {file}")
    if not exists:
        SUCCESS = False

# Check 3: Syntax validation
print("\n[CHECK 3] Python syntax validation...")
try:
    result = subprocess.run(
        [sys.executable, "-m", "py_compile", str(extract_script)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode == 0:
        print(f"  [YES] extract_table_to_csv.py syntax is valid")
    else:
        print(f"  [NO] extract_table_to_csv.py has syntax errors:")
        print(f"       {result.stderr}")
        SUCCESS = False
except Exception as e:
    print(f"  [ERROR] Could not validate syntax: {e}")
    SUCCESS = False

# Summary
print("\n" + "=" * 80)
if SUCCESS:
    print("STATUS: All checks passed - Latest fixes are in place!")
    print("=" * 80 + "\n")
    print("Next steps:")
    print("  1. Run the diagnostic: powershell -ExecutionPolicy Bypass -File run_diagnostic.ps1")
    print("  2. Share the output with the team")
    print("  3. Include your Excel sheet with table row counts")
    print("")
    sys.exit(0)
else:
    print("STATUS: Some checks failed - Code may not be up to date")
    print("=" * 80 + "\n")
    print("Please run: git pull")
    print("Then run this script again")
    print("")
    sys.exit(1)
