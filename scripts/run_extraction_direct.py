#!/usr/bin/env python
"""
Direct extraction run with full output capture and exit tracking.
Runs for all 216 tables and captures exactly where/why it stops.
"""

import sys
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent  # Go up to project root

print("\n" + "=" * 80)
print("STARTING EXTRACTION WITH FULL OUTPUT CAPTURE")
print("=" * 80)
print(f"Project: {PROJECT_ROOT}")
print("=" * 80 + "\n")

# Run extraction and capture all output
cmd = [
    sys.executable,
    str(PROJECT_ROOT / "scripts" / "extract_table_to_csv.py"),
    "--table", "all",
    "--format", "parquet",
    "--no-file-logging",
]

print(f"Command: {' '.join(cmd)}\n")

try:
    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=False,  # Let output go directly to stdout
        text=True,
        timeout=3600,  # 1 hour timeout
    )
    
    print("\n" + "=" * 80)
    print(f"PROCESS EXIT CODE: {result.returncode}")
    print("=" * 80 + "\n")
    
    sys.exit(result.returncode)

except subprocess.TimeoutExpired:
    print("\n" + "=" * 80)
    print("ERROR: Process timed out after 1 hour")
    print("=" * 80 + "\n")
    sys.exit(1)

except KeyboardInterrupt:
    print("\n" + "=" * 80)
    print("ERROR: Process interrupted by user (Ctrl+C)")
    print("=" * 80 + "\n")
    sys.exit(130)

except Exception as exc:
    print("\n" + "=" * 80)
    print(f"ERROR: {exc}")
    print("=" * 80 + "\n")
    sys.exit(1)
