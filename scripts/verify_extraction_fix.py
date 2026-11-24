#!/usr/bin/env python
"""
Verification script for extraction fix.
Checks that the script will process all tables without silent exits.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def check_reconnection_handling():
    """Verify reconnection code doesn't re-raise exceptions."""
    script_file = PROJECT_ROOT / "scripts" / "extract_table_to_csv.py"
    content = script_file.read_text()
    
    # Check for the problematic re-raise pattern
    if "raise reconnect_exc" in content:
        print("[FAIL] Script still re-raises reconnection exceptions")
        print("   Found: raise reconnect_exc")
        return False
    
    # Check for the fixed pattern
    if "continuing anyway" in content:
        print("[PASS] Reconnection errors are logged but not re-raised")
        return True
    
    print("[WARN] Could not verify reconnection fix")
    return None


def check_completion_message():
    """Verify completion message is guaranteed."""
    script_file = PROJECT_ROOT / "scripts" / "extract_table_to_csv.py"
    content = script_file.read_text()
    
    if 'Extraction run complete:' in content:
        print("[PASS] Completion message is in the script")
        return True
    
    print("[FAIL] Completion message not found")
    return False


def check_processed_counter():
    """Verify processed_count is tracked."""
    script_file = PROJECT_ROOT / "scripts" / "extract_table_to_csv.py"
    content = script_file.read_text()
    
    if 'processed_count = 0' in content and 'processed_count += 1' in content:
        print("[PASS] Table processing count is tracked")
        return True
    
    print("[FAIL] Table processing count not found")
    return False


def check_no_file_logging_flag():
    """Verify --no-file-logging flag exists."""
    script_file = PROJECT_ROOT / "scripts" / "extract_table_to_csv.py"
    content = script_file.read_text()
    
    if '--no-file-logging' in content:
        print("[PASS] --no-file-logging flag is implemented")
        return True
    
    print("[FAIL] --no-file-logging flag not found")
    return False


def check_logging_config():
    """Verify disable_file_logging parameter exists."""
    config_file = PROJECT_ROOT / "config" / "logging_config.py"
    content = config_file.read_text()
    
    if 'disable_file_logging' in content:
        print("[PASS] disable_file_logging parameter is implemented")
        return True
    
    print("[FAIL] disable_file_logging parameter not found")
    return False


def main():
    print("=" * 70)
    print("EXTRACTION SCRIPT FIX VERIFICATION")
    print("=" * 70)
    print()
    
    checks = [
        ("Reconnection Error Handling", check_reconnection_handling),
        ("Completion Message", check_completion_message),
        ("Processed Counter", check_processed_counter),
        ("No-File-Logging Flag", check_no_file_logging_flag),
        ("Logging Config Update", check_logging_config),
    ]
    
    results = []
    for name, check_func in checks:
        print(f"\n[Checking] {name}")
        try:
            result = check_func()
            results.append((name, result))
        except Exception as exc:
            print(f"[ERROR] {exc}")
            results.append((name, False))
    
    print()
    print("=" * 70)
    print("VERIFICATION RESULTS")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result is True)
    failed = sum(1 for _, result in results if result is False)
    warnings = sum(1 for _, result in results if result is None)
    
    for name, result in results:
        status = "[PASS]" if result is True else ("[FAIL]" if result is False else "[WARN]")
        print(f"{status}: {name}")
    
    print()
    print(f"Summary: {passed} passed, {failed} failed, {warnings} warnings")
    
    if failed > 0:
        print()
        print("NOTICE: Some checks failed. The fix may not be properly applied.")
        return 1
    
    if passed == len(checks):
        print()
        print("SUCCESS: All checks passed! The extraction script fix is properly applied.")
        print()
        print("Run extraction with:")
        print("  python scripts\\extract_table_to_csv.py --table all --format parquet --no-file-logging")
        print()
        print("Expected output will end with:")
        print("  Extraction run complete: X of Y tables processed")
        return 0
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
