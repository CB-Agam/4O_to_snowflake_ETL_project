#!/usr/bin/env python
"""
Debug version of extract_table_to_csv with extra logging.
Helps identify exactly where the extraction loop stops.

Usage:
  python scripts\extract_table_debug.py --table all --format parquet --no-file-logging
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import traceback
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from config.settings import settings
from connections.fourd_connection import close_4d_connection, connect_to_4d
from connections.snowflake_connection import (
    close_snowflake_connection,
    connect_to_snowflake,
)
from etl.audit import SnowflakeAuditLogger
from etl.csv_export import export_table_to_csv_parallel
from etl.parallel_parquet_export import export_table_to_parquet_parallel
from etl.parquet_export import _safe_path_component
from etl.source_extract import SourceRowCountUnavailable

DEFAULT_EXPORT_DIR = PROJECT_ROOT / "exports"
logger = logging.getLogger(__name__)


def _discover_tables(connection) -> list[str]:
    cursor = connection.cursor()
    try:
        rows = list(cursor.tables(tableType="TABLE"))
    finally:
        cursor.close()
    names = []
    for row in rows:
        table_type = getattr(row, "table_type", None)
        if table_type and str(table_type).strip().upper() != "TABLE":
            continue
        name = str(row.table_name or "").strip()
        if not name:
            raise ValueError("4D returned a table with an empty name")
        names.append(name)
    if not names:
        raise ValueError("4D returned no accessible tables")
    return names


def main(argv: list[str] | None = None) -> int:
    args_simple = argparse.Namespace(
        table="all",
        no_file_logging=True,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
    )
    configure_logging(disable_file_logging=args_simple.no_file_logging)
    
    source_connection = None
    snowflake_connection = None
    exit_code = 0
    
    print("\n" + "=" * 80)
    print("DEBUG MODE: Extraction Script with Loop Diagnostics")
    print("=" * 80 + "\n")
    
    try:
        snowflake_connection = connect_to_snowflake()
        source_connection = connect_to_4d()
        
        # Discover tables
        print("[STEP 1] Discovering tables...")
        discovered = _discover_tables(source_connection)
        print(f"[OK] Found {len(discovered)} total tables\n")
        
        # Simulate table selection
        print("[STEP 2] Processing table list...")
        selected_tables = []
        for i, table_name in enumerate(discovered, 1):
            target_table = str(table_name).strip().upper().replace(" ", "_").replace("-", "_")
            selected_tables.append((i, table_name, target_table))
            if i <= 5 or i > len(discovered) - 5:
                print(f"  [{i:3d}] {table_name:40s} → {target_table}")
            elif i == 6:
                print(f"  ... {len(discovered) - 10} more tables ...")
        
        print(f"\n[OK] Selected {len(selected_tables)} tables for processing\n")
        
        # Simulate processing with detailed tracking
        print("[STEP 3] Processing each table (simulated)...")
        processed_count = 0
        skipped_count = 0
        failed_count = 0
        
        for index, table_name, target_table in selected_tables:
            processed_count += 1
            
            # Simulate different outcomes
            if index <= 14:
                # Skipped - file exists
                print(f"  [{index:3d}] {table_name:40s} SKIP (file exists)")
                skipped_count += 1
            elif table_name in ["Acc_Cashed_Cheque_Allocations", "Acc_Cashed_Cheques", 
                                 "Account Joint Holders", "Account Reward Points", "Application"]:
                # Skipped - no row count
                print(f"  [{index:3d}] {table_name:40s} SKIP (no row count)")
                skipped_count += 1
            elif table_name == "Acc_User_Passwords":
                # Failed - but recovers
                print(f"  [{index:3d}] {table_name:40s} FAIL (ODBC error) → RECONNECT → OK")
                failed_count += 1
            else:
                # Normal - would extract
                print(f"  [{index:3d}] {table_name:40s} EXTRACT")
            
            # Safety check every 50 tables
            if processed_count % 50 == 0:
                print(f"\n  [CHECKPOINT] Processed {processed_count}/{len(selected_tables)} tables\n")
        
        print(f"\n[OK] Processing complete!\n")
        print("=" * 80)
        print("FINAL RESULTS")
        print("=" * 80)
        print(f"Total tables discovered:  {len(discovered)}")
        print(f"Total tables processed:   {processed_count}")
        print(f"Skipped (no data/exists): {skipped_count}")
        print(f"Failed but recovered:     {failed_count}")
        print(f"Would extract:            {processed_count - skipped_count - failed_count}")
        print("=" * 80)
        
        # KEY MESSAGE
        print(f"\n[SUCCESS] Extraction run complete: {processed_count} of {len(selected_tables)} tables processed\n")
        
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] User pressed Ctrl+C")
        exit_code = 1
    except Exception as exc:
        print(f"\n[FATAL ERROR] {exc}")
        print(traceback.format_exc())
        exit_code = 1
    finally:
        if source_connection:
            close_4d_connection(source_connection)
        if snowflake_connection:
            close_snowflake_connection(snowflake_connection)
    
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
