from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from connections.fourd_connection import connect_to_4d
from connections.snowflake_connection import connect_to_snowflake
from etl.ddl import build_create_table_sql
from etl.load import build_source_query, create_missing_tables_from_metadata, get_table_rows, load_dataframe_to_snowflake
from etl.metadata import load_metadata_from_excel


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EXCEL_PATH = PROJECT_ROOT / "4D_to_Snowflake (1-50 tables mapping).xlsx"


def _normalize_target_table_name(value: str) -> str:
    return str(value).strip().upper().replace(" ", "_").replace("-", "_")


def run_for_table(excel_path: str, sheet_name: str, target_table: str | None = None) -> dict:
    metadata = load_metadata_from_excel(excel_path, sheet_name=sheet_name)

    selected_tables = metadata
    if target_table:
        normalized_target = _normalize_target_table_name(target_table)
        selected_tables = {normalized_target: metadata.get(normalized_target, [])}

    snowflake_conn = connect_to_snowflake()
    snowflake_cursor = snowflake_conn.cursor()

    try:
        create_missing_tables_from_metadata(snowflake_cursor, selected_tables, settings.snowflake_schema)

        fourd_conn = connect_to_4d()
        try:
            summary = []
            for table_name, rows in selected_tables.items():
                if not rows:
                    continue

                source_table = rows[0]["source_table"]
                source_columns = [row["source_column"] for row in rows]
                target_columns = [row["target_column"] for row in rows]

                query = build_source_query(source_table, source_columns)
                df = pd.read_sql(query, fourd_conn)
                df.columns = target_columns
                inserted = load_dataframe_to_snowflake(
                    snowflake_cursor,
                    table_name,
                    target_columns,
                    df,
                    schema_name=settings.snowflake_schema,
                )
                summary.append({
                    "table": table_name,
                    "source_table": source_table,
                    "source_columns": source_columns,
                    "target_columns": target_columns,
                    "rows_loaded": inserted,
                })
            return {"tables": summary}
        finally:
            fourd_conn.close()
    finally:
        snowflake_cursor.close()
        snowflake_conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Create missing Snowflake tables from the Excel mapping and load data from 4D.")
    parser.add_argument("--excel", default=str(DEFAULT_EXCEL_PATH), help="Path to the Excel mapping workbook")
    parser.add_argument(
        "--sheet",
        default=None,
        help="Excel sheet name to read (defaults to --table, or Snowflake_table_names)",
    )
    parser.add_argument("--table", default=None, help="Optional target table name to process. If omitted, process all tables.")
    args = parser.parse_args()

    sheet_name = args.sheet or args.table or "Snowflake_table_names"
    result = run_for_table(args.excel, sheet_name, args.table)
    for item in result["tables"]:
        print(f"Loaded {item['rows_loaded']} rows into {item['table']} from {item['source_table']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
