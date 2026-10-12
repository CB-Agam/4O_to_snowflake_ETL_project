from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from connections.fourd_connection import connect_to_4d
from connections.snowflake_connection import connect_to_snowflake
from etl.daily_snapshot import extract_4d_table_to_json, publish_json_snapshot
from etl.metadata import load_metadata_from_excel

logger = logging.getLogger(__name__)
DEFAULT_EXCEL_PATH = PROJECT_ROOT / "4D_to_Snowflake (1-50 tables mapping).xlsx"


def _normalize_table_name(value: str) -> str:
    return str(value).strip().upper().replace(" ", "_").replace("-", "_")


def run_for_table(excel_path: str, table_name: str, export_dir: str | None = None) -> dict:
    metadata = load_metadata_from_excel(excel_path, sheet_name=table_name)
    normalized_table = _normalize_table_name(table_name)
    table_rows = metadata.get(normalized_table, [])
    if not table_rows:
        raise ValueError(f"No mapping rows found for target table: {table_name}")

    batch_id = str(uuid4())
    export_folder = Path(export_dir or settings.local_export_dir).expanduser()
    timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    json_path = export_folder / f"{normalized_table}_{timestamp}_{batch_id[:8]}.json"

    fourd_connection = connect_to_4d()
    try:
        row_count = extract_4d_table_to_json(
            fourd_connection,
            table_rows,
            json_path,
        )
    finally:
        fourd_connection.close()

    logger.info("Exported %d row(s) from 4D to %s", row_count, json_path)
    snowflake_connection = connect_to_snowflake()
    try:
        result = publish_json_snapshot(
            snowflake_connection,
            settings.snowflake_database,
            settings.snowflake_schema,
            normalized_table,
            table_rows,
            json_path,
            batch_id,
            row_count,
        )
    finally:
        snowflake_connection.close()

    return {
        "table": normalized_table,
        "source_table": next(
            row["source_table"].strip()
            for row in table_rows
            if row["source_table"].strip() and row["source_column"].strip()
        ),
        **result,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract a full 4D table to a completed local JSON file, load it into "
            "Snowflake, archive the previous snapshot, and publish the new snapshot."
        )
    )
    parser.add_argument("--excel", default=str(DEFAULT_EXCEL_PATH), help="Path to the Excel mapping workbook")
    parser.add_argument(
        "--local-dir",
        default=settings.local_export_dir,
        help="Folder where completed 4D JSON exports are retained (default: LOCAL_EXPORT_DIR).",
    )
    parser.add_argument(
        "--table",
        nargs="+",
        required=True,
        help="Target table worksheet/name; multiword names may be quoted or unquoted.",
    )
    return parser.parse_args(argv)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args()
    table_name = " ".join(args.table)
    result = run_for_table(args.excel, table_name, args.local_dir)
    print(f"Batch ID: {result['batch_id']}")
    print(f"Exported JSON: {result['json_file']}")
    print(f"Loaded {result['rows_loaded']} rows into {result['table']}")
    if result["history_table"]:
        print(f"Previous snapshot preserved as {result['history_table']}")
    else:
        print("No previous snapshot existed; this is the first load.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
