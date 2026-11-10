from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from uuid import uuid4

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from connections.fourd_connection import close_4d_connection, connect_to_4d
from etl.metadata import load_metadata_from_excel
from etl.parquet_export import export_table_to_parquet

DEFAULT_EXCEL_PATH = PROJECT_ROOT / "4D_to_Snowflake (1-50 tables mapping).xlsx"
DEFAULT_EXPORT_DIR = PROJECT_ROOT / "exports"
logger = logging.getLogger(__name__)


def _normalize_name(value: str) -> str:
    return str(value).strip().upper().replace(" ", "_").replace("-", "_")


def _positive_int(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("fetch size must be an integer") from exc
    if result <= 0:
        raise argparse.ArgumentTypeError("fetch size must be greater than zero")
    return result


def _find_mapping_sheet(excel_path: Path, table_name: str) -> str:
    expected = _normalize_name(table_name)
    with pd.ExcelFile(excel_path) as workbook:
        matches = [
            sheet
            for sheet in workbook.sheet_names
            if _normalize_name(sheet) == expected
        ]
    if len(matches) != 1:
        raise ValueError(
            f"Expected one mapping worksheet matching {table_name!r}; found {matches}"
        )
    return matches[0]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Stream one mapped 4D table to a single local Parquet file. "
            "This command does not connect to or write to Snowflake."
        )
    )
    parser.add_argument("--table", required=True, help="Mapped source/target table name")
    parser.add_argument(
        "--excel",
        type=Path,
        default=DEFAULT_EXCEL_PATH,
        help="Mapping workbook path",
    )
    parser.add_argument(
        "--sheet",
        help="Mapping worksheet name (defaults to the worksheet matching --table)",
    )
    parser.add_argument(
        "--fetch-size",
        type=_positive_int,
        default=50_000,
        help="Rows per ODBC fetch (default: 50000)",
    )
    parser.add_argument(
        "--export-dir",
        type=Path,
        default=DEFAULT_EXPORT_DIR,
        help="Local export root (default: repository exports directory)",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run ID; a UUID is generated when omitted",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = _parse_args(argv)
    source_connection = None
    exit_code = 0
    try:
        if not args.excel.is_file():
            raise FileNotFoundError(f"Mapping workbook not found: {args.excel}")
        sheet_name = args.sheet or _find_mapping_sheet(args.excel, args.table)
        metadata = load_metadata_from_excel(str(args.excel), sheet_name=sheet_name)
        mapping_rows = metadata.get(_normalize_name(args.table))
        if not mapping_rows:
            raise ValueError(
                f"No source-to-target mapping found for {args.table!r} "
                f"in worksheet {sheet_name!r}"
            )

        source_connection = connect_to_4d()
        result = export_table_to_parquet(
            source_connection,
            mapping_rows,
            args.export_dir,
            args.run_id or str(uuid4()),
            fetch_size=args.fetch_size,
        )
        logger.info(
            "Parquet export completed: table=%s rows=%s file=%s size_bytes=%s "
            "query_seconds=%.3f fetch_seconds=%.3f serialize_seconds=%.3f "
            "parquet_write_seconds=%.3f total_seconds=%.3f",
            result.target_table,
            result.row_count,
            result.file_path,
            result.file_size_bytes,
            result.query_seconds,
            result.fetch_seconds,
            result.serialization_seconds,
            result.parquet_write_seconds,
            result.total_seconds,
        )
    except Exception:
        logger.exception("Local Parquet export failed for %s", args.table)
        exit_code = 1
    finally:
        if source_connection is not None:
            try:
                close_4d_connection(source_connection)
            except Exception:
                logger.exception("Could not close the 4D connection cleanly")
                exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
