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
from etl.csv_export import export_table_to_csv_parallel
from etl.metadata import load_metadata_from_excel

DEFAULT_EXCEL_PATH = PROJECT_ROOT / "4D_to_Snowflake (1-50 tables mapping).xlsx"
DEFAULT_EXPORT_DIR = PROJECT_ROOT / "exports"
logger = logging.getLogger(__name__)


def _normalize_name(value: str) -> str:
    return str(value).strip().upper().replace(" ", "_").replace("-", "_")


def _positive_int(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be an integer") from exc
    if result <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return result


def _worker_count(value: str) -> int:
    count = _positive_int(value)
    if count > 3:
        raise argparse.ArgumentTypeError("worker count must be between 1 and 3")
    return count


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
            "Extract one mapped 4D table to one final local CSV file. "
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
        default=10_000,
        help="Rows per worker ODBC fetch (default: 10000)",
    )
    parser.add_argument(
        "--workers",
        type=_worker_count,
        default=3,
        help="Workers for extraction tasks (1-3; default: 3)",
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
        result = export_table_to_csv_parallel(
            source_connection,
            mapping_rows,
            args.export_dir,
            args.run_id or str(uuid4()),
            connection_factory=connect_to_4d,
            fetch_size=args.fetch_size,
            worker_count=args.workers,
        )
        logger.info(
            "4D CSV extraction completed: table=%s rows=%s workers=%s "
            "fetch_size=%s query_seconds=%.3f fetch_seconds=%.3f "
            "serialization_seconds=%.3f csv_write_seconds=%.3f "
            "total_seconds=%.3f files=%s",
            result.target_table,
            result.row_count,
            result.worker_count,
            result.fetch_size,
            result.query_seconds,
            result.fetch_seconds,
            result.serialization_seconds,
            result.csv_write_seconds,
            result.total_seconds,
            ";".join(str(path) for path in result.file_paths),
        )
        print(
            f"Extraction completed: table={result.target_table} "
            f"rows={result.row_count} workers={result.worker_count} "
            f"fetch_size={result.fetch_size} files={len(result.file_paths)}"
        )
        for path in result.file_paths:
            print(path)
    except Exception:
        logger.exception("4D CSV extraction failed for %s", args.table)
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
