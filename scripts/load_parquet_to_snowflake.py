from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from config.settings import settings
from connections.snowflake_connection import (
    close_snowflake_connection,
    connect_to_snowflake,
)
from etl.metadata import load_metadata_from_excel
from etl.parquet_snowflake import load_parquet_to_raw_table

DEFAULT_EXCEL_PATH = PROJECT_ROOT / "4D_to_Snowflake (1-50 tables mapping).xlsx"
logger = logging.getLogger(__name__)


def _normalize_name(value: str) -> str:
    return str(value).strip().upper().replace(" ", "_").replace("-", "_")


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


def _read_run_id(parquet_path: Path) -> str:
    with parquet_path.open("rb") as source:
        metadata = pq.read_metadata(source)
    values = {
        key.decode("utf-8"): value.decode("utf-8")
        for key, value in (metadata.metadata or {}).items()
    }
    run_id = values.get("run_id")
    if not run_id:
        raise ValueError(f"Parquet file has no ETL run ID metadata: {parquet_path}")
    return run_id


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load a completed single-table Parquet export into a Snowflake "
            "VARCHAR raw landing table."
        )
    )
    parser.add_argument("--table", required=True, help="Mapped source/target table name")
    parser.add_argument("--file", required=True, type=Path, help="Completed Parquet file")
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
        "--database",
        default=settings.snowflake_database,
        help="Snowflake database (defaults to SNOWFLAKE_DATABASE)",
    )
    parser.add_argument(
        "--schema",
        default=settings.snowflake_schema,
        help="Snowflake schema (defaults to SNOWFLAKE_SCHEMA)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = _parse_args(argv)
    snowflake_connection = None
    try:
        if not args.file.is_file():
            raise FileNotFoundError(f"Parquet file not found: {args.file}")
        if not args.excel.is_file():
            raise FileNotFoundError(f"Mapping workbook not found: {args.excel}")
        if not args.database.strip() or not args.schema.strip():
            raise ValueError(
                "Snowflake database and schema are required; set SNOWFLAKE_DATABASE "
                "and SNOWFLAKE_SCHEMA or pass --database and --schema"
            )

        sheet_name = args.sheet or _find_mapping_sheet(args.excel, args.table)
        metadata = load_metadata_from_excel(str(args.excel), sheet_name=sheet_name)
        mapping_rows = metadata.get(_normalize_name(args.table))
        if not mapping_rows:
            raise ValueError(
                f"No source-to-target mapping found for {args.table!r} "
                f"in worksheet {sheet_name!r}"
            )
        run_id = _read_run_id(args.file)

        snowflake_connection = connect_to_snowflake()
        result = load_parquet_to_raw_table(
            snowflake_connection,
            args.database,
            args.schema,
            mapping_rows,
            args.file,
            run_id,
        )
        logger.info(
            "Parquet Snowflake load %s: table=%s raw_table=%s rows=%s file=%s "
            "sha256=%s duplicate=%s",
            "skipped as already loaded" if result.skipped_as_duplicate else "completed",
            result.target_table,
            result.raw_table,
            result.loaded_rows,
            result.file_path,
            result.file_sha256,
            result.skipped_as_duplicate,
        )
    except Exception:
        logger.exception("Snowflake Parquet load failed for %s", args.table)
        return 1
    finally:
        if snowflake_connection is not None:
            try:
                close_snowflake_connection(snowflake_connection)
            except Exception:
                logger.exception("Could not close the Snowflake connection cleanly")
                return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
