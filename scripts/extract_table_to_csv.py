from __future__ import annotations

import argparse
import json
import logging
import sys
import traceback
from pathlib import Path

import pandas as pd

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
from etl.metadata import load_metadata_from_excel
from etl.parallel_parquet_export import export_table_to_parquet_parallel

DEFAULT_EXCEL_PATH = PROJECT_ROOT / "4D_to_Snowflake (1-50 tables mapping).xlsx"
DEFAULT_EXPORT_DIR = PROJECT_ROOT / "exports"
logger = logging.getLogger(__name__)


class AuditLoggingError(RuntimeError):
    pass


def _record_skip(audit: SnowflakeAuditLogger, step: str, table: str, message: str) -> None:
    try:
        audit.record(step, "SKIPPED", table, message)
    except Exception as exc:
        raise AuditLoggingError(f"Could not record extraction skip for {table}") from exc


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


def _matching_mapping_sheets(
    sheet_names: list[str],
    table_name: str,
) -> list[str]:
    expected = _normalize_name(table_name)
    return [
        sheet
        for sheet in sheet_names
        if _normalize_name(sheet) == expected
    ]


def _find_mapping_sheet(excel_path: Path, table_name: str) -> str:
    with pd.ExcelFile(excel_path) as workbook:
        matches = _matching_mapping_sheets(workbook.sheet_names, table_name)
    if len(matches) != 1:
        raise ValueError(
            f"Expected one mapping worksheet matching {table_name!r}; found {matches}"
        )
    return matches[0]


def _load_all_table_names(excel_path: Path) -> list[str]:
    table_index = pd.read_excel(excel_path, sheet_name="Snowflake_table_names")
    if table_index.empty or len(table_index.columns) == 0:
        raise ValueError("The Snowflake_table_names worksheet contains no tables")

    table_names = []
    seen_names = set()
    for value in table_index.iloc[:, 0]:
        if pd.isna(value):
            continue
        table_name = str(value).strip()
        normalized_name = _normalize_name(table_name)
        if normalized_name and normalized_name not in seen_names:
            table_names.append(table_name)
            seen_names.add(normalized_name)
    if not table_names:
        raise ValueError("The Snowflake_table_names worksheet contains no tables")
    return table_names


def _mapped_target_table(mapping_rows: list[dict[str, str]]) -> str:
    return next(
        row["target_table"].strip()
        for row in mapping_rows
        if row.get("source_table", "").strip()
        and row.get("source_column", "").strip()
        and row.get("target_table", "").strip()
        and row.get("target_column", "").strip()
    )


def _has_parquet_export(export_dir: Path, target_table: str) -> Path | None:
    table_dir = export_dir / _normalize_name(target_table)
    if not table_dir.is_dir():
        return None
    return next(
        (path for path in table_dir.rglob("*.parquet") if path.is_file()),
        None,
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract mapped 4D tables to final local Parquet files "
            "(or CSV with --format csv), and record extraction status in "
            "Snowflake ETL_PIPELINE_LOG. No extracted data is uploaded."
        )
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Mapped source/target table name, or 'all' for every indexed table",
    )
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
        default=None,
        help=(
            "Rows per ODBC fetch; defaults to a table-count-based value "
            "(maximum 100000)"
        ),
    )
    parser.add_argument(
        "--workers",
        type=_worker_count,
        default=3,
        help="Workers for extraction tasks (1-3; default: 3)",
    )
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=("parquet", "csv"),
        default="parquet",
        help="Output file format (default: parquet)",
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
    parser.add_argument(
        "--database",
        default=settings.snowflake_database,
        help="Snowflake audit database (defaults to SNOWFLAKE_DATABASE)",
    )
    parser.add_argument(
        "--schema",
        default=settings.snowflake_schema,
        help="Snowflake audit schema (defaults to SNOWFLAKE_SCHEMA)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = _parse_args(argv)
    source_connection = None
    snowflake_connection = None
    exit_code = 0
    try:
        if not args.database.strip() or not args.schema.strip():
            raise ValueError(
                "Snowflake database and schema are required for extraction audit logging"
            )
        snowflake_connection = connect_to_snowflake()
        audit = SnowflakeAuditLogger(
            snowflake_connection, args.database, args.schema
        )
        audit.ensure_table()
        run_id = args.run_id or audit.run_id
        step_name = f"{args.output_format.upper()}_EXTRACTION"
        if not args.excel.is_file():
            raise FileNotFoundError(f"Mapping workbook not found: {args.excel}")
        all_tables = _normalize_name(args.table) == "ALL"
        if all_tables and args.sheet:
            raise ValueError("--sheet cannot be used with --table all")

        selected_tables = []
        table_names = (
            _load_all_table_names(args.excel) if all_tables else [args.table]
        )
        mapping_sheets = None
        if all_tables:
            with pd.ExcelFile(args.excel) as workbook:
                mapping_sheets = workbook.sheet_names
        for table_name in table_names:
            try:
                if all_tables:
                    matches = _matching_mapping_sheets(
                        mapping_sheets or [],
                        table_name,
                    )
                    if not matches:
                        message = (
                            f"Skipping {table_name}: no matching mapping worksheet"
                        )
                        logger.warning(message)
                        print(message)
                        _record_skip(audit, step_name, table_name, message)
                        continue
                    if len(matches) != 1:
                        raise ValueError(
                            f"Expected one mapping worksheet matching "
                            f"{table_name!r}; found {matches}"
                        )
                    sheet_name = matches[0]
                else:
                    sheet_name = args.sheet or _find_mapping_sheet(
                        args.excel, table_name
                    )
                metadata = load_metadata_from_excel(
                    str(args.excel),
                    sheet_name=sheet_name,
                )
                mapping_rows = metadata.get(_normalize_name(table_name))
                if not mapping_rows:
                    if all_tables:
                        message = (
                            f"Skipping {table_name}: no source-to-target "
                            f"mapping in worksheet {sheet_name!r}"
                        )
                        logger.warning(message)
                        print(message)
                        _record_skip(audit, step_name, table_name, message)
                        continue
                    raise ValueError(
                        f"No source-to-target mapping found for {table_name!r} "
                        f"in worksheet {sheet_name!r}"
                    )

                target_table = _mapped_target_table(mapping_rows)
                if all_tables and args.output_format == "parquet":
                    existing_file = _has_parquet_export(
                        args.export_dir,
                        target_table,
                    )
                    if existing_file is not None:
                        message = (
                            f"Skipping {table_name}: Parquet already exists at "
                            f"{existing_file}"
                        )
                        logger.info(message)
                        print(message)
                        _record_skip(audit, step_name, target_table, message)
                        continue
                selected_tables.append((table_name, target_table, mapping_rows))
            except AuditLoggingError:
                raise
            except Exception as exc:
                logger.exception("Could not prepare extraction for %s", table_name)
                print(f"Extraction failed for {table_name}: {exc}")
                audit.record(
                    step_name, "FAILED", table_name, str(exc),
                    error_message=traceback.format_exc(),
                )
                exit_code = 1

        if selected_tables:
            try:
                source_connection = connect_to_4d()
            except Exception as exc:
                error = traceback.format_exc()
                for _, target_table, _ in selected_tables:
                    audit.record(
                        step_name, "FAILED", target_table,
                        f"Could not connect to 4D: {exc}",
                        error_message=error,
                    )
                raise
        for table_name, target_table, mapping_rows in selected_tables:
            table_step = audit.record(
                step_name, "RUNNING", target_table,
                json.dumps({"export_run_id": run_id, "format": args.output_format}),
            )
            try:
                if args.output_format == "parquet":
                    result = export_table_to_parquet_parallel(
                        source_connection,
                        mapping_rows,
                        args.export_dir,
                        run_id,
                        connection_factory=connect_to_4d,
                        fetch_size=args.fetch_size,
                        worker_count=args.workers,
                    )
                    logger.info(
                        "4D Parquet extraction completed: table=%s rows=%s "
                        "workers=%s fetch_size=%s query_seconds=%.3f "
                        "fetch_seconds=%.3f serialization_seconds=%.3f "
                        "parquet_write_seconds=%.3f total_seconds=%.3f file=%s",
                        result.target_table,
                        result.row_count,
                        result.worker_count,
                        result.fetch_size,
                        result.query_seconds,
                        result.fetch_seconds,
                        result.serialization_seconds,
                        result.parquet_write_seconds,
                        result.total_seconds,
                        result.file_path,
                    )
                    output_paths = (result.file_path,)
                else:
                    result = export_table_to_csv_parallel(
                        source_connection,
                        mapping_rows,
                        args.export_dir,
                        run_id,
                        connection_factory=connect_to_4d,
                        fetch_size=args.fetch_size,
                        worker_count=args.workers,
                    )
                    logger.info(
                        "4D CSV extraction completed: table=%s rows=%s "
                        "workers=%s fetch_size=%s query_seconds=%.3f "
                        "fetch_seconds=%.3f serialization_seconds=%.3f "
                        "csv_write_seconds=%.3f total_seconds=%.3f files=%s",
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
                    output_paths = result.file_paths
            except Exception as exc:
                logger.exception("4D extraction failed for %s", table_name)
                print(f"Extraction failed for {table_name}: {exc}")
                audit.finish_step(
                    table_step, "FAILED", f"Extraction failed: {exc}",
                    error_message=traceback.format_exc(),
                )
                exit_code = 1
                continue
            audit.finish_step(
                table_step,
                "SUCCESS",
                json.dumps(
                    {
                        "export_run_id": result.run_id,
                        "format": args.output_format,
                        "rows": result.row_count,
                        "files": [str(path) for path in output_paths],
                        "workers": result.worker_count,
                        "fetch_size": result.fetch_size,
                        "query_seconds": result.query_seconds,
                        "fetch_seconds": result.fetch_seconds,
                        "serialization_seconds": result.serialization_seconds,
                        "file_write_seconds": (
                            result.parquet_write_seconds
                            if args.output_format == "parquet"
                            else result.csv_write_seconds
                        ),
                        "total_seconds": result.total_seconds,
                    },
                    sort_keys=True,
                ),
            )
            print(
                f"Extraction completed: format={args.output_format} "
                f"table={result.target_table} rows={result.row_count} "
                f"workers={result.worker_count} "
                f"fetch_size={result.fetch_size} files={len(output_paths)}"
            )
            for path in output_paths:
                print(path)
    except Exception:
        logger.exception("Extraction or audit logging failed for %s", args.table)
        exit_code = 1
    finally:
        if source_connection is not None:
            try:
                close_4d_connection(source_connection)
            except Exception:
                logger.exception("Could not close the 4D connection cleanly")
                exit_code = 1
        if snowflake_connection is not None:
            try:
                close_snowflake_connection(snowflake_connection)
            except Exception:
                logger.exception("Could not close the Snowflake connection cleanly")
                exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
