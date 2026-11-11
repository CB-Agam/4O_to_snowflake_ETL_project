from __future__ import annotations

import argparse
import json
import logging
import sys
import traceback
from datetime import datetime
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
from etl.metadata import load_metadata_from_excel
from etl.parquet_export import export_table_to_parquet
from etl.parquet_snowflake import (
    drop_temporary_raw_table,
    load_parquet_to_raw_table,
)
from etl.parquet_transform import transform_parquet_raw_to_target

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
            "Run a complete 4D-to-Snowflake Parquet ETL for one or more mapped "
            "tables. Each table is extracted to one Parquet file, loaded into "
            "temporary staging, then transformed with invalid values quarantined."
        )
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--table", help="One mapped source/target table")
    selection.add_argument(
        "--tables",
        nargs="+",
        help="Multiple mapped source/target tables; continue after a table fails",
    )
    parser.add_argument(
        "--excel",
        type=Path,
        default=DEFAULT_EXCEL_PATH,
        help="Mapping workbook path",
    )
    parser.add_argument(
        "--sheet",
        help="Worksheet for --table (otherwise inferred from its name)",
    )
    parser.add_argument(
        "--fetch-size",
        type=_positive_int,
        default=50_000,
        help="Rows per streaming 4D fetch (default: 50000)",
    )
    parser.add_argument(
        "--export-dir",
        type=Path,
        default=DEFAULT_EXPORT_DIR,
        help="Local Parquet export root (default: repository exports directory)",
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


def _load_selected_mappings(
    excel_path: Path,
    table_names: list[str],
    sheet_override: str | None,
) -> list[tuple[str, str, list[dict[str, str]]]]:
    if not excel_path.is_file():
        raise FileNotFoundError(f"Mapping workbook not found: {excel_path}")
    if sheet_override and len(table_names) != 1:
        raise ValueError("--sheet can only be used with --table")

    normalized_names = [_normalize_name(name) for name in table_names]
    if len(set(normalized_names)) != len(normalized_names):
        raise ValueError("The table selection contains duplicate table names")

    selected = []
    for table_name, normalized_name in zip(table_names, normalized_names, strict=True):
        sheet_name = sheet_override or _find_mapping_sheet(excel_path, table_name)
        metadata = load_metadata_from_excel(str(excel_path), sheet_name=sheet_name)
        mapping_rows = metadata.get(normalized_name)
        if not mapping_rows:
            raise ValueError(
                f"No source-to-target mapping found for {table_name!r} "
                f"in worksheet {sheet_name!r}"
            )
        selected.append((table_name, sheet_name, mapping_rows))
    return selected


def _record_stage(
    audit: SnowflakeAuditLogger,
    table_name: str,
    stage: str,
    status: str,
    details: dict[str, object],
    error_message: str | None = None,
) -> None:
    audit.record(
        f"PARQUET_{stage}",
        status,
        table_name,
        json.dumps(details, sort_keys=True, default=str),
        error_message=error_message,
    )


def _run_table(
    source_connection,
    snowflake_connection,
    audit: SnowflakeAuditLogger,
    database: str,
    schema: str,
    export_dir: Path,
    fetch_size: int,
    requested_table: str,
    sheet_name: str,
    mapping_rows: list[dict[str, str]],
) -> dict[str, object]:
    target_table = next(
        row["target_table"].strip()
        for row in mapping_rows
        if row.get("source_table", "").strip()
        and row.get("source_column", "").strip()
        and row.get("target_table", "").strip()
        and row.get("target_column", "").strip()
    )
    started_at = datetime.now()
    active_stage = "EXTRACTION"
    table_step = audit.record(
        "PARQUET_TABLE_RUN",
        "RUNNING",
        target_table,
        f"Starting 4D-to-Snowflake Parquet flow from worksheet {sheet_name!r}",
        start_time=started_at,
    )
    details: dict[str, object] = {
        "run_id": audit.run_id,
        "table": target_table,
        "requested_table": requested_table,
        "sheet": sheet_name,
        "extraction_status": "NOT_STARTED",
        "snowflake_load_status": "NOT_STARTED",
        "transform_status": "NOT_STARTED",
        "total_extracted_rows": None,
        "parquet_file_name": None,
        "parquet_file_size_bytes": None,
        "loaded_rows": None,
        "rejected_rows": None,
    }

    temporary_raw_table: str | None = None
    try:
        extraction = export_table_to_parquet(
            source_connection,
            mapping_rows,
            export_dir,
            audit.run_id,
            fetch_size=fetch_size,
        )
        details.update(
            extraction_status="SUCCESS",
            total_extracted_rows=extraction.row_count,
            parquet_file_name=extraction.file_path.name,
            parquet_file_size_bytes=extraction.file_size_bytes,
            parquet_file_path=str(extraction.file_path),
            extraction_seconds=extraction.total_seconds,
        )
        _record_stage(
            audit,
            target_table,
            "EXTRACTION",
            "SUCCESS",
            {
                key: details[key]
                for key in (
                    "total_extracted_rows",
                    "parquet_file_name",
                    "parquet_file_size_bytes",
                    "parquet_file_path",
                    "extraction_seconds",
                    "extraction_status",
                )
            },
        )

        active_stage = "SNOWFLAKE_LOAD"
        raw_load = load_parquet_to_raw_table(
            snowflake_connection,
            database,
            schema,
            mapping_rows,
            extraction.file_path,
            extraction.run_id,
            persistent_raw=False,
        )
        if not raw_load.skipped_as_duplicate:
            temporary_raw_table = raw_load.raw_table
        details.update(
            snowflake_load_status="SUCCESS",
            loaded_rows=raw_load.loaded_rows,
            raw_load_skipped_as_duplicate=raw_load.skipped_as_duplicate,
        )
        _record_stage(
            audit,
            target_table,
            "SNOWFLAKE_LOAD",
            "SUCCESS",
            {
                key: details[key]
                for key in (
                    "loaded_rows",
                    "snowflake_load_status",
                    "raw_load_skipped_as_duplicate",
                )
            },
        )

        active_stage = "TRANSFORM"
        transformed = transform_parquet_raw_to_target(
            snowflake_connection,
            database,
            schema,
            mapping_rows,
            extraction.run_id,
            extraction.file_path.name,
            raw_table_reference=raw_load.raw_table,
        )
        details.update(
            transform_status="SUCCESS",
            transformed_rows=transformed.inserted_rows,
            rejected_rows=transformed.rejected_rows,
            transform_skipped_as_duplicate=transformed.skipped_as_duplicate,
        )
        _record_stage(
            audit,
            target_table,
            "TRANSFORM",
            "SUCCESS",
            {
                key: details[key]
                for key in (
                    "transformed_rows",
                    "rejected_rows",
                    "transform_status",
                    "transform_skipped_as_duplicate",
                )
            },
        )
        active_stage = "FINALIZE"
        details["end_time"] = datetime.now().isoformat()
        details["status"] = "SUCCESS"
        audit.finish_step(
            table_step,
            "SUCCESS",
            json.dumps(details, sort_keys=True, default=str),
        )
        logger.info(
            "Parquet ETL completed for %s: extracted=%s loaded=%s transformed=%s "
            "rejected=%s file=%s",
            target_table,
            extraction.row_count,
            raw_load.loaded_rows,
            transformed.inserted_rows,
            transformed.rejected_rows,
            extraction.file_path,
        )
        return details
    except Exception as exc:
        error_message = traceback.format_exc()
        failed_stage = active_stage
        details.update(
            status="FAILED",
            failed_stage=failed_stage,
            error=str(exc),
            end_time=datetime.now().isoformat(),
        )
        stage_key = {
            "EXTRACTION": "extraction_status",
            "SNOWFLAKE_LOAD": "snowflake_load_status",
            "TRANSFORM": "transform_status",
        }.get(failed_stage)
        if stage_key is not None:
            details[stage_key] = "FAILED"
        try:
            _record_stage(
                audit,
                target_table,
                failed_stage,
                "FAILED",
                details,
                error_message,
            )
            audit.finish_step(
                table_step,
                "FAILED",
                json.dumps(details, sort_keys=True, default=str),
                error_message,
            )
        except Exception:
            logger.exception("Could not write failure audit for %s", target_table)
        logger.exception("Parquet ETL failed for %s", target_table)
        return details
    finally:
        if temporary_raw_table is not None:
            try:
                drop_temporary_raw_table(
                    snowflake_connection,
                    temporary_raw_table,
                )
            except Exception:
                logger.exception(
                    "Could not remove temporary raw staging table %s",
                    temporary_raw_table,
                )


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = _parse_args(argv)
    table_names = [args.table] if args.table else list(args.tables)
    snowflake_connection = None
    source_connection = None
    audit: SnowflakeAuditLogger | None = None
    pipeline_step: int | None = None
    exit_code = 0
    try:
        if not args.database.strip() or not args.schema.strip():
            raise ValueError(
                "Snowflake database and schema are required; set SNOWFLAKE_DATABASE "
                "and SNOWFLAKE_SCHEMA or pass --database and --schema"
            )
        selected = _load_selected_mappings(args.excel, table_names, args.sheet)
        snowflake_connection = connect_to_snowflake()
        audit = SnowflakeAuditLogger(
            snowflake_connection,
            args.database,
            args.schema,
        )
        audit.ensure_table()
        pipeline_step = audit.record(
            "PARQUET_ETL_PIPELINE",
            "RUNNING",
            ", ".join(
                next(
                    row["target_table"].strip()
                    for row in rows
                    if row.get("target_table", "").strip()
                    and row.get("target_column", "").strip()
                )
                for _, _, rows in selected
            ),
            f"Starting Parquet ETL for {len(selected)} table(s)",
            start_time=datetime.now(),
        )
        source_connection = connect_to_4d()
        results = [
            _run_table(
                source_connection,
                snowflake_connection,
                audit,
                args.database,
                args.schema,
                args.export_dir,
                args.fetch_size,
                table_name,
                sheet_name,
                mapping_rows,
            )
            for table_name, sheet_name, mapping_rows in selected
        ]
        failures = [item for item in results if item.get("status") == "FAILED"]
        if failures:
            exit_code = 1
            pipeline_status = "PARTIAL"
        else:
            pipeline_status = "SUCCESS"
        audit.finish_step(
            pipeline_step,
            pipeline_status,
            "; ".join(
                f"{item['table']}: {item['status']}"
                for item in results
            ),
            (
                "; ".join(f"{item['table']}: {item['error']}" for item in failures)
                if failures
                else None
            ),
        )
        for item in results:
            status = str(item.get("status", "SUCCESS"))
            message = (
                f"Table {item['table']} failed at {item['failed_stage']}: {item['error']}"
                if status == "FAILED"
                else (
                    f"Table {item['table']} completed: "
                    f"extracted={item['total_extracted_rows']} "
                    f"loaded={item['loaded_rows']} "
                    f"transformed={item['transformed_rows']} "
                    f"rejected={item['rejected_rows']} "
                    f"file={item['parquet_file_path']}"
                )
            )
            logger.info(message)
            print(message)
    except Exception as exc:
        logger.exception("Parquet ETL pipeline failed")
        exit_code = 1
        if audit is not None and pipeline_step is not None:
            try:
                audit.finish_step(
                    pipeline_step,
                    "FAILED",
                    f"Parquet ETL pipeline failed: {exc}",
                    traceback.format_exc(),
                )
            except Exception:
                logger.exception("Could not write pipeline failure audit")
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
