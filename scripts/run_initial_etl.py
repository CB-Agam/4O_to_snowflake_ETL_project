from __future__ import annotations

import argparse
import logging
import sys
import traceback
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from config.logging_config import configure_logging
from connections.fourd_connection import connect_to_4d
from connections.snowflake_connection import connect_to_snowflake
from etl.audit import SnowflakeAuditLogger
from etl.ddl import (
    build_create_table_sql,
    build_drop_status_column_sql,
    build_timestamp_column_sql,
    build_widen_clob_columns_sql,
)
from etl.load import (
    build_source_query,
    load_query_to_snowflake_in_batches,
    table_exists_in_snowflake,
)
from etl.incremental import (
    acquire_table_run_lock,
    incremental_load_table,
    release_table_run_lock,
    renew_table_run_lock,
)
from etl.metadata import load_metadata_from_excel


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EXCEL_PATH = PROJECT_ROOT / "4D_to_Snowflake (1-50 tables mapping).xlsx"
logger = logging.getLogger(__name__)


def _normalize_target_table_name(value: str) -> str:
    return str(value).strip().upper().replace(" ", "_").replace("-", "_")


def run_for_table(
    excel_path: str,
    sheet_name: str | dict[str, str],
    target_table: str | Sequence[str] | None = None,
    incremental: bool = False,
    overlap_seconds: int = 86400,
    batch_size: int = 50_000,
) -> dict:
    if incremental and overlap_seconds < 0:
        raise ValueError("Incremental overlap seconds cannot be negative")
    if batch_size <= 0:
        raise ValueError("Batch size must be greater than zero")
    if isinstance(target_table, str) or target_table is None:
        selected_target_names = [target_table] if target_table else []
    else:
        selected_target_names = list(target_table)
    audit_table_name = ", ".join(selected_target_names) or None

    pipeline_started_at = datetime.now()
    snowflake_connect_started_at = datetime.now()
    snowflake_conn = connect_to_snowflake()
    audit = SnowflakeAuditLogger(
        snowflake_conn,
        settings.snowflake_database,
        settings.snowflake_schema,
    )
    audit_ready = False
    pipeline_step_number = None
    try:
        audit_table_started_at = datetime.now()
        audit.ensure_table()
        audit_ready = True
        pipeline_step_number = audit.record(
            "ETL_PIPELINE",
            "RUNNING",
            audit_table_name,
            f"ETL run {audit.run_id} started using worksheet {sheet_name}",
            start_time=pipeline_started_at,
        )
        audit.record(
            "CONNECT_SNOWFLAKE",
            "SUCCESS",
            audit_table_name,
            "Snowflake connection established",
            start_time=snowflake_connect_started_at,
        )
        audit.record(
            "ENSURE_AUDIT_TABLE",
            "SUCCESS",
            audit_table_name,
            "Snowflake audit table is ready",
            start_time=audit_table_started_at,
        )

        if isinstance(sheet_name, dict):
            metadata = {}
            for table_name, mapping_sheet in sheet_name.items():
                table_metadata = audit.run_step(
                    "LOAD_MAPPING",
                    lambda mapping_sheet=mapping_sheet: load_metadata_from_excel(
                        excel_path,
                        sheet_name=mapping_sheet,
                    ),
                    table_name,
                )
                metadata.update(table_metadata)
        else:
            metadata = audit.run_step(
                "LOAD_MAPPING",
                lambda: load_metadata_from_excel(excel_path, sheet_name=sheet_name),
                audit_table_name,
            )
        selected_tables = metadata
        if selected_target_names:
            normalized_targets = [
                _normalize_target_table_name(name) for name in selected_target_names
            ]
            selected_tables = {
                name: metadata.get(name, []) for name in normalized_targets
            }

        if not selected_tables or not any(selected_tables.values()):
            raise ValueError(
                "No source-to-target mappings found for table(s): "
                f"{audit_table_name or sheet_name}"
            )

        snowflake_cursor = audit.run_step("OPEN_SNOWFLAKE_CURSOR", snowflake_conn.cursor)
        try:
            fourd_conn = audit.run_step("CONNECT_4D", connect_to_4d)
            try:
                summary = []
                for table_name, rows in selected_tables.items():
                    source_rows = [
                        row
                        for row in rows
                        if row["source_table"].strip()
                        and row["source_column"].strip()
                        and row["target_column"].strip()
                    ]
                    if not source_rows:
                        raise ValueError(f"No complete source-to-target column mappings for table: {table_name}")

                    source_table = source_rows[0]["source_table"]
                    source_columns = [row["source_column"] for row in source_rows]
                    target_columns = [row["target_column"] for row in source_rows]

                    table_lock = audit.run_step(
                        "ACQUIRE_TABLE_LOCK",
                        lambda: acquire_table_run_lock(
                            snowflake_cursor,
                            snowflake_conn,
                            settings.snowflake_database,
                            settings.snowflake_schema,
                            table_name,
                            rows,
                        ),
                        table_name,
                    )
                    try:
                        exists = audit.run_step(
                            "CHECK_TARGET_TABLE",
                            lambda: table_exists_in_snowflake(
                                snowflake_cursor,
                                settings.snowflake_schema,
                                table_name,
                            ),
                            table_name,
                        )
                        if exists:
                            audit.record(
                                "CREATE_TARGET_TABLE",
                                "SKIPPED",
                                table_name,
                                "Target table already exists",
                            )
                            audit.run_step(
                                "DROP_STATUS_COLUMN",
                                lambda: snowflake_cursor.execute(
                                    build_drop_status_column_sql(
                                        table_name,
                                        schema_name=settings.snowflake_schema,
                                    )
                                ),
                                table_name,
                            )
                            for statement in build_widen_clob_columns_sql(
                                table_name,
                                rows,
                                schema_name=settings.snowflake_schema,
                            ):
                                audit.run_step(
                                    "WIDEN_CLOB_COLUMN",
                                    lambda statement=statement: snowflake_cursor.execute(statement),
                                    table_name,
                                )
                            for statement in build_timestamp_column_sql(
                                table_name,
                                rows,
                                schema_name=settings.snowflake_schema,
                            ):
                                audit.run_step(
                                    "ENFORCE_TIMESTAMP_COLUMN",
                                    lambda statement=statement: snowflake_cursor.execute(statement),
                                    table_name,
                                )
                        else:
                            audit.run_step(
                                "CREATE_TARGET_TABLE",
                                lambda: snowflake_cursor.execute(
                                    build_create_table_sql(
                                        table_name,
                                        rows,
                                        schema_name=settings.snowflake_schema,
                                    )
                                ),
                                table_name,
                            )

                        if incremental:
                            result = audit.run_step(
                                "INCREMENTAL_LOAD",
                                lambda: incremental_load_table(
                                    fourd_conn,
                                    snowflake_conn,
                                    snowflake_cursor,
                                    settings.snowflake_database,
                                    settings.snowflake_schema,
                                    table_name,
                                    rows,
                                    audit.run_id,
                                    overlap_seconds=overlap_seconds,
                                    table_lock=table_lock,
                                ),
                                table_name,
                            )
                            summary.append(
                                {
                                    "table": table_name,
                                    "source_table": source_table,
                                    "source_columns": source_columns,
                                    "target_columns": target_columns,
                                    **result,
                                }
                            )
                            continue

                        query = build_source_query(source_table, source_columns)
                        def renew_lock_before_publish() -> None:
                            audit.run_step(
                                "RENEW_TABLE_LOCK",
                                lambda: renew_table_run_lock(
                                    snowflake_cursor,
                                    snowflake_conn,
                                    table_lock,
                                    table_name,
                                ),
                                table_name,
                            )

                        inserted = audit.run_step(
                            "BATCHED_BULK_LOAD",
                            lambda: load_query_to_snowflake_in_batches(
                                fourd_conn,
                                snowflake_conn,
                                snowflake_cursor,
                                query,
                                settings.snowflake_database,
                                settings.snowflake_schema,
                                table_name,
                                target_columns,
                                batch_size=batch_size,
                                before_publish=renew_lock_before_publish,
                            ),
                            table_name,
                        )
                        summary.append({
                            "table": table_name,
                            "source_table": source_table,
                            "source_columns": source_columns,
                            "target_columns": target_columns,
                            "rows_loaded": inserted,
                        })
                    finally:
                        audit.run_step(
                            "RELEASE_TABLE_LOCK",
                            lambda: release_table_run_lock(
                                snowflake_cursor,
                                snowflake_conn,
                                table_lock,
                                table_name,
                            ),
                            table_name,
                        )
            finally:
                audit.run_step("CLOSE_4D_CONNECTION", fourd_conn.close, audit_table_name)
        finally:
            audit.run_step(
                "CLOSE_SNOWFLAKE_CURSOR",
                snowflake_cursor.close,
                audit_table_name,
            )
        audit.finish_step(
            pipeline_step_number,
            "SUCCESS",
            "; ".join(
                (
                    f"No changes for {item['table']}"
                    if item.get("status") == "NO_CHANGE"
                    else f"Loaded {item['rows_loaded']} rows into {item['table']}"
                )
                for item in summary
            ),
        )
        return {"tables": summary}
    except KeyboardInterrupt:
        logger.warning("ETL execution interrupted")
        if audit_ready and pipeline_step_number is not None:
            try:
                audit.finish_step(
                    pipeline_step_number,
                    "INTERRUPTED",
                    "ETL pipeline was interrupted by the user",
                )
            except Exception:
                logger.exception("Could not store pipeline interruption in Snowflake audit table")
        raise
    except Exception as exc:
        logger.exception("ETL execution failed")
        if audit_ready and pipeline_step_number is not None:
            try:
                audit.finish_step(
                    pipeline_step_number,
                    "FAILED",
                    f"ETL pipeline failed: {exc}",
                    traceback.format_exc(),
                )
            except Exception:
                logger.exception("Could not store pipeline failure in Snowflake audit table")
        raise
    finally:
        snowflake_conn.close()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load 4D source tables into Snowflake.")
    parser.add_argument("--excel", default=str(DEFAULT_EXCEL_PATH), help="Path to the Excel mapping workbook")
    parser.add_argument(
        "--sheet",
        default=None,
        help="Excel sheet name to read (defaults to the sheet matching --table)",
    )
    table_group = parser.add_mutually_exclusive_group()
    table_group.add_argument(
        "--table",
        nargs="+",
        default=None,
        help="One target table name. Multiword names may be quoted or unquoted.",
    )
    table_group.add_argument(
        "--tables",
        nargs="+",
        default=None,
        help="Multiple target table names to load in one run.",
    )
    parser.add_argument(
        "--batch-size",
        type=_positive_int,
        default=50_000,
        help="Rows extracted and staged per Snowflake bulk-load batch (default: 50000).",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Maintain a current change-batch table and append prior current rows to a _H history table.",
    )
    parser.add_argument(
        "--overlap-seconds",
        type=_non_negative_seconds,
        default=86400,
        help="Re-read rows modified within this overlap window to catch timestamp ties (default: 86400).",
    )
    return parser.parse_args(argv)


def _non_negative_seconds(value: str) -> int:
    try:
        seconds = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("overlap seconds must be an integer") from exc
    if seconds < 0:
        raise argparse.ArgumentTypeError("overlap seconds cannot be negative")
    return seconds


def _positive_int(value: str) -> int:
    try:
        size = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("batch size must be an integer") from exc
    if size <= 0:
        raise argparse.ArgumentTypeError("batch size must be greater than zero")
    return size


def _find_mapping_sheet(excel_path: str, target_table: str) -> str:
    normalized_table = _normalize_target_table_name(target_table)
    sheet_name = next(
        (
            name
            for name in pd.ExcelFile(excel_path).sheet_names
            if _normalize_target_table_name(name) == normalized_table
        ),
        None,
    )
    if sheet_name is None:
        raise ValueError(
            f"No worksheet matching target table {normalized_table!r} was found. "
            "Specify the mapping worksheet with --sheet."
        )
    return sheet_name


def main() -> int:
    configure_logging()
    logger = logging.getLogger(__name__)
    try:
        args = _parse_args()
        if args.tables:
            table_names = args.tables
            sheet_name = {
                _normalize_target_table_name(table_name): (
                    args.sheet or _find_mapping_sheet(args.excel, table_name)
                )
                for table_name in table_names
            }
            selected_tables = table_names
            table_name = ", ".join(table_names)
        elif args.table:
            table_name = " ".join(args.table)
            sheet_name = args.sheet or _find_mapping_sheet(args.excel, table_name)
            selected_tables = table_name
        else:
            sheet_name = "Snowflake_table_names"
            table_name = None
            selected_tables = None

        logger.info(
            "Starting ETL run for table %s using worksheet %s",
            table_name or "all mapped tables",
            sheet_name,
        )
        result = run_for_table(
            args.excel,
            sheet_name,
            selected_tables,
            incremental=args.incremental,
            overlap_seconds=args.overlap_seconds,
            batch_size=args.batch_size,
        )
        for item in result["tables"]:
            if item.get("status") == "NO_CHANGE":
                message = f"No changes detected for {item['table']}"
            else:
                message = (
                    f"Loaded {item['rows_loaded']} changed rows into {item['table']} "
                    f"from {item['source_table']}"
                )
            logger.info(message)
            print(message)

        logger.info("ETL run completed successfully")
        return 0
    except Exception:
        logger.exception("ETL pipeline failed")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
