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

# Tables to skip during extraction (add more as needed)
TABLES_TO_SKIP = {
    "Acc_User_Passwords",  # ODBC error 08004 - Server rejected connection
    "Application",
    "Acc_Cashed_Cheque_Allocations",
    "Acc_Cashed_Cheques",
    "Account Joint Holders",
    "Account Reward Points",
    "Batches",
    "Budget_Table",
    "Claims_Relations",
    "Communications Log",
    "Converted Data Instructions",
    "Dialogs",
    "EB Claims",
    "EB Claims Service Dates",
    "Global Events",
    "Global_Driver_Convictions",
    "Holidays",
    "Marine Vessel Categories",
    "Mirror_Configs",
    "Payment Plans Terms",
    "Policy Payment Plans",
    "Policy Payment Plans Terms",
    "Portfolios",
    "Profile TP Limit Items",
    "Proposals",
    "Risk EB Employee Action Log","Risk EB Employees","Risk Extensions","SBX_Documents","SBX_Files","SBX_Pending Deletions","SBX_Photos","SBX_Schedules","Surveys",
    "System_Actions","Web Payment Request Log","Web_Proposal_Questions","zSubtable_Do_Not_Use"
}



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


def _source_mapping(connection, table_name: str, target_table: str) -> list[dict[str, str]]:
    cursor = connection.cursor()
    try:
        columns = list(cursor.columns(table=table_name))
    finally:
        cursor.close()
    rows = []
    for column in columns:
        if str(column.table_name or "").casefold() != table_name.casefold():
            continue
        name = str(column.column_name or "").strip()
        if not name:
            raise ValueError(f"4D returned an empty column name for {table_name}")
        rows.append({
            "source_table": table_name,
            "source_column": name,
            "source_data_type": str(column.type_name or ""),
            "target_table": target_table,
            "target_column": name,
            "snowflake_datatype": "VARCHAR",
        })
    if not rows:
        raise ValueError(f"4D returned no columns for table {table_name!r}")
    return rows


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
            "Extract all columns of 4D tables to final local Parquet files "
            "(or CSV with --format csv), and record extraction status in "
            "Snowflake ETL_PIPELINE_LOG. No extracted data is uploaded."
        )
    )
    parser.add_argument(
        "--table",
        required=True,
        help="4D table name, or 'all' for every accessible table",
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
    parser.add_argument(
        "--no-file-logging",
        action="store_true",
        default=False,
        help="Disable file logging, only log to Snowflake audit table",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    configure_logging(disable_file_logging=args.no_file_logging)
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
        all_tables = _normalize_name(args.table) == "ALL"
        try:
            source_connection = connect_to_4d()
            discovered = _discover_tables(source_connection)
        except Exception as exc:
            audit.record(
                step_name, "FAILED", args.table,
                f"Could not discover 4D tables: {exc}",
                error_message=traceback.format_exc(),
            )
            raise
        table_names = (
            discovered
            if all_tables
            else [
                name for name in discovered
                if _normalize_name(name) == _normalize_name(args.table)
            ]
        )
        if not table_names:
            message = f"No 4D table matches {args.table!r}"
            audit.record(step_name, "FAILED", args.table, message)
            raise ValueError(message)
        normalized_names = [_normalize_name(name) for name in table_names]
        if len(normalized_names) != len(set(normalized_names)):
            message = (
                "4D table names are ambiguous after normalization; "
                "cannot safely choose unique export destinations"
            )
            audit.record(step_name, "FAILED", args.table, message)
            raise ValueError(message)

        selected_tables = []
        print("table_names>>>>>>>>>>>",table_names)
        for table_name in table_names:
            try:
                # Skip tables in the skip list
                if table_name in TABLES_TO_SKIP:
                    message = f"Skipping {table_name}: Table is in skip list (blacklisted)"
                    logger.info(message)
                    print(message)
                    _record_skip(audit, step_name, table_name, message)
                    continue
                
                target_table = _normalize_name(table_name)
                if _safe_path_component(target_table, "Target table") != target_table:
                    raise ValueError(
                        f"4D table name cannot be used safely as an export target: "
                        f"{table_name!r}"
                    )
                if len(target_table) > 255:
                    raise ValueError(f"Target table name is too long: {target_table!r}")
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
                mapping_rows = _source_mapping(source_connection, table_name, target_table)
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
        
        print(f"Prepared {len(selected_tables)} tables for extraction")
        logger.info("Prepared %d tables for extraction", len(selected_tables))

        processed_count = 0
        for table_name, target_table, mapping_rows in selected_tables:
            processed_count += 1
            print(f"[{processed_count}/{len(selected_tables)}] Processing table: {table_name}")
            logger.info("Processing table %d/%d: %s", processed_count, len(selected_tables), table_name)
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
            except SourceRowCountUnavailable as exc:
                details = {
                    "reason": "SOURCE_ROW_COUNT_UNAVAILABLE",
                    "row_count": None,
                    "message": str(exc),
                }
                logger.warning("Skipping %s: %s; row count unknown", table_name, exc)
                audit.finish_step(table_step, "SKIPPED", json.dumps(details, sort_keys=True))
                print(f"Skipping {table_name}: {exc}; row count unknown")
            except Exception as exc:
                logger.exception("4D extraction failed for %s", table_name)
                print(f"Extraction failed for {table_name}: {exc}")
                audit.finish_step(
                    table_step, "FAILED", f"Extraction failed: {exc}",
                    error_message=traceback.format_exc(),
                )
                exit_code = 1
                try:
                    close_4d_connection(source_connection)
                    source_connection = connect_to_4d()
                    logger.info("Reconnected to 4D after extraction failure")
                except Exception as reconnect_exc:
                    logger.exception("Failed to reconnect to 4D after extraction error; continuing anyway")
        
        logger.info("Extraction loop completed: processed %d of %d tables", processed_count, len(selected_tables))
        print(f"Extraction run complete: {processed_count} of {len(selected_tables)} tables processed")
        
        # Record completion summary
        tables_remaining = len(selected_tables) - processed_count
        if tables_remaining > 0:
            summary_message = f"Extraction completed with {processed_count} tables processed, {tables_remaining} tables remaining"
            logger.warning(summary_message)
            print(f"WARNING: {summary_message}")
            try:
                audit.record(
                    step_name, "PARTIAL_COMPLETE", f"EXTRACTION_PROCESS_{args.table}",
                    summary_message,
                )
                logger.info("Recorded partial completion to audit table")
            except Exception as audit_exc:
                logger.exception("Could not record partial completion to audit table: %s", audit_exc)
        else:
            summary_message = f"Extraction completed successfully: all {processed_count} tables processed"
            logger.info(summary_message)
            print(f"SUCCESS: {summary_message}")
            try:
                audit.record(
                    step_name, "COMPLETE", f"EXTRACTION_PROCESS_{args.table}",
                    summary_message,
                )
                logger.info("Recorded successful completion to audit table")
            except Exception as audit_exc:
                logger.exception("Could not record completion to audit table: %s", audit_exc)
    except Exception as outer_exc:
        logger.exception("OUTER EXCEPTION - Extraction or audit logging failed for %s: %s", args.table, outer_exc)
        print(f"ERROR: Extraction stopped unexpectedly: {outer_exc}")
        print(f"Processed {processed_count} tables before stopping")
        
        # Record the reason for stopping to audit table
        try:
            stop_reason = f"Extraction stopped after processing {processed_count} tables. Reason: {str(outer_exc)}"
            audit.record(
                step_name, "STOPPED", f"EXTRACTION_PROCESS_{args.table}",
                stop_reason,
                error_message=traceback.format_exc(),
            )
            logger.info("Recorded extraction stop reason to audit table")
        except Exception as audit_exc:
            logger.exception("Could not record extraction stop reason to audit table: %s", audit_exc)
            print(f"WARNING: Could not log stop reason to audit: {audit_exc}")
        
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
