from __future__ import annotations

import argparse
import logging
import sys
import traceback
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
from etl.ddl import build_create_table_sql
from etl.load import build_source_query, load_dataframe_to_snowflake, table_exists_in_snowflake
from etl.metadata import load_metadata_from_excel


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EXCEL_PATH = PROJECT_ROOT / "4D_to_Snowflake (1-50 tables mapping).xlsx"
logger = logging.getLogger(__name__)


def _normalize_target_table_name(value: str) -> str:
    return str(value).strip().upper().replace(" ", "_").replace("-", "_")


def run_for_table(excel_path: str, sheet_name: str, target_table: str | None = None) -> dict:
    snowflake_conn = connect_to_snowflake()
    audit = SnowflakeAuditLogger(
        snowflake_conn,
        settings.snowflake_database,
        settings.snowflake_schema,
    )
    try:
        audit.ensure_table()
        audit.record(
            "CONNECT_SNOWFLAKE",
            "SUCCESS",
            target_table,
            "Snowflake connection established",
        )
        audit.record(
            "ENSURE_AUDIT_TABLE",
            "SUCCESS",
            target_table,
            "Snowflake audit table is ready",
        )

        metadata = audit.run_step(
            "LOAD_MAPPING",
            lambda: load_metadata_from_excel(excel_path, sheet_name=sheet_name),
            target_table,
        )
        selected_tables = metadata
        if target_table:
            normalized_target = _normalize_target_table_name(target_table)
            selected_tables = {normalized_target: metadata.get(normalized_target, [])}

        if not selected_tables or not any(selected_tables.values()):
            raise ValueError(
                f"No source-to-target mappings found for table: {target_table or sheet_name}"
            )

        snowflake_cursor = audit.run_step("OPEN_SNOWFLAKE_CURSOR", snowflake_conn.cursor)
        try:
            for table_name, rows in selected_tables.items():
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

                    query = build_source_query(source_table, source_columns)
                    df = audit.run_step(
                        "EXTRACT_SOURCE_DATA",
                        lambda: pd.read_sql(query, fourd_conn),
                        table_name,
                    )
                    df.columns = target_columns
                    inserted = audit.run_step(
                        "INSERT_TARGET_DATA",
                        lambda: load_dataframe_to_snowflake(
                            snowflake_cursor,
                            table_name,
                            target_columns,
                            df,
                            schema_name=settings.snowflake_schema,
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
                audit.record(
                    "ETL_PIPELINE",
                    "SUCCESS",
                    target_table,
                    "; ".join(
                        f"Loaded {item['rows_loaded']} rows into {item['table']}"
                        for item in summary
                    ),
                )
                return {"tables": summary}
            finally:
                audit.run_step("CLOSE_4D_CONNECTION", fourd_conn.close, target_table)
        finally:
            audit.run_step("CLOSE_SNOWFLAKE_CURSOR", snowflake_cursor.close, target_table)
    except Exception as exc:
        logger.exception("ETL execution failed")
        try:
            audit.record(
                "ETL_PIPELINE",
                "FAILURE",
                target_table,
                f"ETL pipeline failed: {exc}",
                traceback.format_exc(),
            )
        except Exception:
            logger.exception("Could not store pipeline failure in Snowflake audit table")
        raise
    finally:
        snowflake_conn.close()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create missing Snowflake tables from the Excel mapping and load data from 4D.")
    parser.add_argument("--excel", default=str(DEFAULT_EXCEL_PATH), help="Path to the Excel mapping workbook")
    parser.add_argument(
        "--sheet",
        default=None,
        help="Excel sheet name to read (defaults to the sheet matching --table)",
    )
    parser.add_argument(
        "--table",
        nargs="+",
        default=None,
        help="Optional target table name to process. Multiword names may be quoted or unquoted.",
    )
    return parser.parse_args(argv)


def main() -> int:
    configure_logging()
    logger = logging.getLogger(__name__)
    try:
        args = _parse_args()
        table_name = " ".join(args.table) if args.table else None

        if args.sheet:
            sheet_name = args.sheet
        elif table_name:
            normalized_table = _normalize_target_table_name(table_name)
            sheet_name = next(
                (
                    name
                    for name in pd.ExcelFile(args.excel).sheet_names
                    if _normalize_target_table_name(name) == normalized_table
                ),
                None,
            )
            if sheet_name is None:
                raise ValueError(
                    f"No worksheet matching target table {normalized_table!r} was found. "
                    "Specify the mapping worksheet with --sheet."
                )
        else:
            sheet_name = "Snowflake_table_names"

        logger.info(
            "Starting ETL run for table %s using worksheet %s",
            table_name or "all mapped tables",
            sheet_name,
        )
        result = run_for_table(args.excel, sheet_name, table_name)
        for item in result["tables"]:
            message = (
                f"Loaded {item['rows_loaded']} rows into {item['table']} "
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
