from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from connections.snowflake_connection import (
    close_snowflake_connection,
    connect_to_snowflake,
)

logger = logging.getLogger(__name__)


def _quote_identifier(identifier: str) -> str:
    if not identifier.strip():
        raise ValueError("Snowflake identifiers cannot be blank")
    return '"' + identifier.replace('"', '""') + '"'


def _qualified_name(database: str, schema: str, table: str) -> str:
    return ".".join(
        _quote_identifier(part) for part in (database, schema, table)
    )


def _build_timing_query(log_table: str) -> str:
    return f"""
        SELECT
            RUN_ID,
            TABLE_NAME,
            MIN(START_TIME) AS START_TIME,
            MAX(END_TIME) AS END_TIME,
            DATEDIFF(
                'millisecond',
                MIN(START_TIME),
                MAX(END_TIME)
            ) / 1000.0 AS TOTAL_TIME_SECONDS
        FROM {log_table}
        WHERE TABLE_NAME IS NOT NULL
          AND UPPER(TRIM(TABLE_NAME)) <> 'ETL_PIPELINE_LOG'
          AND START_TIME IS NOT NULL
          AND END_TIME IS NOT NULL
        GROUP BY RUN_ID, TABLE_NAME
        ORDER BY START_TIME, TABLE_NAME
    """


def _format_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"


def export_table_load_summary(
    connection,
    database: str,
    schema: str,
    output_path: Path,
) -> int:
    log_table = _qualified_name(database, schema, "ETL_PIPELINE_LOG")
    timing_query = _build_timing_query(log_table)
    cursor = connection.cursor()
    try:
        timing_rows = cursor.execute(timing_query).fetchall()
        report_rows = []
        for run_id, table_name, start_time, end_time, total_time_seconds in timing_rows:
            table = _qualified_name(database, schema, table_name)
            row_count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            report_rows.append(
                {
                    "RUN_ID": run_id,
                    "TABLE_NAME": table_name,
                    "NUMBER_OF_ROWS": row_count,
                    "START_TIME": _format_datetime(start_time),
                    "END_TIME": _format_datetime(end_time),
                    "TOTAL_TIME_SECONDS": total_time_seconds,
                }
            )
    finally:
        cursor.close()

    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "RUN_ID",
        "TABLE_NAME",
        "NUMBER_OF_ROWS",
        "START_TIME",
        "END_TIME",
        "TOTAL_TIME_SECONDS",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)

    logger.info("Saved %s table/run summaries to %s", len(report_rows), output_path)
    return len(report_rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Export per-run table timings and current row counts from "
            "Snowflake ETL_PIPELINE_LOG to CSV."
        )
    )
    parser.add_argument("--database", default="DW_AGAM_DEV")
    parser.add_argument("--schema", default="UW_RAW_STG")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("logs")
        / f"table_load_summary_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}_UTC.csv",
    )
    args = parser.parse_args(argv)

    configure_logging()
    connection = None
    try:
        connection = connect_to_snowflake()
        count = export_table_load_summary(
            connection,
            args.database,
            args.schema,
            args.output,
        )
        print(f"Exported {count} table/run summaries to {args.output.resolve()}")
        return 0
    except Exception:
        logger.exception("Could not export the table load summary")
        raise
    finally:
        close_snowflake_connection(connection)


if __name__ == "__main__":
    raise SystemExit(main())
