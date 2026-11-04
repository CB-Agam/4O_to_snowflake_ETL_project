from __future__ import annotations

import logging
import json
import tempfile
from collections.abc import Callable
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Sequence
from uuid import UUID, uuid4

import pandas as pd

from etl.ddl import build_create_table_sql

logger = logging.getLogger(__name__)
_ETL_TIMESTAMP_COLUMNS = ("CREATED_TIMESTAMP", "UPDATED_TIMESTAMP")
_CURRENT_TIMESTAMP_NTZ = "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"


def _json_value(value: Any) -> Any:
    value = _clean_value(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Unsupported source value for JSON staging: {type(value).__name__}")


def _copy_staged_json_file(
    snowflake_cursor,
    stage: str,
    stage_name: str,
    staged_filename: str,
    file_path: Path,
    expected_rows: int,
) -> int:
    file_uri = file_path.resolve().as_uri().replace("'", "''")
    snowflake_cursor.execute(
        f"PUT '{file_uri}' @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    put_columns = [
        str(description[0]).casefold()
        for description in (snowflake_cursor.description or [])
    ]
    put_rows = snowflake_cursor.fetchall()
    if "status" not in put_columns or not put_rows:
        raise RuntimeError(f"Snowflake PUT returned no upload status for {staged_filename}")
    status_index = put_columns.index("status")
    message_index = put_columns.index("message") if "message" in put_columns else None
    for row in put_rows:
        status = str(row[status_index]).upper()
        if status not in {"UPLOADED", "SKIPPED"}:
            message = row[message_index] if message_index is not None else status
            raise RuntimeError(f"Snowflake stage upload failed: {message}")

    snowflake_cursor.execute(
        f"COPY INTO {stage_name} FROM @{stage} "
        f"FILES = ('{staged_filename}') "
        "FILE_FORMAT=(TYPE='JSON' STRIP_OUTER_ARRAY=TRUE) "
        "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"
    )
    copy_columns = [
        str(description[0]).casefold()
        for description in (snowflake_cursor.description or [])
    ]
    copy_rows = snowflake_cursor.fetchall()
    if not copy_columns or not copy_rows or "rows_loaded" not in copy_columns:
        raise RuntimeError(f"Snowflake COPY INTO returned no load status for {staged_filename}")
    loaded_index = copy_columns.index("rows_loaded")
    errors_index = copy_columns.index("errors_seen") if "errors_seen" in copy_columns else None
    loaded_rows = sum(int(row[loaded_index] or 0) for row in copy_rows)
    errors = sum(int(row[errors_index] or 0) for row in copy_rows) if errors_index is not None else 0
    if errors:
        raise RuntimeError(f"Snowflake COPY INTO reported {errors} load error(s)")
    if loaded_rows != expected_rows:
        raise RuntimeError(
            f"Snowflake copied {loaded_rows} of {expected_rows} "
            f"rows from {staged_filename}"
        )
    snowflake_cursor.execute(f"REMOVE @{stage}/{staged_filename}")
    return loaded_rows


def get_table_rows(metadata: Dict[str, List[Dict[str, str]]], table_name: str) -> List[Dict[str, str]]:
    normalized_name = str(table_name).strip().upper().replace(" ", "_").replace("-", "_")
    return metadata.get(normalized_name, [])


def build_source_query(source_table: str, source_columns: Sequence[str]) -> str:
    columns = [str(column).strip() for column in source_columns if str(column).strip()]
    if not columns:
        raise ValueError(f"No source columns provided for table: {source_table}")

    def quote_identifier(identifier: str) -> str:
        return "[" + identifier.replace("]", "]]") + "]"

    quoted_columns = ", ".join(quote_identifier(column) for column in columns)
    return f"SELECT {quoted_columns} FROM {quote_identifier(source_table.strip())}"


def build_insert_sql(table_name: str, target_columns: Sequence[str], schema_name: str | None = None) -> str:
    source_columns = [
        column
        for column in target_columns
        if column.upper() not in _ETL_TIMESTAMP_COLUMNS
    ]
    inserted_columns = [*source_columns, *_ETL_TIMESTAMP_COLUMNS]

    qualified_name = table_name
    if schema_name:
        qualified_name = f"{schema_name}.{table_name}"
    columns_sql = ", ".join(inserted_columns)
    values = ["%s"] * len(source_columns) + [_CURRENT_TIMESTAMP_NTZ] * len(_ETL_TIMESTAMP_COLUMNS)
    placeholders = ", ".join(values)
    return f"INSERT INTO {qualified_name} ({columns_sql}) VALUES ({placeholders})"


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime()
        except Exception:
            return value
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def load_dataframe_to_snowflake(
    snowflake_cursor,
    table_name: str,
    target_columns: Sequence[str],
    df: pd.DataFrame,
    schema_name: str | None = None,
) -> int:
    if df.empty:
        return 0

    ordered_columns = [
        column
        for column in target_columns
        if column.upper() not in _ETL_TIMESTAMP_COLUMNS
    ]
    df = df.copy()
    df = df.loc[:, ordered_columns]
    rows = [tuple(_clean_value(value) for value in row) for row in df.itertuples(index=False, name=None)]

    if not rows:
        return 0

    sql = build_insert_sql(table_name, ordered_columns, schema_name=schema_name)
    snowflake_cursor.executemany(sql, rows)
    return len(rows)


def load_query_to_snowflake_in_batches(
    source_connection,
    snowflake_connection,
    snowflake_cursor,
    query: str,
    database_name: str,
    schema_name: str,
    table_name: str,
    target_columns: Sequence[str],
    batch_size: int = 50_000,
    before_publish: Callable[[], None] | None = None,
) -> int:
    if batch_size <= 0:
        raise ValueError("Batch size must be greater than zero")

    source_target_columns = [
        column
        for column in target_columns
        if column.upper() not in _ETL_TIMESTAMP_COLUMNS
    ]
    if not source_target_columns:
        raise ValueError(f"No source columns provided for table: {table_name}")

    def quote_identifier(identifier: str) -> str:
        if not identifier or "\x00" in identifier:
            raise ValueError(f"Invalid Snowflake identifier: {identifier!r}")
        return '"' + identifier.replace('"', '""') + '"'

    def qualify(database: str, schema: str, table: str) -> str:
        return ".".join(
            quote_identifier(part) for part in (database, schema, table)
        )

    target = qualify(database_name, schema_name, table_name)
    stage_name = f"ETL_STAGE_{uuid4().hex[:24].upper()}"
    stage = qualify(database_name, schema_name, stage_name)
    stage_table_name = f"ETL_LOAD_{uuid4().hex[:24].upper()}"
    stage_table = qualify(database_name, schema_name, stage_table_name)
    columns_sql = ", ".join(quote_identifier(column) for column in source_target_columns)
    created_timestamp = quote_identifier("CREATED_TIMESTAMP")
    updated_timestamp = quote_identifier("UPDATED_TIMESTAMP")
    stage_created = False
    operation_error: BaseException | None = None
    transaction_started = False
    total_staged = 0

    try:
        snowflake_cursor.execute(
            f"CREATE TEMPORARY TABLE {stage_table} AS "
            f"SELECT {columns_sql} FROM {target} WHERE 1 = 0"
        )
        stage_created = True
        snowflake_cursor.execute(
            f"CREATE TEMPORARY STAGE {stage} "
            "FILE_FORMAT=(TYPE='JSON' STRIP_OUTER_ARRAY=TRUE)"
        )

        source_cursor = source_connection.cursor()
        try:
            source_cursor.execute(query)
            batch_number = 0
            while True:
                rows = source_cursor.fetchmany(batch_size)
                if not rows:
                    break
                batch_number += 1
                staged_filename = f"{stage_name}_{batch_number}.json"
                with tempfile.TemporaryDirectory(
                    prefix=f"{stage_name}_{batch_number}_"
                ) as temp_dir:
                    file_path = Path(temp_dir) / staged_filename
                    with file_path.open("w", encoding="utf-8", newline="") as output:
                        output.write("[")
                        for row_index, row in enumerate(rows):
                            if len(row) != len(target_columns):
                                raise ValueError(
                                    f"Source batch {batch_number} returned "
                                    f"{len(row)} column(s); expected "
                                    f"{len(target_columns)} for {table_name}"
                                )
                            if row_index:
                                output.write(",")
                            record = {
                                column: _json_value(value)
                                for column, value in zip(
                                    target_columns,
                                    row,
                                    strict=True,
                                )
                                if column.upper() not in _ETL_TIMESTAMP_COLUMNS
                            }
                            json.dump(
                                record,
                                output,
                                ensure_ascii=False,
                                separators=(",", ":"),
                                allow_nan=False,
                                default=_json_value,
                            )
                        output.write("]")
                    loaded_rows = _copy_staged_json_file(
                        snowflake_cursor,
                        stage,
                        stage_table,
                        staged_filename,
                        file_path,
                        len(rows),
                    )
                total_staged += loaded_rows
                logger.info(
                    "Staged batch %s for %s: %s rows (%s total)",
                    batch_number,
                    table_name,
                    loaded_rows,
                    total_staged,
                )
        finally:
            source_cursor.close()

        staged_count = snowflake_cursor.execute(
            f"SELECT COUNT(*) FROM {stage_table}"
        ).fetchone()
        if not staged_count or int(staged_count[0]) != total_staged:
            actual_rows = int(staged_count[0]) if staged_count else 0
            raise RuntimeError(
                f"Staging row count mismatch for {table_name}: "
                f"expected {total_staged}, found {actual_rows}"
            )

        if before_publish is not None:
            before_publish()
        if total_staged:
            snowflake_cursor.execute("BEGIN")
            transaction_started = True
            insert_columns = [
                *[quote_identifier(column) for column in source_target_columns],
                created_timestamp,
                updated_timestamp,
            ]
            select_columns = [
                *[quote_identifier(column) for column in source_target_columns],
                _CURRENT_TIMESTAMP_NTZ,
                _CURRENT_TIMESTAMP_NTZ,
            ]
            snowflake_cursor.execute(
                f"INSERT INTO {target} ({', '.join(insert_columns)}) "
                f"SELECT {', '.join(select_columns)} FROM {stage_table}"
            )
            snowflake_cursor.execute("COMMIT")
            transaction_started = False
        return total_staged
    except BaseException as exc:
        operation_error = exc
        if transaction_started:
            try:
                snowflake_connection.rollback()
            except Exception:
                logger.exception(
                    "Could not roll back batched load for %s",
                    table_name,
                )
        raise
    finally:
        if stage_created:
            try:
                snowflake_cursor.execute(f"DROP TABLE IF EXISTS {stage_table}")
            except Exception as cleanup_error:
                logger.exception(
                    "Could not remove temporary staging table for %s",
                    table_name,
                )
                if operation_error is None:
                    raise RuntimeError(
                        f"Batched load for {table_name} completed with staging cleanup failure"
                    ) from cleanup_error


def create_missing_tables_from_metadata(cursor, metadata: Dict[str, List[Dict[str, str]]], schema_name: str) -> List[str]:
    created_tables: List[str] = []
    for table_name, rows in metadata.items():
        if not rows:
            continue

        if table_exists_in_snowflake(cursor, schema_name, table_name):
            continue

        ddl = build_create_table_sql(table_name, rows, schema_name=schema_name)
        cursor.execute(ddl)
        created_tables.append(table_name)
    return created_tables


def table_exists_in_snowflake(cursor, schema_name: str, table_name: str) -> bool:
    sql = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
    """
    result = cursor.execute(sql, (schema_name, table_name)).fetchone()
    return (result[0] if result else 0) > 0
