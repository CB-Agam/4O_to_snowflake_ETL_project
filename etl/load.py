from __future__ import annotations

import logging
import json
import tempfile
from collections.abc import Callable
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Sequence
from uuid import UUID, uuid4

import pandas as pd

from etl.ddl import build_create_table_sql
from etl.exception_log import SnowflakeExceptionLogger

logger = logging.getLogger(__name__)
_ETL_TIMESTAMP_COLUMNS = ("CREATED_TIMESTAMP", "UPDATED_TIMESTAMP")
_CURRENT_TIMESTAMP_NTZ = "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"


class RowLoadError(RuntimeError):
    """A bulk COPY completed with source-row errors."""


def _is_row_level_error(error: BaseException) -> bool:
    if isinstance(error, RowLoadError):
        return True
    sqlstate = str(getattr(error, "sqlstate", "") or "")
    if sqlstate.startswith(("22", "23")):
        return True
    return getattr(error, "errno", None) in {
        100038,
        100039,
        100040,
        100071,
        100072,
    }


def _log_row_failure(
    exception_logger: SnowflakeExceptionLogger | None,
    *,
    pipeline_name: str,
    source_table: str,
    target_table: str,
    row_number: int,
    record: dict[str, Any],
    error: BaseException,
    batch_id: str,
) -> None:
    if exception_logger is None:
        raise RuntimeError(
            "A row failed but no ETL_EXCEPTION_LOG writer was configured"
        ) from error
    exception_logger.log_row_failure(
        pipeline_name=pipeline_name,
        source_table=source_table,
        target_table=target_table,
        row_number=row_number,
        record=record,
        error=error,
        batch_id=batch_id,
    )


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
        detail_index = copy_columns.index("first_error") if "first_error" in copy_columns else None
        detail = (
            "; ".join(str(row[detail_index]) for row in copy_rows if row[detail_index])
            if detail_index is not None
            else ""
        )
        message = f"Snowflake COPY INTO reported {errors} load error(s)"
        if detail:
            message = f"{message}: {detail}"
        raise RowLoadError(message)
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


def load_dataframe_to_snowflake_with_exceptions(
    snowflake_connection,
    snowflake_cursor,
    table_name: str,
    target_columns: Sequence[str],
    df: pd.DataFrame,
    *,
    exception_logger: SnowflakeExceptionLogger,
    pipeline_name: str,
    source_table: str,
    batch_id: str,
    first_row_number: int = 1,
    schema_name: str | None = None,
    target_table_name: str | None = None,
) -> dict[str, Any]:
    if df.empty:
        return {"rows_loaded": 0, "rows_failed": 0, "failed_indexes": []}

    ordered_columns = [
        column
        for column in target_columns
        if column.upper() not in _ETL_TIMESTAMP_COLUMNS
    ]
    ordered_df = df.loc[:, ordered_columns]
    rows = [
        tuple(_clean_value(value) for value in row)
        for row in ordered_df.itertuples(index=False, name=None)
    ]
    records = [
        dict(zip(ordered_columns, row, strict=True))
        for row in rows
    ]
    sql = build_insert_sql(table_name, ordered_columns, schema_name=schema_name)
    failed_indexes: list[int] = []

    def insert_with_isolation(indexes: list[int]) -> None:
        if not indexes:
            return
        try:
            snowflake_cursor.execute("BEGIN")
            snowflake_cursor.executemany(sql, [rows[index] for index in indexes])
            snowflake_cursor.execute("COMMIT")
        except BaseException as exc:
            try:
                snowflake_connection.rollback()
            except Exception:
                logger.exception("Could not roll back staged row batch for %s", table_name)
                raise
            if not isinstance(exc, Exception):
                raise
            if not _is_row_level_error(exc):
                raise
            if len(indexes) > 1:
                middle = len(indexes) // 2
                insert_with_isolation(indexes[:middle])
                insert_with_isolation(indexes[middle:])
                return
            index = indexes[0]
            row_number = first_row_number + index
            _log_row_failure(
                exception_logger,
                pipeline_name=pipeline_name,
                source_table=source_table,
                target_table=target_table_name or table_name,
                row_number=row_number,
                record=records[index],
                error=exc,
                batch_id=batch_id,
            )
            failed_indexes.append(index)

    insert_with_isolation(list(range(len(rows))))
    return {
        "rows_loaded": len(rows) - len(failed_indexes),
        "rows_failed": len(failed_indexes),
        "failed_indexes": failed_indexes,
    }


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
    exception_logger: SnowflakeExceptionLogger | None = None,
    source_table: str | None = None,
    pipeline_name: str = "4D_TO_SNOWFLAKE_ETL",
    batch_id: str | None = None,
    return_stats: bool = False,
) -> int | dict[str, Any]:
    if batch_size <= 0:
        raise ValueError("Batch size must be greater than zero")

    load_started = datetime.now(timezone.utc)
    batch_id = batch_id or str(uuid4())
    source_table = source_table or table_name
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
    stage_row_number_name = f"__ETL_ROW_NUMBER_{uuid4().hex[:8].upper()}"
    columns_sql = ", ".join(quote_identifier(column) for column in source_target_columns)
    created_timestamp = quote_identifier("CREATED_TIMESTAMP")
    updated_timestamp = quote_identifier("UPDATED_TIMESTAMP")
    stage_row_number = quote_identifier(stage_row_number_name)
    stage_created = False
    stage_object_created = False
    operation_error: BaseException | None = None
    total_staged = 0
    total_processed = 0
    total_published = 0
    failed_row_count = 0
    copy_attempt = 0

    try:
        snowflake_cursor.execute(
            f"CREATE TEMPORARY TABLE {stage_table} AS "
            f"SELECT {columns_sql} FROM {target} WHERE 1 = 0"
        )
        stage_created = True
        snowflake_cursor.execute(
            f"ALTER TABLE {stage_table} ADD COLUMN {stage_row_number} NUMBER(38, 0)"
        )
        snowflake_cursor.execute(
            f"CREATE TEMPORARY STAGE {stage} "
            "FILE_FORMAT=(TYPE='JSON' STRIP_OUTER_ARRAY=TRUE)"
        )
        stage_object_created = True

        source_cursor = source_connection.cursor()
        try:
            source_cursor.execute(query)
            batch_number = 0
            while True:
                rows = source_cursor.fetchmany(batch_size)
                if not rows:
                    break
                batch_number += 1
                batch_items: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
                for row in rows:
                    total_processed += 1
                    row_number = total_processed
                    if len(row) != len(target_columns):
                        raise ValueError(
                            f"Source batch {batch_number} returned "
                            f"{len(row)} column(s); expected "
                            f"{len(target_columns)} for {table_name}"
                        )
                    record = {
                        column: _clean_value(value)
                        for column, value in zip(target_columns, row, strict=True)
                        if column.upper() not in _ETL_TIMESTAMP_COLUMNS
                    }
                    try:
                        json_record = {
                            column: _json_value(value)
                            for column, value in record.items()
                        }
                        json.dumps(
                            json_record,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            allow_nan=False,
                            default=_json_value,
                        )
                    except (TypeError, ValueError) as exc:
                        failed_row_count += 1
                        _log_row_failure(
                            exception_logger,
                            pipeline_name=pipeline_name,
                            source_table=source_table,
                            target_table=table_name,
                            row_number=row_number,
                            record=record,
                            error=exc,
                            batch_id=batch_id,
                        )
                    else:
                        batch_items.append((row_number, record, json_record))

                def copy_with_isolation(
                    items: list[tuple[int, dict[str, Any], dict[str, Any]]],
                ) -> int:
                    nonlocal copy_attempt, failed_row_count
                    if not items:
                        return 0
                    copy_attempt += 1
                    attempt_name = f"{stage_name}_{batch_number}_{copy_attempt}.json"
                    with tempfile.TemporaryDirectory(
                        prefix=f"{stage_name}_{batch_number}_{copy_attempt}_"
                    ) as temp_dir:
                        file_path = Path(temp_dir) / attempt_name
                        with file_path.open("w", encoding="utf-8", newline="") as output:
                            json.dump(
                                [
                                    {**item[2], stage_row_number_name: item[0]}
                                    for item in items
                                ],
                                output,
                                ensure_ascii=False,
                                separators=(",", ":"),
                                allow_nan=False,
                                default=_json_value,
                            )
                        try:
                            loaded = _copy_staged_json_file(
                                snowflake_cursor,
                                stage,
                                stage_table,
                                attempt_name,
                                file_path,
                                len(items),
                            )
                        except RowLoadError as exc:
                            snowflake_cursor.execute(
                                f"DELETE FROM {stage_table} WHERE {stage_row_number} "
                                "BETWEEN %s AND %s",
                                (items[0][0], items[-1][0]),
                            )
                            if len(items) == 1:
                                row_number, record, _ = items[0]
                                _log_row_failure(
                                    exception_logger,
                                    pipeline_name=pipeline_name,
                                    source_table=source_table,
                                    target_table=table_name,
                                    row_number=row_number,
                                    record=record,
                                    error=exc,
                                    batch_id=batch_id,
                                )
                                failed_row_count += 1
                                return 0
                            middle = len(items) // 2
                            return (
                                copy_with_isolation(items[:middle])
                                + copy_with_isolation(items[middle:])
                            )
                    return loaded

                loaded_rows = copy_with_isolation(batch_items)
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

        def publish_with_isolation(first_number: int, last_number: int) -> int:
            nonlocal total_published, failed_row_count
            if first_number > last_number:
                return 0
            count_result = snowflake_cursor.execute(
                f"SELECT COUNT(*) FROM {stage_table} WHERE {stage_row_number} "
                "BETWEEN %s AND %s",
                (first_number, last_number),
            ).fetchone()
            rows_to_publish = int(count_result[0] or 0) if count_result else 0
            if rows_to_publish == 0:
                return 0
            try:
                snowflake_cursor.execute("BEGIN")
                snowflake_cursor.execute(
                    f"INSERT INTO {target} ({', '.join(insert_columns)}) "
                    f"SELECT {', '.join(select_columns)} FROM {stage_table} "
                    f"WHERE {stage_row_number} BETWEEN %s AND %s",
                    (first_number, last_number),
                )
                snowflake_cursor.execute("COMMIT")
            except BaseException as exc:
                try:
                    snowflake_connection.rollback()
                except Exception:
                    logger.exception(
                        "Could not roll back target rows for %s",
                        table_name,
                    )
                    raise
                if not isinstance(exc, Exception):
                    raise
                if not _is_row_level_error(exc):
                    raise
                if first_number < last_number:
                    middle = (first_number + last_number) // 2
                    return (
                        publish_with_isolation(first_number, middle)
                        + publish_with_isolation(middle + 1, last_number)
                    )
                row_number = first_number
                record_row = snowflake_cursor.execute(
                    f"SELECT {columns_sql} FROM {stage_table} "
                    f"WHERE {stage_row_number} = %s",
                    (row_number,),
                ).fetchone()
                if record_row is None:
                    raise RuntimeError(
                        f"Could not retrieve failed source row {row_number} "
                        f"from staging for {table_name}"
                    ) from exc
                _log_row_failure(
                    exception_logger,
                    pipeline_name=pipeline_name,
                    source_table=source_table,
                    target_table=table_name,
                    row_number=row_number,
                    record=dict(zip(source_target_columns, record_row, strict=True)),
                    error=exc,
                    batch_id=batch_id,
                )
                failed_row_count += 1
                return 0
            total_published += rows_to_publish
            return rows_to_publish

        if total_processed:
            publish_with_isolation(1, total_processed)
        end_time = datetime.now(timezone.utc)
        if return_stats:
            status = "SUCCESS_WITH_ERRORS" if failed_row_count else "SUCCESS"
            return {
                "source_table": source_table,
                "target_table": table_name,
                "rows_processed": total_processed,
                "rows_loaded": total_published,
                "rows_failed": failed_row_count,
                "exception_count": failed_row_count,
                "total_records_processed": total_processed,
                "successful_record_count": total_published,
                "failed_record_count": failed_row_count,
                "exception_log_count": failed_row_count,
                "batch_id": batch_id,
                "load_started_at": load_started.isoformat(),
                "load_ended_at": end_time.isoformat(),
                "status": status,
                "overall_status": status,
            }
        return total_published
    except BaseException as exc:
        operation_error = exc
        raise
    finally:
        cleanup_error: Exception | None = None
        if stage_object_created:
            try:
                snowflake_cursor.execute(f"DROP STAGE IF EXISTS {stage}")
            except Exception as exc:
                logger.exception("Could not remove temporary stage for %s", table_name)
                cleanup_error = exc
        if stage_created:
            try:
                snowflake_cursor.execute(f"DROP TABLE IF EXISTS {stage_table}")
            except Exception as exc:
                logger.exception(
                    "Could not remove temporary staging table for %s",
                    table_name,
                )
                if cleanup_error is None:
                    cleanup_error = exc
        if operation_error is None and cleanup_error is not None:
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
