from __future__ import annotations

import logging
import json
import tempfile
import shutil
import time as time_module
from collections.abc import Callable
from datetime import date, datetime, time as datetime_time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Sequence
from uuid import UUID, uuid4

import pandas as pd

from etl.ddl import build_create_table_sql
from etl.quarantine import record_quarantine

logger = logging.getLogger(__name__)
_ETL_TIMESTAMP_COLUMNS = ("CREATED_TIMESTAMP", "UPDATED_TIMESTAMP")
_SOURCE_RAW_TIMESTAMP_COLUMNS = {
    "CREATED_ON",
    "MODIFIED_ON",
    "CREATED_DATE",
    "MODIFIED_DATE",
    "CREATED_TIMESTAMP",
    "MODIFIED_TIMESTAMP",
    "UPDATED_ON",
    "UPDATED_TIMESTAMP",
}


def _record_quarantine_on_connection(
    snowflake_connection,
    database_name: str,
    schema_name: str,
    run_id: str,
    target_table: str,
    source_table: str | None,
    batch_number: int,
    record: dict[str, Any],
    error: BaseException | str,
    target_columns: Sequence[str],
) -> None:
    quarantine_cursor = snowflake_connection.cursor()
    try:
        record_quarantine(
            quarantine_cursor,
            database_name,
            schema_name,
            run_id,
            target_table,
            source_table,
            batch_number,
            record,
            error,
            target_columns,
        )
        snowflake_connection.commit()
    finally:
        quarantine_cursor.close()

_CURRENT_TIMESTAMP_NTZ = "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"


class StageUploadError(RuntimeError):
    """Local file could not be uploaded; do not treat this as source-row data."""


def _normalize_source_timestamp(value: Any, column: str) -> Any:
    if column.strip().upper().replace(" ", "_").replace("-", "_") not in _SOURCE_RAW_TIMESTAMP_COLUMNS:
        return _clean_value(value)

    cleaned = _clean_value(value)
    if cleaned is None:
        return None
    if isinstance(cleaned, datetime):
        if not 1 <= cleaned.year <= 9999:
            raise ValueError(f"year {cleaned.year} is outside the supported timestamp range")
        return cleaned.isoformat()
    if isinstance(cleaned, (date, datetime_time)):
        return cleaned.isoformat()
    if not isinstance(cleaned, str):
        return _json_value(cleaned)

    raw = cleaned.strip()
    if not raw:
        return None
    for date_format in (
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            parsed = datetime.strptime(raw, date_format)
            return parsed.isoformat()
        except ValueError:
            pass
    raise ValueError(f"Invalid source timestamp for {column}: {raw!r}")


def _json_value(value: Any) -> Any:
    value = _clean_value(value)
    if isinstance(value, (datetime, date, datetime_time)):
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
    if not file_path.is_file():
        raise StageUploadError(f"Staged local file does not exist: {file_path}")
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            if not file_path.is_file():
                raise StageUploadError(f"Staged local file does not exist: {file_path}")
            snowflake_cursor.execute(
                f"PUT '{file_uri}' @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            last_error = None
            break
        except Exception as exc:
            last_error = exc
            message = str(exc).casefold()
            if "failed to upload files" in message or "file does not exist" in message:
                if attempt < 2:
                    time_module.sleep(1 * (attempt + 1))
                    continue
                raise StageUploadError(str(exc)) from exc
            raise
    if last_error is not None:
        raise last_error
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

    def source_expression(column: str) -> str:
        normalized = column.strip().upper().replace(" ", "_").replace("-", "_")
        if normalized in _SOURCE_RAW_TIMESTAMP_COLUMNS:
            # Fetch source timestamps as text. 4D/ODBC otherwise converts invalid
            # years (for example 13698) while fetchmany() runs and hides the row
            # from Python so it cannot be quarantined.
            return f"CAST({quote_identifier(column)} AS VARCHAR)"
        return quote_identifier(column)

    selected_columns = ", ".join(source_expression(column) for column in columns)
    return f"SELECT {selected_columns} FROM {quote_identifier(source_table.strip())}"


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
    try:
        if pd.isna(value):
            return None
    except Exception as exc:
        raise ValueError(f"Could not inspect source value {value!r}: {exc}") from exc
    if isinstance(value, pd.Timestamp):
        try:
            stamp = value
            if not 1 <= stamp.year <= 9999:
                raise ValueError(f"year {stamp.year} is outside the supported timestamp range")
            return stamp.to_pydatetime()
        except Exception as exc:
            raise ValueError(f"Invalid timestamp value {value!r}: {exc}") from exc
    if hasattr(value, "to_pydatetime"):
        try:
            stamp = value.to_pydatetime()
        except Exception as exc:
            raise ValueError(f"Invalid timestamp value {value!r}: {exc}") from exc
        if not 1 <= stamp.year <= 9999:
            raise ValueError(f"year {stamp.year} is outside the supported timestamp range")
        return stamp
    if isinstance(value, float):
        return Decimal(str(value))
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
    run_id: str | None = None,
    source_table: str | None = None,
    quarantine_invalid_rows: bool = True,
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
    staging_temp_dir = Path(tempfile.mkdtemp(prefix=f"{stage_name}_"))

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

            def load_source_subset(subset: Sequence[Sequence[Any]], subset_tag: str) -> int:
                valid_records: list[dict[str, Any]] = []
                for row in subset:
                    if isinstance(row, dict):
                        valid_records.append(row)
                        continue
                    if len(row) != len(target_columns):
                        exc = ValueError(
                            f"Source row returned {len(row)} column(s); expected "
                            f"{len(target_columns)} for {table_name}"
                        )
                        if run_id and quarantine_invalid_rows:
                            _record_quarantine_on_connection(
                                snowflake_connection, database_name, schema_name, run_id,
                                table_name, source_table, batch_number,
                                {"raw_source_row": str(row)}, exc, target_columns,
                            )
                            continue
                        raise exc
                    try:
                        record = {
                            column: _normalize_source_timestamp(value, column)
                            for column, value in zip(target_columns, row, strict=True)
                            if column.upper() not in _ETL_TIMESTAMP_COLUMNS
                        }
                        json.dumps(record, ensure_ascii=False, allow_nan=False, default=_json_value)
                        valid_records.append(record)
                    except Exception as exc:
                        if run_id and quarantine_invalid_rows:
                            _record_quarantine_on_connection(
                                snowflake_connection, database_name, schema_name, run_id,
                                table_name, source_table, batch_number,
                                dict(zip(target_columns, row, strict=False)), exc, target_columns,
                            )
                            continue
                        raise

                if not valid_records:
                    return 0

                staged_filename = f"{stage_name}_{subset_tag}.json"
                file_path = staging_temp_dir / staged_filename
                with file_path.open("w", encoding="utf-8", newline="") as output:
                    json.dump(
                        valid_records,
                        output,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        allow_nan=False,
                        default=_json_value,
                    )
                    output.flush()
                try:
                    return _copy_staged_json_file(
                        snowflake_cursor, stage, stage_table,
                        staged_filename, file_path, len(valid_records),
                    )
                except StageUploadError:
                    raise
                except Exception as exc:
                    if run_id and quarantine_invalid_rows and len(valid_records) > 1:
                        midpoint = len(valid_records) // 2
                        return (
                            load_source_subset(valid_records[:midpoint], f"{subset_tag}_a")
                            + load_source_subset(valid_records[midpoint:], f"{subset_tag}_b")
                        )
                    if run_id and quarantine_invalid_rows and len(valid_records) == 1:
                        _record_quarantine_on_connection(
                            snowflake_connection, database_name, schema_name, run_id,
                            table_name, source_table, batch_number,
                            valid_records[0], exc, target_columns,
                        )
                        logger.error(
                            "Quarantined invalid row for %s: %s",
                            table_name,
                            exc,
                        )
                        return 0
                    raise

            while True:
                try:
                    rows = source_cursor.fetchmany(batch_size)
                except Exception as fetch_error:
                    logger.error(
                        "Source batch fetch failed for %s; retrying row-by-row: %s",
                        table_name,
                        fetch_error,
                    )
                    fallback_rows: list[Sequence[Any]] = []
                    row_failures = 0
                    while True:
                        try:
                            rows_one = source_cursor.fetchmany(1)
                        except Exception as row_error:
                            row_failures += 1
                            if run_id and quarantine_invalid_rows:
                                _record_quarantine_on_connection(
                                    snowflake_connection,
                                    database_name,
                                    schema_name,
                                    run_id,
                                    table_name,
                                    source_table,
                                    batch_number + 1,
                                    {"SOURCE_FETCH_POSITION": row_failures},
                                    row_error,
                                    target_columns,
                                )
                            if row_failures >= batch_size:
                                break
                            continue
                        if not rows_one:
                            break
                        fallback_rows.extend(rows_one)
                        if len(fallback_rows) >= batch_size:
                            break
                    if not fallback_rows and not row_failures:
                        raise
                    rows = fallback_rows
                    if not rows:
                        continue

                if not rows:
                    break
                batch_number += 1
                loaded_rows = load_source_subset(rows, f"{batch_number}")
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
        shutil.rmtree(staging_temp_dir, ignore_errors=True)
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
