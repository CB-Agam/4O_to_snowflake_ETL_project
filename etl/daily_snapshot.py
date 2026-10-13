from __future__ import annotations

import logging
import re
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Sequence
from uuid import UUID

import simplejson as json

from etl.load import build_source_query, create_missing_tables_from_metadata, table_exists_in_snowflake
from etl.sftp_snowpipe import (
    create_batch_audit_table,
    finish_batch_audit,
    snowpipe_object_names,
    start_batch_audit,
    transform_json_file,
    _local_file_uri,
)

logger = logging.getLogger(__name__)
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _quote_identifier(identifier: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"Unsupported Snowflake identifier: {identifier!r}")
    return f'"{identifier}"'


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"Value of type {type(value).__name__} cannot be written to the 4D JSON export")


def extract_4d_table_to_json(
    fourd_connection,
    metadata_rows: Sequence[dict[str, str]],
    export_path: Path,
    fetch_size: int = 1000,
) -> int:
    source_rows = [
        row
        for row in metadata_rows
        if row["source_table"].strip()
        and row["source_column"].strip()
        and row["target_column"].strip()
    ]
    if not source_rows:
        raise ValueError("The selected table has no complete source-to-target column mappings")
    if fetch_size <= 0:
        raise ValueError("fetch_size must be greater than zero")

    source_tables = {row["source_table"].strip() for row in source_rows}
    if len(source_tables) != 1:
        raise ValueError("A daily JSON export requires all mapped columns to come from one 4D source table")

    source_columns = list(dict.fromkeys(row["source_column"].strip() for row in source_rows))
    query = build_source_query(next(iter(source_tables)), source_columns)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = export_path.with_name(f"{export_path.name}.partial")
    cursor = fourd_connection.cursor()
    record_count = 0
    try:
        cursor.execute(query)
        with partial_path.open("w", encoding="utf-8", newline="") as output:
            output.write("[")
            while True:
                batch = cursor.fetchmany(fetch_size)
                if not batch:
                    break
                for source_record in batch:
                    record = dict(zip(source_columns, source_record, strict=True))
                    if record_count:
                        output.write(",")
                    json.dump(
                        record,
                        output,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        allow_nan=False,
                        default=_json_default,
                    )
                    record_count += 1
            output.write("]")
            output.flush()
        partial_path.replace(export_path)
    except Exception:
        partial_path.unlink(missing_ok=True)
        raise
    finally:
        cursor.close()
    return record_count


def _snowflake_copy_rows(cursor, destination: str, stage: str, filename: str) -> int:
    cursor.execute(
        f"COPY INTO {destination} FROM @{stage} "
        f"FILES = ('{filename}') "
        "FILE_FORMAT=(TYPE='JSON' STRIP_OUTER_ARRAY=TRUE) "
        "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"
    )
    descriptions = cursor.description or []
    columns = [str(description[0]).casefold() for description in descriptions]
    result_rows = cursor.fetchall()
    if not columns or not result_rows:
        raise RuntimeError("Snowflake COPY INTO returned no load status")

    loaded_index = columns.index("rows_loaded") if "rows_loaded" in columns else None
    errors_index = columns.index("errors_seen") if "errors_seen" in columns else None
    if loaded_index is None:
        raise RuntimeError(f"Snowflake COPY INTO response omitted rows_loaded: {columns}")

    total_loaded = 0
    errors = 0
    for row in result_rows:
        total_loaded += int(row[loaded_index] or 0)
        if errors_index is not None:
            errors += int(row[errors_index] or 0)
    if errors:
        raise RuntimeError(f"Snowflake COPY INTO reported {errors} load error(s)")
    return total_loaded


def publish_json_snapshot(
    snowflake_connection,
    database: str,
    schema: str,
    target_table: str,
    metadata_rows: Sequence[dict[str, str]],
    json_path: Path,
    batch_id: str,
    expected_rows: int,
) -> dict[str, Any]:
    objects = snowpipe_object_names(database, schema, target_table)
    target_name = objects["table"]
    stage_name = objects["stage"]
    audit_table = objects["audit_table"]
    batch_tag = re.sub(r"[^A-Za-z0-9]", "", batch_id).upper()[:12]
    staging_table_name = f"ETL_LOAD_{batch_tag}"
    staging_table = (
        f"{_quote_identifier(database)}.{_quote_identifier(schema)}."
        f"{_quote_identifier(staging_table_name)}"
    )
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    history_table_name = f"{target_table[:180]}_HS_{timestamp}_{batch_tag}"
    history_table = (
        f"{_quote_identifier(database)}.{_quote_identifier(schema)}."
        f"{_quote_identifier(history_table_name)}"
    )
    staged_filename = f"{batch_tag}.json"
    transformed_path = json_path.with_name(staged_filename)

    loaded_rows: int | None = None
    staging_created = False
    audit_started = False
    cursor = None
    try:
        cursor = snowflake_connection.cursor()
        create_batch_audit_table(cursor, audit_table)
        start_batch_audit(cursor, audit_table, batch_id, target_table, "4D_ODBC", 1)
        audit_started = True

        had_current_table = table_exists_in_snowflake(cursor, schema, target_table)
        create_missing_tables_from_metadata(
            cursor,
            {target_table: list(metadata_rows)},
            schema,
        )
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
        cursor.execute(f"CREATE TABLE {staging_table} LIKE {target_name}")
        staging_created = True

        transformed_rows = transform_json_file(json_path, transformed_path, metadata_rows)
        if transformed_rows != expected_rows:
            raise RuntimeError(
                f"JSON mapping produced {transformed_rows} row(s), but 4D extraction produced {expected_rows}"
            )

        source_uri = _local_file_uri(transformed_path).replace("'", "''")
        cursor.execute(
            f"PUT '{source_uri}' @{stage_name} "
            "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        put_columns = [str(description[0]).casefold() for description in (cursor.description or [])]
        put_rows = cursor.fetchall()
        if "status" in put_columns:
            status_index = put_columns.index("status")
            message_index = put_columns.index("message") if "message" in put_columns else None
            for row in put_rows:
                status = str(row[status_index]).upper()
                if status not in {"UPLOADED", "SKIPPED"}:
                    message = row[message_index] if message_index is not None else status
                    raise RuntimeError(f"Snowflake stage upload failed: {message}")
        elif not put_rows:
            raise RuntimeError("Snowflake PUT returned no upload status")

        loaded_rows = _snowflake_copy_rows(cursor, staging_table, stage_name, staged_filename)
        if loaded_rows != expected_rows:
            raise RuntimeError(
                f"Snowflake loaded {loaded_rows} row(s), but 4D exported {expected_rows}"
            )

        history_name: str | None = None
        if had_current_table:
            cursor.execute(f"CREATE TABLE {history_table} CLONE {target_name}")
            history_name = history_table_name
        cursor.execute(f"CREATE OR REPLACE TABLE {target_name} CLONE {staging_table}")

        try:
            cursor.execute(f"REMOVE @{stage_name}/{staged_filename}")
        except Exception:
            logger.exception("Snapshot loaded, but staged file cleanup failed for %s", staged_filename)
        finish_batch_audit(
            cursor,
            audit_table,
            batch_id,
            "COMPLETED",
            1,
            loaded_rows,
        )
        logger.info(
            "Completed batch %s for %s: %s rows loaded; history table: %s",
            batch_id,
            target_table,
            loaded_rows,
            history_name or "none (first snapshot)",
        )
        return {
            "batch_id": batch_id,
            "rows_loaded": loaded_rows,
            "history_table": history_name,
            "json_file": str(json_path),
        }
    except Exception as exc:
        if audit_started and cursor is not None:
            try:
                finish_batch_audit(
                    cursor,
                    audit_table,
                    batch_id,
                    "FAILED",
                    0,
                    loaded_rows or 0,
                    str(exc)[:4000],
                )
            except Exception as audit_error:
                raise RuntimeError(
                    f"Snapshot batch {batch_id} failed ({exc}); audit update also failed: {audit_error}"
                ) from exc
        raise
    finally:
        if cursor is not None:
            if staging_created and loaded_rows is not None:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
                except Exception:
                    logger.exception("Could not remove temporary staging table %s", staging_table_name)
            cursor.close()
        transformed_path.unlink(missing_ok=True)
