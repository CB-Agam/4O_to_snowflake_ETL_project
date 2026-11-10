from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

import pyarrow.parquet as pq

from etl.incremental import acquire_table_run_lock, release_table_run_lock
from etl.load import _local_file_uri

logger = logging.getLogger(__name__)
_RAW_METADATA_COLUMNS = {
    "ETL_RUN_ID",
    "ETL_SOURCE_FILE",
    "ETL_LOADED_AT",
}


@dataclass(frozen=True)
class ParquetLoadResult:
    run_id: str
    target_table: str
    raw_table: str
    file_path: Path
    file_sha256: str
    file_size_bytes: int
    parquet_rows: int
    loaded_rows: int
    skipped_as_duplicate: bool


def _quote_identifier(identifier: str) -> str:
    value = str(identifier).strip()
    if not value or "\x00" in value:
        raise ValueError(f"Invalid Snowflake identifier: {identifier!r}")
    return '"' + value.replace('"', '""') + '"'


def _qualified_name(database: str, schema: str, object_name: str) -> str:
    return ".".join(
        _quote_identifier(part) for part in (database, schema, object_name)
    )


def _safe_stage_component(value: str, label: str) -> str:
    component = re.sub(r"[^A-Za-z0-9_-]+", "_", value.strip()).strip("_")
    if not component:
        raise ValueError(f"{label} must contain at least one alphanumeric character")
    return component


def _mapped_target_columns(mapping_rows: Sequence[dict[str, str]]) -> tuple[str, list[str]]:
    source_rows = [
        row
        for row in mapping_rows
        if row.get("source_table", "").strip()
        and row.get("source_column", "").strip()
        and row.get("target_table", "").strip()
        and row.get("target_column", "").strip()
    ]
    if not source_rows:
        raise ValueError("The selected table has no complete source-to-target column mappings")
    target_tables = {row["target_table"].strip() for row in source_rows}
    if len(target_tables) != 1:
        raise ValueError("A Parquet load requires exactly one mapped Snowflake target table")
    target_columns = [row["target_column"].strip() for row in source_rows]
    normalized_columns = [column.upper() for column in target_columns]
    if len(set(normalized_columns)) != len(normalized_columns):
        raise ValueError("The mapping contains duplicate target columns")
    collisions = _RAW_METADATA_COLUMNS.intersection(normalized_columns)
    if collisions:
        raise ValueError(
            "Mapped columns collide with raw-load metadata columns: "
            + ", ".join(sorted(collisions))
        )
    return next(iter(target_tables)), target_columns


def _file_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_parquet_file(
    file_path: Path,
    target_columns: Sequence[str],
    run_id: str,
    target_table: str,
) -> int:
    if not file_path.is_file():
        raise FileNotFoundError(f"Parquet file does not exist: {file_path}")
    with file_path.open("rb") as source:
        metadata = pq.read_metadata(source)
    parquet_columns = [str(column).casefold() for column in metadata.schema.names]
    expected_columns = [column.casefold() for column in target_columns]
    if parquet_columns != expected_columns:
        raise ValueError(
            f"Parquet columns do not match the mapping for {target_table}: "
            f"expected {expected_columns}, found {parquet_columns}"
        )
    key_values = {
        key.decode("utf-8"): value.decode("utf-8")
        for key, value in (metadata.metadata or {}).items()
    }
    if key_values.get("run_id") != run_id:
        raise ValueError(
            f"Parquet run ID mismatch: expected {run_id!r}, "
            f"found {key_values.get('run_id')!r}"
        )
    if key_values.get("target_table", "").casefold() != target_table.casefold():
        raise ValueError(
            f"Parquet target table mismatch: expected {target_table!r}, "
            f"found {key_values.get('target_table')!r}"
        )
    return int(metadata.num_rows)


def _ensure_raw_target(
    cursor: Any,
    database: str,
    schema: str,
    raw_table: str,
    target_columns: Sequence[str],
) -> str:
    target = _qualified_name(database, schema, raw_table)
    definitions = [
        f"{_quote_identifier(column)} VARCHAR"
        for column in target_columns
    ]
    definitions.extend(
        [
            f"{_quote_identifier('ETL_RUN_ID')} VARCHAR(36) NOT NULL",
            f"{_quote_identifier('ETL_SOURCE_FILE')} VARCHAR NOT NULL",
            f"{_quote_identifier('ETL_LOADED_AT')} TIMESTAMP_NTZ "
            "DEFAULT CURRENT_TIMESTAMP()",
        ]
    )
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {target} ({', '.join(definitions)})"
    )
    cursor.execute(f"DESC TABLE {target}")
    descriptions = cursor.fetchall()
    existing_types = {
        str(row[0]).casefold(): str(row[1]).upper().split("(", 1)[0]
        for row in descriptions
        if len(row) > 1
    }
    required_types = {
        **{column.casefold(): "VARCHAR" for column in target_columns},
        "etl_run_id": "VARCHAR",
        "etl_source_file": "VARCHAR",
        "etl_loaded_at": "TIMESTAMP_NTZ",
    }
    mismatches = {
        column: (required, existing_types.get(column))
        for column, required in required_types.items()
        if existing_types.get(column) != required
    }
    if mismatches:
        raise RuntimeError(
            f"Raw table {raw_table} has incompatible columns; expected "
            f"VARCHAR landing fields and ETL metadata types, found {mismatches}"
        )
    return target


def _ensure_file_ledger(cursor: Any, database: str, schema: str) -> str:
    ledger = _qualified_name(database, schema, "ETL_PARQUET_FILE_LOADS")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ledger} (
            RUN_ID VARCHAR(36) NOT NULL,
            TABLE_NAME VARCHAR(255) NOT NULL,
            FILE_NAME VARCHAR(1024) NOT NULL,
            FILE_SHA256 VARCHAR(64) NOT NULL,
            FILE_SIZE_BYTES NUMBER(38, 0) NOT NULL,
            PARQUET_ROWS NUMBER(38, 0) NOT NULL,
            STATUS VARCHAR(20) NOT NULL,
            LOADED_ROWS NUMBER(38, 0),
            START_TIME TIMESTAMP_NTZ NOT NULL,
            END_TIME TIMESTAMP_NTZ,
            ERROR_MESSAGE VARCHAR
        )
        """
    )
    return ledger


def _start_file_ledger(
    cursor: Any,
    connection: Any,
    ledger: str,
    run_id: str,
    target_table: str,
    file_path: Path,
    digest: str,
    size_bytes: int,
    parquet_rows: int,
) -> int | None:
    existing = cursor.execute(
        f"""
        SELECT STATUS, LOADED_ROWS
        FROM {ledger}
        WHERE TABLE_NAME = %s AND FILE_SHA256 = %s
        """,
        (target_table, digest),
    ).fetchall()
    if len(existing) > 1:
        raise RuntimeError(
            f"File ledger has duplicate records for {target_table} and SHA256 {digest}"
        )
    if existing and str(existing[0][0]).upper() == "SUCCESS":
        return int(existing[0][1] or 0)
    if existing:
        cursor.execute(
            f"""
            UPDATE {ledger}
            SET RUN_ID = %s, FILE_NAME = %s, FILE_SIZE_BYTES = %s,
                PARQUET_ROWS = %s, STATUS = 'RUNNING', LOADED_ROWS = NULL,
                START_TIME = CURRENT_TIMESTAMP(), END_TIME = NULL, ERROR_MESSAGE = NULL
            WHERE TABLE_NAME = %s AND FILE_SHA256 = %s
            """,
            (
                run_id,
                str(file_path),
                size_bytes,
                parquet_rows,
                target_table,
                digest,
            ),
        )
    else:
        cursor.execute(
            f"""
            INSERT INTO {ledger}
                (RUN_ID, TABLE_NAME, FILE_NAME, FILE_SHA256, FILE_SIZE_BYTES,
                 PARQUET_ROWS, STATUS, START_TIME)
            VALUES (%s, %s, %s, %s, %s, %s, 'RUNNING', CURRENT_TIMESTAMP())
            """,
            (
                run_id,
                target_table,
                str(file_path),
                digest,
                size_bytes,
                parquet_rows,
            ),
        )
    connection.commit()
    return None


def _mark_file_failed(
    cursor: Any,
    connection: Any,
    ledger: str,
    target_table: str,
    digest: str,
    error: BaseException,
) -> None:
    cursor.execute(
        f"""
        UPDATE {ledger}
        SET STATUS = 'FAILED', END_TIME = CURRENT_TIMESTAMP(), ERROR_MESSAGE = %s
        WHERE TABLE_NAME = %s AND FILE_SHA256 = %s
        """,
        (str(error), target_table, digest),
    )
    connection.commit()


def load_parquet_to_raw_table(
    snowflake_connection: Any,
    database: str,
    schema: str,
    mapping_rows: Sequence[dict[str, str]],
    parquet_path: str | Path,
    run_id: str,
) -> ParquetLoadResult:
    file_path = Path(parquet_path).resolve()
    target_table, target_columns = _mapped_target_columns(mapping_rows)
    run_component = _safe_stage_component(run_id, "Run ID")
    target_component = _safe_stage_component(target_table, "Target table")
    parquet_rows = _validate_parquet_file(
        file_path,
        target_columns,
        run_component,
        target_table,
    )
    file_size = file_path.stat().st_size
    digest = _file_sha256(file_path)
    raw_table = f"{target_table}_RAW"
    if len(raw_table) > 255:
        raise ValueError(f"Raw table name exceeds Snowflake identifier limit: {raw_table}")
    stage_name = "ETL_PARQUET_STAGE"
    stage = _qualified_name(database, schema, stage_name)

    cursor = snowflake_connection.cursor()
    lock: dict[str, str] | None = None
    stage_table: str | None = None
    started_ledger = False
    try:
        ledger = _ensure_file_ledger(cursor, database, schema)
        lock_table_name = f"{target_component[:230]}_PARQUET_RAW"
        lock = acquire_table_run_lock(
            cursor,
            snowflake_connection,
            database,
            schema,
            lock_table_name,
            mapping_rows,
        )
        skipped_rows = _start_file_ledger(
            cursor,
            snowflake_connection,
            ledger,
            run_component,
            target_table,
            file_path,
            digest,
            file_size,
            parquet_rows,
        )
        if skipped_rows is not None:
            return ParquetLoadResult(
                run_id=run_component,
                target_table=target_table,
                raw_table=raw_table,
                file_path=file_path,
                file_sha256=digest,
                file_size_bytes=file_size,
                parquet_rows=parquet_rows,
                loaded_rows=skipped_rows,
                skipped_as_duplicate=True,
            )
        started_ledger = True

        raw_target = _ensure_raw_target(
            cursor,
            database,
            schema,
            raw_table,
            target_columns,
        )
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage}")
        stage_table_name = f"ETL_PARQUET_LOAD_{uuid4().hex[:16].upper()}"
        stage_table = _qualified_name(database, schema, stage_table_name)
        column_sql = ", ".join(_quote_identifier(column) for column in target_columns)
        definitions = ", ".join(
            f"{_quote_identifier(column)} VARCHAR" for column in target_columns
        )
        cursor.execute(f"CREATE TEMPORARY TABLE {stage_table} ({definitions})")

        stage_directory = f"{run_component}/{target_component}/{digest[:16]}"
        stage_file_name = file_path.name.replace("'", "''")
        stage_location = f"@{stage}/{stage_directory}"
        source_uri = _local_file_uri(file_path).replace("'", "''")
        cursor.execute(
            f"PUT '{source_uri}' {stage_location} "
            "AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
        )
        put_columns = [
            str(description[0]).casefold()
            for description in (cursor.description or [])
        ]
        put_rows = cursor.fetchall()
        if "status" not in put_columns or not put_rows:
            raise RuntimeError("Snowflake PUT returned no upload status")
        status_index = put_columns.index("status")
        message_index = put_columns.index("message") if "message" in put_columns else None
        for row in put_rows:
            status = str(row[status_index]).upper()
            if status not in {"UPLOADED", "SKIPPED"}:
                message = row[message_index] if message_index is not None else status
                raise RuntimeError(f"Snowflake stage upload failed: {message}")

        cursor.execute(
            f"COPY INTO {stage_table} FROM {stage_location} "
            f"FILES = ('{stage_file_name}') "
            "FILE_FORMAT=(TYPE=PARQUET) "
            "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"
        )
        copy_columns = [
            str(description[0]).casefold()
            for description in (cursor.description or [])
        ]
        copy_rows = cursor.fetchall()
        if "rows_loaded" not in copy_columns or not copy_rows:
            raise RuntimeError("Snowflake COPY INTO returned no row-load status")
        loaded_index = copy_columns.index("rows_loaded")
        errors_index = copy_columns.index("errors_seen") if "errors_seen" in copy_columns else None
        copy_loaded_rows = sum(int(row[loaded_index] or 0) for row in copy_rows)
        copy_errors = (
            sum(int(row[errors_index] or 0) for row in copy_rows)
            if errors_index is not None
            else 0
        )
        if copy_errors:
            raise RuntimeError(f"Snowflake Parquet COPY reported {copy_errors} error(s)")
        if copy_loaded_rows != parquet_rows:
            raise RuntimeError(
                f"Snowflake loaded {copy_loaded_rows} of {parquet_rows} Parquet rows"
            )

        insert_columns = [
            *[_quote_identifier(column) for column in target_columns],
            _quote_identifier("ETL_RUN_ID"),
            _quote_identifier("ETL_SOURCE_FILE"),
            _quote_identifier("ETL_LOADED_AT"),
        ]
        cursor.execute("BEGIN")
        try:
            cursor.execute(
                f"INSERT INTO {raw_target} ({', '.join(insert_columns)}) "
                f"SELECT {column_sql}, %s, %s, CURRENT_TIMESTAMP() "
                f"FROM {stage_table}",
                (run_component, file_path.name),
            )
            inserted_count = cursor.execute(
                f"SELECT COUNT(*) FROM {raw_target} "
                f"WHERE {_quote_identifier('ETL_RUN_ID')} = %s "
                f"AND {_quote_identifier('ETL_SOURCE_FILE')} = %s",
                (run_component, file_path.name),
            ).fetchone()
            if not inserted_count or int(inserted_count[0]) != copy_loaded_rows:
                found = int(inserted_count[0]) if inserted_count else 0
                raise RuntimeError(
                    f"Raw table insert count mismatch for {raw_table}: "
                    f"expected {copy_loaded_rows}, found {found}"
                )
            cursor.execute(
                f"""
                UPDATE {ledger}
                SET STATUS = 'SUCCESS', LOADED_ROWS = %s,
                    END_TIME = CURRENT_TIMESTAMP(), ERROR_MESSAGE = NULL
                WHERE TABLE_NAME = %s AND FILE_SHA256 = %s
                """,
                (copy_loaded_rows, target_table, digest),
            )
            snowflake_connection.commit()
        except Exception:
            snowflake_connection.rollback()
            raise

        try:
            cursor.execute(f"REMOVE {stage_location}/{stage_file_name}")
        except Exception:
            logger.exception(
                "Parquet load succeeded, but staged-file cleanup failed: %s",
                stage_file_name,
            )
        logger.info(
            "Loaded Parquet file into %s: run=%s rows=%s sha256=%s",
            raw_table,
            run_component,
            copy_loaded_rows,
            digest,
        )
        return ParquetLoadResult(
            run_id=run_component,
            target_table=target_table,
            raw_table=raw_table,
            file_path=file_path,
            file_sha256=digest,
            file_size_bytes=file_size,
            parquet_rows=parquet_rows,
            loaded_rows=copy_loaded_rows,
            skipped_as_duplicate=False,
        )
    except Exception as exc:
        if started_ledger:
            try:
                _mark_file_failed(
                    cursor,
                    snowflake_connection,
                    ledger,
                    target_table,
                    digest,
                    exc,
                )
            except Exception:
                logger.exception("Could not mark Parquet load as failed for %s", target_table)
        raise
    finally:
        if stage_table is not None:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {stage_table}")
            except Exception:
                logger.exception("Could not remove temporary Parquet table for %s", target_table)
        if lock is not None:
            try:
                release_table_run_lock(
                    cursor,
                    snowflake_connection,
                    lock,
                    f"{_safe_stage_component(target_table, 'Target table')[:230]}_PARQUET_RAW",
                )
            except Exception:
                logger.exception("Could not release Parquet load lock for %s", target_table)
        cursor.close()
