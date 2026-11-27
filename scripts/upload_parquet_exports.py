from __future__ import annotations

import argparse
import logging
import shutil
import sys
import time
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from config.settings import settings
from connections.snowflake_connection import (
    close_snowflake_connection,
    connect_to_snowflake,
)
from etl.incremental import acquire_table_run_lock, release_table_run_lock
from etl.load import _local_file_uri
from etl.parquet_snowflake import _qualified_name, _quote_identifier, _safe_stage_component
from etl.table_status import TableStatusStore

logger = logging.getLogger(__name__)


def _parquet_info(path: Path) -> tuple[str, str, list[str], int]:
    parquet = pq.ParquetFile(path)
    try:
        schema = parquet.schema_arrow
        metadata = schema.metadata or {}
        target = metadata.get(b"target_table", b"").decode("utf-8").strip()
        batch_id = metadata.get(b"run_id", b"").decode("utf-8").strip()
        if not target:
            raise ValueError(f"Parquet file has no target_table metadata: {path}")
        if not batch_id:
            raise ValueError(f"Parquet file has no run_id metadata: {path}")
        if _safe_stage_component(target, "Target table") != target:
            raise ValueError(f"Unsupported target table name in {path}: {target!r}")
        if len(target) > 255:
            raise ValueError(f"Target table name is too long: {target!r}")
        columns = schema.names
        if not columns:
            raise ValueError(f"Parquet file has no columns: {path}")
        if any(not name or name != name.strip() or "\x00" in name for name in columns):
            raise ValueError(f"Parquet file has an invalid column name: {path}")
        if len({name.casefold() for name in columns}) != len(columns):
            raise ValueError(f"Parquet file has case-insensitive duplicate columns: {path}")
        unsupported = [
            field.name
            for field in schema
            if not (pa.types.is_string(field.type) or pa.types.is_large_string(field.type))
        ]
        if unsupported:
            raise ValueError(
                f"Parquet columns must contain text for VARCHAR upload: {unsupported}"
            )
        return batch_id, target, columns, parquet.metadata.num_rows
    finally:
        parquet.close()


def _upload_file(
    connection,
    database: str,
    schema: str,
    path: Path,
    target: str,
    columns: list[str],
    expected_rows: int,
) -> int:
    cursor = connection.cursor()
    token = uuid4().hex.upper()
    stage = _qualified_name(database, schema, f"ETL_UPLOAD_STAGE_{token}")
    staging_table = _qualified_name(database, schema, f"ETL_UPLOAD_TMP_{token}")
    target_table = _qualified_name(database, schema, target)
    lock_name = f"{_safe_stage_component(target, 'Target table')[:230]}_PARQUET_RAW"
    lock = None
    stage_created = False
    table_created = False
    try:
        lock = acquire_table_run_lock(
            cursor, connection, database, schema, lock_name, [], reuse_existing_strategy=True
        )
        definitions = ", ".join(f"{_quote_identifier(name)} VARCHAR" for name in columns)
        column_sql = ", ".join(_quote_identifier(name) for name in columns)
        cursor.execute(f"CREATE TEMPORARY STAGE {stage}")
        stage_created = True
        cursor.execute(f"CREATE TEMPORARY TABLE {staging_table} ({definitions})")
        table_created = True

        source_uri = _local_file_uri(path).replace("'", "''")
        stage_file_name = path.name.replace("'", "''")
        cursor.execute(f"PUT '{source_uri}' @{stage} AUTO_COMPRESS=FALSE OVERWRITE=FALSE")
        put_columns = [str(item[0]).casefold() for item in (cursor.description or [])]
        put_rows = cursor.fetchall()
        if "status" not in put_columns or not put_rows:
            raise RuntimeError("Snowflake PUT returned no upload status")
        status_index = put_columns.index("status")
        if any(str(row[status_index]).upper() != "UPLOADED" for row in put_rows):
            raise RuntimeError(f"Snowflake PUT did not upload {path.name}: {put_rows}")

        cursor.execute(
            f"COPY INTO {staging_table} FROM @{stage} "
            f"FILES = ('{stage_file_name}') FILE_FORMAT=(TYPE=PARQUET NULL_IF=()) "
            "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"
        )
        copy_columns = [str(item[0]).casefold() for item in (cursor.description or [])]
        copy_rows = cursor.fetchall()
        if "rows_loaded" not in copy_columns or "errors_seen" not in copy_columns or not copy_rows:
            raise RuntimeError("Snowflake COPY returned incomplete load status")
        loaded_index = copy_columns.index("rows_loaded")
        errors_index = copy_columns.index("errors_seen")
        loaded_rows = sum(int(row[loaded_index] or 0) for row in copy_rows)
        errors = sum(int(row[errors_index] or 0) for row in copy_rows)
        if errors or loaded_rows != expected_rows:
            raise RuntimeError(
                f"Snowflake COPY loaded {loaded_rows} of {expected_rows} rows "
                f"with {errors} error(s)"
            )
        cursor.execute(
            f"CREATE OR REPLACE TABLE {target_table} COPY GRANTS "
            f"AS SELECT {column_sql} FROM {staging_table}"
        )
        return loaded_rows
    finally:
        had_error = sys.exc_info()[0] is not None
        cleanup_failed = False
        if table_created:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
            except Exception:
                logger.exception("Could not remove temporary table %s", staging_table)
                cleanup_failed = True
        if stage_created:
            try:
                cursor.execute(f"DROP STAGE IF EXISTS {stage}")
            except Exception:
                logger.exception("Could not remove temporary stage %s", stage)
                cleanup_failed = True
        if lock is not None:
            try:
                release_table_run_lock(cursor, connection, lock, lock_name)
            except Exception:
                logger.exception("Could not release upload lock for %s", target)
                cleanup_failed = True
        cursor.close()
        if cleanup_failed and not had_error:
            raise RuntimeError(
                f"Upload cleanup failed for {target}; the target table may already be replaced"
            )


def _archive_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as input_file:
        output_file = destination.open("xb")
        try:
            with output_file:
                shutil.copyfileobj(input_file, output_file)
        except Exception:
            try:
                destination.unlink()
            except OSError:
                logger.exception("Could not remove incomplete archive file %s", destination)
            raise
    source.unlink()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Replace Snowflake target tables from Parquet files without the mapping "
            "workbook, then archive successfully uploaded files."
        )
    )
    parser.add_argument("--export-dir", type=Path, default=PROJECT_ROOT / "exports")
    parser.add_argument("--archive-dir", type=Path, default=PROJECT_ROOT / "uploaded_exports")
    parser.add_argument(
        "--batch-id",
        help="Batch run ID; inferred only when exactly one batch is eligible",
    )
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=2,
        help="Additional upload attempts for failed tables (default: 2)",
    )
    parser.add_argument("--database", default=settings.snowflake_database)
    parser.add_argument("--schema", default=settings.snowflake_schema)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None, *, connection=None) -> int:
    owns_connection = connection is None
    if owns_connection:
        configure_logging()
    args = _parse_args(argv)
    exit_code = 0
    try:
        export_dir = args.export_dir.resolve()
        archive_dir = args.archive_dir.resolve()
        if args.retry_attempts < 0:
            raise ValueError("--retry-attempts cannot be negative")
        if not export_dir.is_dir():
            raise FileNotFoundError(f"Export directory not found: {export_dir}")
        if archive_dir == export_dir or export_dir in archive_dir.parents:
            raise ValueError("Archive directory must be outside the export directory")
        if not args.database.strip() or not args.schema.strip():
            raise ValueError("Snowflake database and schema are required")

        if owns_connection:
            connection = connect_to_snowflake()
        status_store = TableStatusStore(connection, args.database, args.schema)
        status_store.ensure_table()
        eligible_batches = status_store.eligible_batches()
        batch_id = args.batch_id
        if batch_id is None:
            if not eligible_batches:
                print("No READY or upload-failed table statuses are available")
                return 0
            if len(eligible_batches) > 1:
                raise ValueError(
                    "Multiple eligible batches exist; specify one with --batch-id: "
                    + ", ".join(eligible_batches)
                )
            batch_id = eligible_batches[0]

        selected = status_store.eligible_rows(batch_id)
        if not selected:
            print(f"No eligible tables found for batch {batch_id}")
            return 0
        print(f"Uploading batch {batch_id}: {len(selected)} eligible table(s)")

        for row in selected:
            if not status_store.begin_upload(batch_id, row.table_name):
                logger.info(
                    "Skipping %s in batch %s; it is no longer eligible",
                    row.table_name,
                    batch_id,
                )
                continue

            try:
                if not row.parquet_path:
                    raise ValueError(
                        f"Status row has no Parquet path for {row.table_name}"
                    )
                relative_path = Path(row.parquet_path)
                if relative_path.is_absolute() or ".." in relative_path.parts:
                    raise ValueError(
                        f"Unsafe Parquet path in status row for {row.table_name}: "
                        f"{row.parquet_path!r}"
                    )
                path = (export_dir / relative_path).resolve()
                if path == export_dir or export_dir not in path.parents:
                    raise ValueError(
                        f"Parquet path resolves outside export directory: {path}"
                    )
                if not path.is_file():
                    raise FileNotFoundError(f"Parquet file not found: {path}")
                file_batch, target, columns, row_count = _parquet_info(path)
                if file_batch != batch_id:
                    raise ValueError(
                        f"Parquet batch ID mismatch for {path.name}: "
                        f"expected {batch_id!r}, found {file_batch!r}"
                    )
                if target.casefold() != row.table_name.casefold():
                    raise ValueError(
                        f"Parquet target mismatch for {path.name}: "
                        f"expected {row.table_name!r}, found {target!r}"
                    )
                if row.expected_rows is None or row_count != row.expected_rows:
                    raise ValueError(
                        f"Parquet row count mismatch for {row.table_name}: "
                        f"status={row.expected_rows}, file={row_count}"
                    )
                destination = archive_dir / relative_path
                if destination.exists():
                    raise FileExistsError(f"Archive file already exists: {destination}")
            except Exception as exc:
                status_store.mark_upload_failed(batch_id, row.table_name, str(exc))
                logger.exception(
                    "Parquet pre-upload validation failed for %s in batch %s",
                    row.table_name,
                    batch_id,
                )
                exit_code = 1
                continue

            uploaded = False
            for attempt in range(args.retry_attempts + 1):
                if attempt:
                    if not status_store.begin_upload(batch_id, row.table_name):
                        logger.error(
                            "Could not restart upload for %s in batch %s",
                            row.table_name,
                            batch_id,
                        )
                        exit_code = 1
                        break
                try:
                    loaded = _upload_file(
                        connection,
                        args.database,
                        args.schema,
                        path,
                        target,
                        columns,
                        row_count,
                    )
                except Exception as exc:
                    status_store.mark_upload_failed(
                        batch_id, row.table_name, str(exc)
                    )
                    logger.exception(
                        "Upload attempt %d failed for %s in batch %s",
                        attempt + 1,
                        row.table_name,
                        batch_id,
                    )
                    if attempt < args.retry_attempts:
                        time.sleep(min(2 ** attempt, 8))
                        continue
                    exit_code = 1
                    break

                try:
                    status_store.mark_uploaded(batch_id, row.table_name, loaded)
                except Exception as exc:
                    logger.exception(
                        "Snowflake load succeeded but UPLOADED status failed for %s "
                        "in batch %s",
                        row.table_name,
                        batch_id,
                    )
                    try:
                        status_store.mark_upload_failed(
                            batch_id,
                            row.table_name,
                            f"Snowflake load succeeded but status update failed: {exc}",
                        )
                    except Exception:
                        logger.exception(
                            "Could not record upload-status failure for %s in batch %s",
                            row.table_name,
                            batch_id,
                        )
                    exit_code = 1
                    break
                try:
                    _archive_file(path, destination)
                    status_store.record_archive(
                        batch_id, row.table_name, destination.relative_to(archive_dir)
                    )
                except Exception:
                    logger.exception(
                        "Upload succeeded but archive failed for %s in batch %s",
                        row.table_name,
                        batch_id,
                    )
                    exit_code = 1
                else:
                    print(
                        f"Uploaded {path} to {target}: {loaded} rows; "
                        f"moved to {destination}"
                    )
                uploaded = True
                break

            if not uploaded:
                continue
    except Exception:
        logger.exception("Parquet upload failed")
        exit_code = 1
    finally:
        if owns_connection and connection is not None:
            try:
                close_snowflake_connection(connection)
            except Exception:
                logger.exception("Could not close the Snowflake connection cleanly")
                exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
