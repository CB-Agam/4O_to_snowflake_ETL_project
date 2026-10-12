from __future__ import annotations

import hashlib
import logging
import re
import stat
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Iterator, Sequence

import ijson
import simplejson as json
from snowflake.ingest import SimpleIngestManager, StagedFile
from cryptography.hazmat.primitives import serialization

from config.settings import settings

logger = logging.getLogger(__name__)
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _quote_identifier(identifier: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"Unsupported Snowflake identifier: {identifier!r}")
    return f'"{identifier}"'


def _normalize_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", value).casefold()


def find_table_json_files(sftp_client, remote_dir: str, table_names: Iterable[str]) -> list[tuple[str, str]]:
    normalized_names = [_normalize_filename(name) for name in table_names if name]
    matches = []
    for item in sftp_client.listdir_attr(remote_dir):
        filename = item.filename
        if not filename.lower().endswith(".json"):
            continue
        if isinstance(item.st_mode, int) and not stat.S_ISREG(item.st_mode):
            continue
        stem = Path(filename).stem
        normalized_stem = _normalize_filename(stem)
        if any(normalized_stem.startswith(name) for name in normalized_names):
            matches.append((filename, str(PurePosixPath(remote_dir) / filename)))
    return sorted(matches)


def find_local_table_json_files(export_dir: Path, table_names: Iterable[str]) -> list[tuple[str, str]]:
    export_dir = export_dir.expanduser().resolve()
    export_dir.mkdir(parents=True, exist_ok=True)

    normalized_names = [_normalize_filename(name) for name in table_names if name]
    matches = []
    for path in export_dir.iterdir():
        if not path.is_file() or path.suffix.casefold() != ".json":
            continue
        normalized_stem = _normalize_filename(path.stem)
        if any(normalized_stem.startswith(name) for name in normalized_names):
            matches.append((path.name, str(path)))
    return sorted(matches)


def iter_json_export_to_target(
    records: Iterable[Any],
    metadata_rows: Sequence[dict[str, str]],
    timestamp: str | None = None,
) -> Iterator[dict[str, Any]]:
    source_rows = [
        row for row in metadata_rows
        if row["source_table"].strip() and row["source_column"].strip() and row["target_column"].strip()
    ]
    if not source_rows:
        raise ValueError("The selected table has no complete source-to-target column mappings")

    generated_columns = [
        row["target_column"]
        for row in metadata_rows
        if row["target_column"].strip()
        and "current_timestamp()" in row.get("transformation", "").casefold()
    ]
    timestamp = timestamp or datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")

    source_columns = {row["source_column"].strip().casefold() for row in source_rows}
    for row_number, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ValueError(f"JSON row {row_number} must be an object")
        values_by_name: dict[str, Any] = {}
        for key, value in record.items():
            normalized_key = str(key).strip().casefold()
            if normalized_key in values_by_name:
                raise ValueError(f"JSON row {row_number} has duplicate case-insensitive key {key!r}")
            values_by_name[normalized_key] = value

        missing_columns = source_columns.difference(values_by_name)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"JSON row {row_number} is missing mapped source columns: {missing}")

        target_record = {
            row["target_column"]: values_by_name[row["source_column"].strip().casefold()]
            for row in source_rows
        }
        for column in generated_columns:
            target_record[column] = timestamp
        yield target_record


def map_json_export_to_target(records: Any, metadata_rows: Sequence[dict[str, str]]) -> list[dict[str, Any]]:
    if not isinstance(records, list):
        raise ValueError("Each SFTP JSON export must contain an array of row objects")
    return list(iter_json_export_to_target(records, metadata_rows))


def download_sftp_file(sftp_client, remote_path: str, local_path: Path) -> str:
    digest = hashlib.sha256()
    with sftp_client.open(remote_path, "rb") as remote_file, local_path.open("wb") as local_file:
        while True:
            chunk = remote_file.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            local_file.write(chunk)
    return digest.hexdigest()


def hash_local_file(local_path: Path) -> str:
    digest = hashlib.sha256()
    with local_path.open("rb") as source_file:
        while True:
            chunk = source_file.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def transform_json_file(
    source_path: Path,
    target_path: Path,
    metadata_rows: Sequence[dict[str, str]],
) -> int:
    with source_path.open("rb") as source_file:
        prefix = source_file.read(3)
        start_offset = 3 if prefix == b"\xef\xbb\xbf" else 0
        source_file.seek(start_offset)
        first_character = source_file.read(1)
        while first_character in {b" ", b"\t", b"\r", b"\n"}:
            first_character = source_file.read(1)
        if first_character != b"[":
            raise ValueError("Each SFTP JSON export must contain an array of row objects")
        source_file.seek(start_offset)

        records = ijson.items(source_file, "item")
        timestamp = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
        count = 0
        try:
            with target_path.open("w", encoding="utf-8", newline="") as target_file:
                target_file.write("[")
                for record in iter_json_export_to_target(records, metadata_rows, timestamp):
                    if count:
                        target_file.write(",")
                    json.dump(
                        record,
                        target_file,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                    count += 1
                target_file.write("]")
        except ijson.JSONError as exc:
            raise ValueError("SFTP export is not valid JSON") from exc
    return count


def snowpipe_object_names(database: str, schema: str, target_table: str) -> dict[str, str]:
    safe_table = re.sub(r"[^A-Za-z0-9_$]", "_", target_table).upper()
    stage = f"ETL_{safe_table}_STAGE"
    pipe = f"ETL_{safe_table}_PIPE"
    qualified_database = _quote_identifier(database)
    qualified_schema = _quote_identifier(schema)
    qualified_table = _quote_identifier(target_table)
    qualified_stage = _quote_identifier(stage)
    qualified_pipe = _quote_identifier(pipe)
    audit_table = _quote_identifier("ETL_BATCH_AUDIT")
    return {
        "table": f"{qualified_database}.{qualified_schema}.{qualified_table}",
        "stage": f"{qualified_database}.{qualified_schema}.{qualified_stage}",
        "pipe": f"{qualified_database}.{qualified_schema}.{qualified_pipe}",
        "pipe_api": f"{database}.{schema}.{pipe}",
        "audit_table": f"{qualified_database}.{qualified_schema}.{audit_table}",
    }


def create_batch_audit_table(cursor, audit_table: str) -> None:
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {audit_table} ("
        "BATCH_ID VARCHAR NOT NULL, "
        "TARGET_TABLE VARCHAR NOT NULL, "
        "SOURCE_TYPE VARCHAR NOT NULL, "
        "STATUS VARCHAR NOT NULL, "
        "FILES_DISCOVERED NUMBER(38,0) NOT NULL, "
        "FILES_QUEUED NUMBER(38,0) NOT NULL, "
        "ROWS_QUEUED NUMBER(38,0) NOT NULL, "
        "STARTED_AT TIMESTAMP_TZ NOT NULL, "
        "COMPLETED_AT TIMESTAMP_TZ, "
        "ERROR_MESSAGE VARCHAR(4000)"
        ")"
    )


def start_batch_audit(cursor, audit_table: str, batch_id: str, table: str, source: str, file_count: int) -> None:
    cursor.execute(
        f"INSERT INTO {audit_table} "
        "(BATCH_ID, TARGET_TABLE, SOURCE_TYPE, STATUS, FILES_DISCOVERED, "
        "FILES_QUEUED, ROWS_QUEUED, STARTED_AT) "
        "VALUES (%s, %s, %s, 'PROCESSING', %s, 0, 0, CURRENT_TIMESTAMP())",
        (batch_id, table, source.upper(), file_count),
    )


def finish_batch_audit(
    cursor,
    audit_table: str,
    batch_id: str,
    status: str,
    files_queued: int,
    rows_queued: int,
    error_message: str | None = None,
) -> None:
    cursor.execute(
        f"UPDATE {audit_table} SET STATUS = %s, FILES_QUEUED = %s, "
        "ROWS_QUEUED = %s, COMPLETED_AT = CURRENT_TIMESTAMP(), ERROR_MESSAGE = %s "
        "WHERE BATCH_ID = %s",
        (status, files_queued, rows_queued, error_message, batch_id),
    )


def create_snowpipe_objects(cursor, objects: dict[str, str]) -> None:
    cursor.execute(f"CREATE STAGE IF NOT EXISTS {objects['stage']}")
    cursor.execute(
        f"CREATE PIPE IF NOT EXISTS {objects['pipe']} AS "
        f"COPY INTO {objects['table']} FROM @{objects['stage']} "
        "FILE_FORMAT=(TYPE='JSON' STRIP_OUTER_ARRAY=TRUE) "
        "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"
    )


def _load_private_key() -> str:
    key_path = settings.snowflake_pipe_private_key_file
    if not key_path:
        raise ValueError("Set SNOWFLAKE_PIPE_PRIVATE_KEY_FILE for Snowpipe REST key-pair authentication")

    password = settings.snowflake_pipe_private_key_passphrase
    key_bytes = Path(key_path).expanduser().read_bytes()
    private_key = serialization.load_pem_private_key(
        key_bytes,
        password=password.encode("utf-8") if password else None,
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")


def create_ingest_manager(pipe_name: str) -> SimpleIngestManager:
    if not settings.snowflake_account or not settings.snowflake_user:
        raise ValueError("SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER are required for Snowpipe REST")
    options: dict[str, Any] = {}
    if settings.snowflake_pipe_host:
        options["host"] = settings.snowflake_pipe_host
    return SimpleIngestManager(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        pipe=pipe_name,
        private_key=_load_private_key(),
        **options,
    )


def stage_and_submit_json_file(
    snowflake_connection,
    objects: dict[str, str],
    ingest_manager: SimpleIngestManager,
    staged_file_path: Path,
    content_hash: str,
) -> str:
    staged_filename = f"{content_hash}.json"

    source_uri = staged_file_path.resolve().as_uri().replace("'", "''")
    cursor = snowflake_connection.cursor()
    try:
        cursor.execute(
            f"PUT '{source_uri}' @{objects['stage']} "
            "AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
        )
        put_rows = cursor.fetchall()
        if cursor.description and put_rows:
            columns = [description[0].lower() for description in cursor.description]
            if "status" in columns:
                status_index = columns.index("status")
                message_index = columns.index("message") if "message" in columns else None
                for row in put_rows:
                    status = str(row[status_index]).upper()
                    if status not in {"UPLOADED", "SKIPPED"}:
                        message = row[message_index] if message_index is not None else status
                        raise RuntimeError(f"Snowflake stage upload failed: {message}")
    finally:
        cursor.close()

    response = ingest_manager.ingest_files(
        [StagedFile(staged_filename, staged_file_path.stat().st_size)]
    )
    if response.get("responseCode") != "SUCCESS":
        raise RuntimeError(f"Snowpipe did not accept {staged_filename}: {response}")
    logger.info("Snowpipe accepted %s for loading", staged_filename)
    return staged_filename
