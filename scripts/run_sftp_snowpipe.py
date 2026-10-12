from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from uuid import uuid4
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from connections.snowflake_connection import connect_to_snowflake
from etl.load import create_missing_tables_from_metadata
from etl.metadata import load_metadata_from_excel
from etl.sftp_snowpipe import (
    create_ingest_manager,
    create_batch_audit_table,
    create_snowpipe_objects,
    finish_batch_audit,
    find_table_json_files,
    find_local_table_json_files,
    download_sftp_file,
    hash_local_file,
    snowpipe_object_names,
    start_batch_audit,
    stage_and_submit_json_file,
    transform_json_file,
)

logger = logging.getLogger(__name__)
DEFAULT_EXCEL_PATH = PROJECT_ROOT / "4D_to_Snowflake (1-50 tables mapping).xlsx"


def _normalize_table_name(value: str) -> str:
    return str(value).strip().upper().replace(" ", "_").replace("-", "_")


def run_for_table(
    excel_path: str,
    table_name: str,
    source: str = "local",
    local_dir: str | None = None,
) -> dict:
    metadata = load_metadata_from_excel(excel_path, sheet_name=table_name)
    normalized_table = _normalize_table_name(table_name)
    table_rows = metadata.get(normalized_table, [])
    if not table_rows:
        raise ValueError(f"No mapping rows found for target table: {table_name}")

    source_table_names = {
        row["source_table"] for row in table_rows if row["source_table"].strip()
    }
    ssh_client = None
    sftp_client = None
    snowflake_connection = None
    submitted_files = []
    batch_id = None
    try:
        table_names = [table_name, normalized_table, *source_table_names]
        if source == "local":
            export_dir = Path(local_dir or settings.local_export_dir).expanduser()
            files = find_local_table_json_files(export_dir, table_names)
            file_source = "local"
            logger.info("Searching local JSON exports in %s", export_dir.resolve())
        elif source == "sftp":
            from connections.sftp_connection import connect_to_sftp

            ssh_client, sftp_client = connect_to_sftp()
            files = find_table_json_files(sftp_client, settings.sftp_remote_dir, table_names)
            file_source = "sftp"
        else:
            raise ValueError(f"Unsupported export source: {source}")

        if not files:
            location = str(export_dir.resolve()) if source == "local" else settings.sftp_remote_dir
            logger.info(
                "No JSON exports found for %s in %s; place a completed table JSON export there and rerun",
                table_name,
                location,
            )
            return {"table": normalized_table, "batch_id": None, "files_queued": 0, "files": []}

        objects = snowpipe_object_names(
            settings.snowflake_database,
            settings.snowflake_schema,
            normalized_table,
        )
        ingest_manager = create_ingest_manager(objects["pipe_api"])
        snowflake_connection = connect_to_snowflake()
        batch_id = str(uuid4())
        logger.info(
            "Starting Snowpipe batch %s for %s with %d discovered file(s)",
            batch_id,
            normalized_table,
            len(files),
        )
        with snowflake_connection.cursor() as cursor:
            create_batch_audit_table(cursor, objects["audit_table"])
            start_batch_audit(
                cursor,
                objects["audit_table"],
                batch_id,
                normalized_table,
                source,
                len(files),
            )
        try:
            with snowflake_connection.cursor() as cursor:
                create_missing_tables_from_metadata(
                    cursor,
                    {normalized_table: table_rows},
                    settings.snowflake_schema,
                )
                create_snowpipe_objects(cursor, objects)

            for filename, source_path_string in files:
                source_path = Path(source_path_string)
                with tempfile.TemporaryDirectory(prefix=f"{file_source}_snowpipe_") as temporary_dir:
                    staged_path = Path(temporary_dir) / "transformed.json"
                    try:
                        if source == "sftp":
                            source_path = Path(temporary_dir) / "source.json"
                            content_hash = download_sftp_file(sftp_client, source_path_string, source_path)
                        else:
                            content_hash = hash_local_file(source_path)
                        row_count = transform_json_file(source_path, staged_path, table_rows)
                    except ValueError as exc:
                        raise ValueError(f"Invalid JSON export {filename!r}: {exc}") from exc
                    staged_file = stage_and_submit_json_file(
                        snowflake_connection,
                        objects,
                        ingest_manager,
                        staged_path,
                        content_hash,
                    )
                    submitted_files.append({
                        "source_file": filename,
                        "staged_file": staged_file,
                        "rows": row_count,
                    })

            with snowflake_connection.cursor() as cursor:
                finish_batch_audit(
                    cursor,
                    objects["audit_table"],
                    batch_id,
                    "QUEUED",
                    len(submitted_files),
                    sum(item["rows"] for item in submitted_files),
                )
        except Exception as exc:
            logger.exception("Snowpipe batch %s failed", batch_id)
            try:
                with snowflake_connection.cursor() as cursor:
                    finish_batch_audit(
                        cursor,
                        objects["audit_table"],
                        batch_id,
                        "FAILED",
                        len(submitted_files),
                        sum(item["rows"] for item in submitted_files),
                        str(exc)[:4000],
                    )
            except Exception as audit_error:
                raise RuntimeError(
                    f"ETL batch {batch_id} failed ({exc}); audit status update also failed: {audit_error}"
                ) from exc
            raise

        return {
            "table": normalized_table,
            "batch_id": batch_id,
            "files_queued": len(submitted_files),
            "files": submitted_files,
        }
    finally:
        if sftp_client is not None:
            sftp_client.close()
        if ssh_client is not None:
            ssh_client.close()
        if snowflake_connection is not None:
            snowflake_connection.close()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read local or SFTP JSON exports, stage them in Snowflake, and queue Snowpipe loads."
    )
    parser.add_argument("--excel", default=str(DEFAULT_EXCEL_PATH), help="Path to the Excel mapping workbook")
    parser.add_argument(
        "--source",
        choices=("local", "sftp"),
        default="local",
        help="Where to read JSON exports from (default: local).",
    )
    parser.add_argument(
        "--local-dir",
        default=settings.local_export_dir,
        help="Folder with completed local JSON exports (default: LOCAL_EXPORT_DIR).",
    )
    parser.add_argument(
        "--table",
        nargs="+",
        required=True,
        help="Target table worksheet/name; multiword names may be quoted or unquoted.",
    )
    return parser.parse_args(argv)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args()
    table_name = " ".join(args.table)
    result = run_for_table(args.excel, table_name, args.source, args.local_dir)
    if result["batch_id"]:
        print(f"Batch ID: {result['batch_id']}")
    print(f"Queued {result['files_queued']} file(s) for Snowpipe table {result['table']}")
    for item in result["files"]:
        print(
            f"  {args.source.upper()} {item['source_file']} ({item['rows']} rows) "
            f"-> Snowflake stage {item['staged_file']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
