from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from connections.sftp_connection import connect_to_sftp
from connections.snowflake_connection import connect_to_snowflake
from etl.load import create_missing_tables_from_metadata
from etl.metadata import load_metadata_from_excel
from etl.sftp_snowpipe import (
    create_ingest_manager,
    create_snowpipe_objects,
    find_table_json_files,
    download_sftp_file,
    snowpipe_object_names,
    stage_and_submit_json_file,
    transform_json_file,
)

logger = logging.getLogger(__name__)
DEFAULT_EXCEL_PATH = PROJECT_ROOT / "4D_to_Snowflake (1-50 tables mapping).xlsx"


def _normalize_table_name(value: str) -> str:
    return str(value).strip().upper().replace(" ", "_").replace("-", "_")


def run_for_table(excel_path: str, table_name: str) -> dict:
    metadata = load_metadata_from_excel(excel_path, sheet_name=table_name)
    normalized_table = _normalize_table_name(table_name)
    table_rows = metadata.get(normalized_table, [])
    if not table_rows:
        raise ValueError(f"No mapping rows found for target table: {table_name}")

    source_table_names = {
        row["source_table"] for row in table_rows if row["source_table"].strip()
    }
    ssh_client, sftp_client = connect_to_sftp()
    snowflake_connection = None
    try:
        remote_files = find_table_json_files(
            sftp_client,
            settings.sftp_remote_dir,
            [table_name, normalized_table, *source_table_names],
        )
        if not remote_files:
            logger.info("No JSON exports found for %s in %s", table_name, settings.sftp_remote_dir)
            return {"table": normalized_table, "files_queued": 0, "files": []}

        objects = snowpipe_object_names(
            settings.snowflake_database,
            settings.snowflake_schema,
            normalized_table,
        )
        ingest_manager = create_ingest_manager(objects["pipe_api"])
        snowflake_connection = connect_to_snowflake()
        with snowflake_connection.cursor() as cursor:
            create_missing_tables_from_metadata(
                cursor,
                {normalized_table: table_rows},
                settings.snowflake_schema,
            )
            create_snowpipe_objects(cursor, objects)

        submitted_files = []
        for filename, remote_path in remote_files:
            with tempfile.TemporaryDirectory(prefix="sftp_snowpipe_") as temporary_dir:
                source_path = Path(temporary_dir) / "source.json"
                staged_path = Path(temporary_dir) / "transformed.json"
                try:
                    content_hash = download_sftp_file(sftp_client, remote_path, source_path)
                    row_count = transform_json_file(source_path, staged_path, table_rows)
                except ValueError as exc:
                    raise ValueError(f"Invalid SFTP export {filename!r}: {exc}") from exc
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

        return {
            "table": normalized_table,
            "files_queued": len(submitted_files),
            "files": submitted_files,
        }
    finally:
        sftp_client.close()
        ssh_client.close()
        if snowflake_connection is not None:
            snowflake_connection.close()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch JSON exports from SFTP, stage them in Snowflake, and queue Snowpipe loads."
    )
    parser.add_argument("--excel", default=str(DEFAULT_EXCEL_PATH), help="Path to the Excel mapping workbook")
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
    result = run_for_table(args.excel, table_name)
    print(f"Queued {result['files_queued']} file(s) for Snowpipe table {result['table']}")
    for item in result["files"]:
        print(
            f"  SFTP {item['source_file']} ({item['rows']} rows) "
            f"-> Snowflake stage {item['staged_file']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
