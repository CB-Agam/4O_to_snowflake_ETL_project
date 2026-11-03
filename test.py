from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, Sequence

from config.logging_config import configure_logging
from config.settings import settings
from connections.fourd_connection import close_4d_connection, connect_to_4d
from connections.snowflake_connection import (
    close_snowflake_connection,
    connect_to_snowflake,
)
from etl.ddl import build_create_table_sql
from etl.load import load_query_to_snowflake_in_batches, table_exists_in_snowflake
from etl.metadata import load_metadata_from_excel

PROJECT_ROOT = Path(__file__).resolve().parent
WORKBOOK = PROJECT_ROOT / "4D_to_Snowflake mapping document.xlsx"
MAPPING_SHEET = "Tickets"
SOURCE_TABLE = "Tickets"
TARGET_TABLE = "TICKETS_REPLICATOR_TEST"
CSV_PATH = PROJECT_ROOT / "tickets_4d_export.csv"
BATCH_SIZE = 50_000
NULL_TOKEN = r"\N"
ESCAPE_PREFIX = "\\"

logger = logging.getLogger(__name__)


def _csv_value(value: Any) -> str:
    if value is None:
        return NULL_TOKEN
    if isinstance(value, bytes):
        raise TypeError("Tickets export returned a binary value that cannot be safely CSV encoded")
    if hasattr(value, "isoformat"):
        value = value.isoformat()
    text = str(value)
    if text.startswith(ESCAPE_PREFIX):
        return ESCAPE_PREFIX + text
    return text


def _decode_csv_value(value: str) -> Any:
    if value == NULL_TOKEN:
        return None
    if value.startswith(ESCAPE_PREFIX * 2):
        return value[1:]
    return value


def export_4d_table(source_connection, source_columns: Sequence[str]) -> tuple[int, int, int]:
    query = (
        "SELECT "
        + ", ".join(f"[{column.replace(']', ']]')}]" for column in source_columns)
        + f" FROM [{SOURCE_TABLE}]"
    )
    cursor = source_connection.cursor()
    total_rows = 0
    null_keys = 0
    seen_keys: set[str] = set()
    duplicate_keys: set[str] = set()
    replicator_index = next(
        index
        for index, column in enumerate(source_columns)
        if column.casefold() == "replicator_key"
    )
    try:
        cursor.execute(query)
        with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(source_columns)
            while True:
                batch = cursor.fetchmany(BATCH_SIZE)
                if not batch:
                    break
                for row in batch:
                    if len(row) != len(source_columns):
                        raise ValueError(
                            f"4D returned {len(row)} columns; expected {len(source_columns)}"
                        )
                    key = row[replicator_index]
                    if key is None:
                        null_keys += 1
                    else:
                        normalized_key = str(key).strip().casefold()
                        if normalized_key in seen_keys:
                            duplicate_keys.add(normalized_key)
                        seen_keys.add(normalized_key)
                    writer.writerow([_csv_value(value) for value in row])
                    total_rows += 1
    finally:
        cursor.close()
    return total_rows, null_keys, len(duplicate_keys)


class _CsvSourceCursor:
    def __init__(self, path: Path, expected_columns: Sequence[str]) -> None:
        self._path = path
        self._expected_columns = list(expected_columns)
        self._file = None
        self._reader = None

    def execute(self, query: str) -> _CsvSourceCursor:
        if not query.strip():
            raise ValueError("CSV loader requires a non-empty source query placeholder")
        self._file = self._path.open("r", encoding="utf-8-sig", newline="")
        self._reader = csv.reader(self._file)
        header = next(self._reader, None)
        if header != self._expected_columns:
            self.close()
            raise ValueError(
                f"CSV header mismatch: expected {self._expected_columns!r}, got {header!r}"
            )
        return self

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        if self._reader is None:
            raise RuntimeError("CSV cursor has not been opened")
        rows = []
        for _ in range(size):
            try:
                row = next(self._reader)
            except StopIteration:
                break
            if len(row) != len(self._expected_columns):
                raise ValueError(
                    f"CSV row has {len(row)} columns; expected {len(self._expected_columns)}"
                )
            rows.append(tuple(_decode_csv_value(value) for value in row))
        return rows

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
            self._reader = None


class _CsvSourceConnection:
    def __init__(self, path: Path, columns: Sequence[str]) -> None:
        self._path = path
        self._columns = columns

    def cursor(self) -> _CsvSourceCursor:
        return _CsvSourceCursor(self._path, self._columns)


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def main() -> None:
    configure_logging()
    if not WORKBOOK.is_file():
        raise FileNotFoundError(f"Mapping workbook not found: {WORKBOOK}")
    if CSV_PATH.exists():
        raise FileExistsError(
            f"Refusing to overwrite the existing export: {CSV_PATH}. "
            "Move or rename it before running this test again."
        )

    metadata = load_metadata_from_excel(str(WORKBOOK), sheet_name=MAPPING_SHEET)
    rows = metadata.get(TARGET_TABLE.removesuffix("_REPLICATOR_TEST"), [])
    if not rows:
        rows = metadata.get("TICKETS", [])
    source_rows = [
        row
        for row in rows
        if row["source_table"].strip()
        and row["source_column"].strip()
        and row["target_column"].strip()
    ]
    if not source_rows or not any(
        row["source_column"].strip().casefold() == "replicator_key"
        for row in source_rows
    ):
        raise ValueError("Tickets mapping must include the Replicator_Key source column")
    source_columns = [row["source_column"].strip() for row in source_rows]
    target_columns = [row["target_column"].strip() for row in source_rows]

    snowflake_connection = connect_to_snowflake()
    fourd_connection = None
    try:
        cursor = snowflake_connection.cursor()
        try:
            if table_exists_in_snowflake(
                cursor,
                settings.snowflake_schema,
                TARGET_TABLE,
            ):
                raise FileExistsError(
                    f"Refusing to append to existing Snowflake test table "
                    f"{settings.snowflake_database}.{settings.snowflake_schema}.{TARGET_TABLE}"
                )
        finally:
            cursor.close()

        fourd_connection = connect_to_4d()
        source_count, null_keys, duplicate_keys = export_4d_table(
            fourd_connection,
            source_columns,
        )
        logger.info(
            "Exported %s rows from 4D %s to %s; Replicator_Key NULLs=%s, duplicate keys=%s",
            source_count,
            SOURCE_TABLE,
            CSV_PATH,
            null_keys,
            duplicate_keys,
        )
        if source_count == 0:
            raise RuntimeError(f"4D table {SOURCE_TABLE} returned no rows")

        cursor = snowflake_connection.cursor()
        try:
            create_sql = build_create_table_sql(
                TARGET_TABLE,
                rows,
                schema_name=settings.snowflake_schema,
            )
            cursor.execute(create_sql)
            qualified_table = ".".join(
                _quoted_identifier(part)
                for part in (
                    settings.snowflake_database,
                    settings.snowflake_schema,
                    TARGET_TABLE,
                )
            )
            loaded_rows = load_query_to_snowflake_in_batches(
                _CsvSourceConnection(CSV_PATH, source_columns),
                snowflake_connection,
                cursor,
                "SELECT data from exported Tickets CSV",
                settings.snowflake_database,
                settings.snowflake_schema,
                TARGET_TABLE,
                target_columns,
                batch_size=BATCH_SIZE,
            )
            if loaded_rows != source_count:
                raise RuntimeError(
                    f"Loaded {loaded_rows} rows into {qualified_table}; "
                    f"CSV contains {source_count}"
                )

            cursor.execute(
                f"SELECT COUNT(*), COUNT(REPLICATOR_KEY), "
                f"COUNT(DISTINCT REPLICATOR_KEY) FROM {qualified_table}"
            )
            snowflake_count, non_null_keys, distinct_keys = cursor.fetchone()
        finally:
            cursor.close()

        logger.info(
            "Loaded %s rows into %s; rows=%s, non-null Replicator_Key=%s, "
            "distinct Replicator_Key=%s",
            loaded_rows,
            qualified_table,
            snowflake_count,
            non_null_keys,
            distinct_keys,
        )
        print(f"4D CSV: {CSV_PATH}")
        print(f"Snowflake test table: {qualified_table}")
        print(f"Rows exported: {source_count}")
        print(f"Rows loaded: {loaded_rows}")
        print(f"4D Replicator_Key NULLs: {null_keys}")
        print(f"4D duplicate Replicator_Key values: {duplicate_keys}")
        print(f"Snowflake non-null Replicator_Key values: {non_null_keys}")
        print(f"Snowflake distinct Replicator_Key values: {distinct_keys}")
    finally:
        if fourd_connection is not None:
            close_4d_connection(fourd_connection)
        close_snowflake_connection(snowflake_connection)


if __name__ == "__main__":
    main()
