from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from etl.load import build_source_query


@dataclass(frozen=True)
class SourceExtractionStats:
    rows_extracted: int
    query_seconds: float
    fetch_seconds: float
    consume_seconds: float


def _is_temporal_source_type(source_data_type: str) -> bool:
    normalized = source_data_type.strip().upper()
    return any(token in normalized for token in ("DATE", "TIME", "TIMESTAMP", "INTERVAL"))


def _is_undefined_source_type(source_data_type: str) -> bool:
    normalized = source_data_type.strip().upper().strip("() ")
    return not normalized or normalized in {"UNDEFINED", "UNKNOWN"}


def build_full_table_query(mapping_rows: Sequence[dict[str, str]]) -> str:
    source_rows = [
        row
        for row in mapping_rows
        if row.get("source_table", "").strip()
        and row.get("source_column", "").strip()
        and row.get("target_column", "").strip()
    ]
    if not source_rows:
        raise ValueError("The selected table has no complete source-to-target column mappings")

    source_tables = {row["source_table"].strip() for row in source_rows}
    if len(source_tables) != 1:
        raise ValueError("A full-table extraction requires all mapped columns to use one 4D source table")

    source_columns = list(dict.fromkeys(row["source_column"].strip() for row in source_rows))
    raw_text_columns = list(
        dict.fromkeys(
            row["source_column"].strip()
            for row in source_rows
            if (
                _is_temporal_source_type(row.get("source_data_type", ""))
                or _is_undefined_source_type(row.get("source_data_type", ""))
            )
        )
    )
    return build_source_query(
        next(iter(source_tables)),
        source_columns,
        raw_text_columns=raw_text_columns,
    )


def count_source_rows(source_connection: Any, source_table: str) -> int:
    table_name = source_table.strip()
    if not table_name or "\x00" in table_name:
        raise ValueError(f"Invalid 4D source table name: {source_table!r}")
    quoted_table = "[" + table_name.replace("]", "]]") + "]"
    cursor = source_connection.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {quoted_table}")
        result = cursor.fetchone()
        if not result or result[0] is None:
            raise RuntimeError(f"4D did not return a row count for {source_table}")
        count = int(result[0])
        if count < 0:
            raise RuntimeError(f"4D returned an invalid row count for {source_table}: {count}")
        return count
    finally:
        cursor.close()


def stream_source_batches(
    source_connection: Any,
    query: str,
    fetch_size: int,
    consume_batch: Callable[[Sequence[Sequence[Any]]], None],
) -> SourceExtractionStats:
    if fetch_size <= 0:
        raise ValueError("Fetch size must be greater than zero")
    if not query.strip():
        raise ValueError("Source query cannot be blank")

    cursor = source_connection.cursor()
    rows_extracted = 0
    query_seconds = 0.0
    fetch_seconds = 0.0
    consume_seconds = 0.0
    try:
        started = perf_counter()
        cursor.execute(query)
        query_seconds = perf_counter() - started

        while True:
            started = perf_counter()
            batch = cursor.fetchmany(fetch_size)
            fetch_seconds += perf_counter() - started
            if not batch:
                break

            started = perf_counter()
            consume_batch(batch)
            consume_seconds += perf_counter() - started
            rows_extracted += len(batch)
    finally:
        cursor.close()

    return SourceExtractionStats(
        rows_extracted=rows_extracted,
        query_seconds=query_seconds,
        fetch_seconds=fetch_seconds,
        consume_seconds=consume_seconds,
    )
