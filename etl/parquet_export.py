from __future__ import annotations

import base64
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any, Sequence
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from etl.source_extract import (
    SourceExtractionStats,
    build_full_table_query,
    count_source_rows,
    stream_source_batches,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParquetExportResult:
    run_id: str
    source_table: str
    target_table: str
    file_path: Path
    row_count: int
    file_size_bytes: int
    source_count_before: int
    source_count_after: int
    query_seconds: float
    fetch_seconds: float
    serialization_seconds: float
    parquet_write_seconds: float
    total_seconds: float


def _safe_path_component(value: str, label: str) -> str:
    component = re.sub(r"[^A-Za-z0-9_-]+", "_", value.strip()).strip("_")
    if not component:
        raise ValueError(f"{label} must contain at least one alphanumeric character")
    return component


def _raw_value_as_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return "base64:" + base64.b64encode(value).decode("ascii")
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, int):
        return str(value)
    return str(value)


def _normalize_timestamp_text(value: str, column_name: str) -> str | None:
    value = value.strip()
    if not value:
        return value
    if re.fullmatch(r"0{1,2}/0{1,2}/0{1,4}\s+0{1,2}:0{2}:0{2}", value):
        logger.debug(
            "Replacing zero-date timestamp with NULL for %s: %r",
            column_name,
            value,
        )
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        parsed = None

    if parsed is None:
        slash_date = re.fullmatch(
            r"(\d{1,2})/(\d{1,2})/(\d{4})(?:[ T](\d{1,2}:\d{2}:\d{2}(?:\.\d{1,6})?))?",
            value,
        )
        if slash_date:
            first, second = int(slash_date.group(1)), int(slash_date.group(2))
            if first > 12 and second <= 12:
                date_format = "%d/%m/%Y"
            elif second > 12 and first <= 12:
                date_format = "%m/%d/%Y"
            else:
                date_format = "%d/%m/%Y"
            time_value = slash_date.group(4)
            format_string = date_format
            if time_value:
                format_string += " %H:%M:%S"
                if "." in time_value:
                    format_string += ".%f"
            try:
                parsed = datetime.strptime(value, format_string)
            except ValueError:
                parsed = None

    if parsed is None:
        for format_string in (
            "%Y/%m/%d %H:%M:%S.%f",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d",
            "%Y-%m-%d",
        ):
            try:
                parsed = datetime.strptime(value, format_string)
                break
            except ValueError:
                continue

    if parsed is None:
        logger.debug(
            "Preserving unparsed source timestamp for %s: %r",
            column_name,
            value,
        )
        return value
    return parsed.isoformat(sep=" ")


def _mapped_value_as_text(value: Any, snowflake_type: str, column_name: str) -> str | None:
    text = _raw_value_as_text(value)
    if text is None:
        return None
    normalized_type = re.sub(r"/\*.*?\*/", "", snowflake_type, flags=re.DOTALL)
    if normalized_type.strip().upper().startswith("TIMESTAMP"):
        return _normalize_timestamp_text(text, column_name)
    return text


def _mapping_for_export(
    mapping_rows: Sequence[dict[str, str]],
) -> tuple[str, str, list[str], list[str], list[dict[str, str]]]:
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

    source_tables = {row["source_table"].strip() for row in source_rows}
    target_tables = {row["target_table"].strip() for row in source_rows}
    if len(source_tables) != 1:
        raise ValueError("A Parquet export requires all mapped columns to use one 4D source table")
    if len(target_tables) != 1:
        raise ValueError("A Parquet export requires all mapped columns to use one Snowflake target table")

    target_columns = [row["target_column"].strip() for row in source_rows]
    normalized_targets = [column.upper() for column in target_columns]
    if len(normalized_targets) != len(set(normalized_targets)):
        raise ValueError("The mapping contains duplicate Snowflake target columns")

    source_columns = list(dict.fromkeys(row["source_column"].strip() for row in source_rows))
    return (
        next(iter(source_tables)),
        next(iter(target_tables)),
        source_columns,
        target_columns,
        source_rows,
    )


def export_table_to_parquet(
    source_connection: Any,
    mapping_rows: Sequence[dict[str, str]],
    export_root: str | Path,
    run_id: str,
    fetch_size: int = 50_000,
) -> ParquetExportResult:
    if fetch_size <= 0:
        raise ValueError("Fetch size must be greater than zero")

    started_at = perf_counter()
    (
        source_table,
        target_table,
        source_columns,
        target_columns,
        source_rows,
    ) = _mapping_for_export(mapping_rows)
    run_component = _safe_path_component(run_id, "Run ID")
    table_component = _safe_path_component(target_table, "Target table")
    run_directory = Path(export_root) / table_component / run_component
    run_directory.mkdir(parents=True, exist_ok=True)
    final_path = run_directory / f"{table_component.lower()}.parquet"
    if final_path.exists():
        raise FileExistsError(f"Refusing to overwrite completed Parquet export: {final_path}")

    query = build_full_table_query(mapping_rows)
    source_count_before = count_source_rows(source_connection, source_table)
    if final_path.exists():
        raise FileExistsError(f"Refusing to overwrite completed Parquet export: {final_path}")

    source_indexes = {column: index for index, column in enumerate(source_columns)}
    row_mapping = [
        (source_indexes[row["source_column"].strip()], target_index)
        for target_index, row in enumerate(source_rows)
    ]
    snowflake_types = [
        row.get("snowflake_datatype", "") or "VARCHAR"
        for row in source_rows
    ]
    schema = pa.schema(
        [pa.field(column, pa.string(), nullable=True) for column in target_columns],
        metadata={
            b"run_id": run_component.encode("utf-8"),
            b"source_table": source_table.encode("utf-8"),
            b"target_table": target_table.encode("utf-8"),
        },
    )

    partial_path = run_directory / f".{table_component.lower()}.{uuid4().hex}.partial"
    writer: pq.ParquetWriter | None = None
    serialization_seconds = 0.0
    parquet_write_seconds = 0.0

    def write_batch(batch: Sequence[Sequence[Any]]) -> None:
        nonlocal serialization_seconds, parquet_write_seconds
        started = perf_counter()
        column_values: list[list[str | None]] = [[] for _ in target_columns]
        for row in batch:
            if len(row) != len(source_columns):
                raise ValueError(
                    f"4D returned {len(row)} column(s) for {source_table}; "
                    f"expected {len(source_columns)}"
                )
            for source_index, target_index in row_mapping:
                column_values[target_index].append(
                    _mapped_value_as_text(
                        row[source_index],
                        snowflake_types[target_index],
                        target_columns[target_index],
                    )
                )
        arrays = [
            pa.array(values, type=pa.string())
            for values in column_values
        ]
        table = pa.Table.from_arrays(arrays, schema=schema)
        serialization_seconds += perf_counter() - started
        started = perf_counter()
        writer.write_table(table)
        parquet_write_seconds += perf_counter() - started

    try:
        writer = pq.ParquetWriter(partial_path, schema, compression="snappy")
        extraction: SourceExtractionStats = stream_source_batches(
            source_connection,
            query,
            fetch_size,
            write_batch,
        )
        writer.close()
        writer = None

        source_count_after = count_source_rows(source_connection, source_table)
        if extraction.rows_extracted != source_count_before:
            raise RuntimeError(
                f"4D row count changed or extraction was incomplete for {source_table}: "
                f"count before={source_count_before}, extracted={extraction.rows_extracted}"
            )
        if source_count_after != extraction.rows_extracted:
            raise RuntimeError(
                f"4D row count changed during extraction for {source_table}: "
                f"extracted={extraction.rows_extracted}, count after={source_count_after}"
            )

        parquet_rows = pq.ParquetFile(partial_path).metadata.num_rows
        if parquet_rows != extraction.rows_extracted:
            raise RuntimeError(
                f"Parquet row count mismatch for {source_table}: "
                f"extracted={extraction.rows_extracted}, parquet={parquet_rows}"
            )
        if final_path.exists():
            raise FileExistsError(
                f"Refusing to overwrite completed Parquet export: {final_path}"
            )
        partial_path.rename(final_path)
    except BaseException:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                logger.exception("Could not close incomplete Parquet writer for %s", source_table)
        try:
            partial_path.unlink(missing_ok=True)
        except OSError:
            logger.exception("Could not remove incomplete Parquet file %s", partial_path)
        raise

    return ParquetExportResult(
        run_id=run_component,
        source_table=source_table,
        target_table=target_table,
        file_path=final_path,
        row_count=extraction.rows_extracted,
        file_size_bytes=final_path.stat().st_size,
        source_count_before=source_count_before,
        source_count_after=source_count_after,
        query_seconds=extraction.query_seconds,
        fetch_seconds=extraction.fetch_seconds,
        serialization_seconds=serialization_seconds,
        parquet_write_seconds=parquet_write_seconds,
        total_seconds=perf_counter() - started_at,
    )
