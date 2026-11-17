from __future__ import annotations

import logging
import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from time import perf_counter
from typing import Any, Sequence
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from etl.csv_export import (
    _integer_key,
    _primary_key_source_column,
    _query_key_ranges,
    _quote_4d_identifier,
    _resolve_fetch_size,
)
from etl.parquet_export import (
    _mapped_value_as_text,
    _mapping_for_export,
    _safe_path_component,
)
from etl.source_extract import (
    SourceExtractionStats,
    build_full_table_query,
    count_source_rows,
    stream_source_batches,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParallelParquetExportResult:
    run_id: str
    source_table: str
    target_table: str
    file_path: Path
    row_count: int
    file_size_bytes: int
    worker_count: int
    fetch_size: int
    source_count_before: int
    source_count_after: int
    query_seconds: float
    fetch_seconds: float
    serialization_seconds: float
    parquet_write_seconds: float
    total_seconds: float


@dataclass(frozen=True)
class _WorkerResult:
    worker_number: int
    path: Path
    row_count: int
    expected_rows: int
    query_seconds: float = 0.0
    fetch_seconds: float = 0.0
    serialization_seconds: float = 0.0
    parquet_write_seconds: float = 0.0


def _convert_batch(
    batch: Sequence[Sequence[Any]],
    source_columns: Sequence[str],
    target_columns: Sequence[str],
    row_mapping: Sequence[tuple[int, int]],
    snowflake_types: Sequence[str],
    source_table: str,
    schema: pa.Schema,
) -> tuple[pa.Table, float]:
    started = perf_counter()
    values: list[list[str | None]] = [[] for _ in target_columns]
    for row in batch:
        if len(row) != len(source_columns):
            raise ValueError(
                f"4D returned {len(row)} column(s) for {source_table}; "
                f"expected {len(source_columns)}"
            )
        for source_index, target_index in row_mapping:
            values[target_index].append(
                _mapped_value_as_text(
                    row[source_index],
                    snowflake_types[target_index],
                    target_columns[target_index],
                )
            )
    table = pa.Table.from_arrays(
        [pa.array(column, type=pa.string()) for column in values],
        schema=schema,
    )
    return table, perf_counter() - started


def _merge_parquet(
    worker_results: Sequence[_WorkerResult],
    output_path: Path,
    schema: pa.Schema,
    expected_rows: int,
    source_table: str,
) -> int:
    merged_rows = 0
    with pq.ParquetWriter(output_path, schema, compression="snappy") as writer:
        for result in worker_results:
            parquet_file = pq.ParquetFile(result.path)
            try:
                if not parquet_file.schema_arrow.equals(schema, check_metadata=True):
                    raise RuntimeError(
                        f"Parquet schema mismatch in worker file {result.path}"
                    )
                for row_group in range(parquet_file.metadata.num_row_groups):
                    table = parquet_file.read_row_group(row_group)
                    writer.write_table(table)
                    merged_rows += table.num_rows
            finally:
                parquet_file.close()
    if merged_rows != expected_rows:
        raise RuntimeError(
            f"Final Parquet row count mismatch for {source_table}: "
            f"expected={expected_rows}, parquet={merged_rows}"
        )
    return merged_rows


def export_table_to_parquet_parallel(
    source_connection: Any,
    mapping_rows: Sequence[dict[str, str]],
    export_root: str | Path,
    run_id: str,
    connection_factory: Any,
    fetch_size: int | None = None,
    worker_count: int = 3,
) -> ParallelParquetExportResult:
    if not 1 <= worker_count <= 3:
        raise ValueError("Worker count must be between 1 and 3")

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
        raise FileExistsError(
            f"Refusing to overwrite completed Parquet export: {final_path}"
        )

    primary_key = _primary_key_source_column(source_rows)
    key_ranges = None
    if primary_key is None:
        source_count_before = count_source_rows(source_connection, source_table)
    else:
        source_count_before, key_ranges = _query_key_ranges(
            source_connection,
            source_table,
            primary_key,
            worker_count,
        )
    resolved_fetch_size = _resolve_fetch_size(
        source_count_before,
        worker_count,
        fetch_size,
    )
    source_indexes = {column: index for index, column in enumerate(source_columns)}
    row_mapping = [
        (source_indexes[row["source_column"].strip()], index)
        for index, row in enumerate(source_rows)
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
    final_partial = run_directory / f".{table_component.lower()}.{uuid4().hex}.partial"
    partial_paths: list[Path] = []
    results: list[_WorkerResult] = []
    connections: list[Any] = []
    connection_lock = Lock()
    query_seconds = 0.0
    fetch_seconds = 0.0

    try:
        if primary_key is None:
            task_count = max(1, math.ceil(source_count_before / resolved_fetch_size))
            active_workers = min(worker_count, task_count)
            pending: list[Any] = []
            task_number = 0

            def write_chunk(
                batch: Sequence[Sequence[Any]],
                number: int,
                path: Path,
            ) -> _WorkerResult:
                table, serialization_seconds = _convert_batch(
                    batch,
                    source_columns,
                    target_columns,
                    row_mapping,
                    snowflake_types,
                    source_table,
                    schema,
                )
                write_started = perf_counter()
                with pq.ParquetWriter(path, schema, compression="snappy") as writer:
                    writer.write_table(table)
                return _WorkerResult(
                    number,
                    path,
                    table.num_rows,
                    table.num_rows,
                    serialization_seconds=serialization_seconds,
                    parquet_write_seconds=perf_counter() - write_started,
                )

            extraction: SourceExtractionStats | None = None
            with ThreadPoolExecutor(max_workers=active_workers) as executor:
                def dispatch(batch: Sequence[Sequence[Any]]) -> None:
                    nonlocal task_number
                    task_number += 1
                    path = run_directory / (
                        f".{table_component.lower()}_{uuid4().hex}_"
                        f"task_{task_number:06d}.partial"
                    )
                    partial_paths.append(path)
                    pending.append(
                        executor.submit(write_chunk, batch, task_number, path)
                    )
                    if len(pending) >= active_workers:
                        results.append(pending.pop(0).result())

                extraction = stream_source_batches(
                    source_connection,
                    build_full_table_query(mapping_rows),
                    resolved_fetch_size,
                    dispatch,
                )
                results.extend(future.result() for future in pending)
            if extraction is None:
                raise RuntimeError("4D Parquet extraction did not complete")
            query_seconds = extraction.query_seconds
            fetch_seconds = extraction.fetch_seconds
            results.sort(key=lambda result: result.worker_number)
            active_workers = min(worker_count, max(1, len(results)))
        else:
            if key_ranges is None:
                raise RuntimeError("Primary-key range planning did not complete")
            active_workers = len(key_ranges)
            key_index = source_indexes[primary_key]
            query = build_full_table_query(mapping_rows)

            def extract_range(number: int, key_range: Any) -> _WorkerResult:
                key_sql = _quote_4d_identifier(primary_key)
                predicate = f"{key_sql} >= {key_range.lower}"
                if key_range.upper is not None:
                    predicate += f" AND {key_sql} < {key_range.upper}"
                worker_query = f"{query} WHERE {predicate} ORDER BY {key_sql}"
                connection = connection_factory()
                with connection_lock:
                    if connection is source_connection or any(
                        connection is existing for existing in connections
                    ):
                        raise RuntimeError(
                            "Each Parquet worker must use a distinct 4D connection"
                        )
                    connections.append(connection)
                path = run_directory / (
                    f".{table_component.lower()}_{uuid4().hex}_{number:03d}.partial"
                )
                partial_paths.append(path)
                cursor = None
                rows_written = 0
                worker_query_seconds = 0.0
                worker_fetch_seconds = 0.0
                serialization_seconds = 0.0
                parquet_write_seconds = 0.0
                try:
                    cursor = connection.cursor()
                    query_started = perf_counter()
                    cursor.execute(worker_query)
                    worker_query_seconds = perf_counter() - query_started
                    with pq.ParquetWriter(path, schema, compression="snappy") as writer:
                        while True:
                            fetch_started = perf_counter()
                            batch = cursor.fetchmany(resolved_fetch_size)
                            worker_fetch_seconds += perf_counter() - fetch_started
                            if not batch:
                                break
                            for row in batch:
                                if len(row) != len(source_columns):
                                    raise ValueError(
                                        f"4D returned {len(row)} column(s) for "
                                        f"{source_table}; expected {len(source_columns)}"
                                    )
                                key = _integer_key(row[key_index], "row")
                                if key < key_range.lower or (
                                    key_range.upper is not None
                                    and key >= key_range.upper
                                ):
                                    raise RuntimeError(
                                        f"Primary key {key} fell outside worker "
                                        f"{number} range"
                                    )
                            table, elapsed = _convert_batch(
                                batch,
                                source_columns,
                                target_columns,
                                row_mapping,
                                snowflake_types,
                                source_table,
                                schema,
                            )
                            serialization_seconds += elapsed
                            write_started = perf_counter()
                            writer.write_table(table)
                            parquet_write_seconds += perf_counter() - write_started
                            rows_written += table.num_rows
                    if rows_written != key_range.expected_rows:
                        raise RuntimeError(
                            f"Worker {number} extracted {rows_written} rows; "
                            f"expected {key_range.expected_rows}"
                        )
                finally:
                    if cursor is not None:
                        try:
                            cursor.close()
                        finally:
                            connection.close()
                    else:
                        connection.close()
                return _WorkerResult(
                    number,
                    path,
                    rows_written,
                    key_range.expected_rows,
                    worker_query_seconds,
                    worker_fetch_seconds,
                    serialization_seconds,
                    parquet_write_seconds,
                )

            with ThreadPoolExecutor(max_workers=active_workers) as executor:
                futures = [
                    executor.submit(extract_range, index + 1, key_range)
                    for index, key_range in enumerate(key_ranges)
                ]
                results = [future.result() for future in futures]
            query_seconds = sum(result.query_seconds for result in results)
            fetch_seconds = sum(result.fetch_seconds for result in results)

        extracted_count = sum(result.row_count for result in results)
        if extracted_count != source_count_before:
            raise RuntimeError(
                f"4D row count changed or extraction was incomplete for "
                f"{source_table}: count before={source_count_before}, "
                f"extracted={extracted_count}"
            )
        source_count_after = count_source_rows(source_connection, source_table)
        if source_count_after != extracted_count:
            raise RuntimeError(
                f"4D row count changed during extraction for {source_table}: "
                f"extracted={extracted_count}, count after={source_count_after}"
            )

        results.sort(key=lambda result: result.worker_number)
        _merge_parquet(results, final_partial, schema, extracted_count, source_table)
        final_file = pq.ParquetFile(final_partial)
        try:
            if final_file.metadata.num_rows != extracted_count:
                raise RuntimeError(
                    f"Parquet row count mismatch for {source_table}"
                )
        finally:
            final_file.close()
        if final_path.exists():
            raise FileExistsError(
                f"Refusing to overwrite completed Parquet export: {final_path}"
            )
        for path in partial_paths:
            path.unlink(missing_ok=True)
        final_partial.rename(final_path)
    except BaseException:
        for path in (*partial_paths, final_partial):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logger.exception("Could not remove incomplete Parquet file %s", path)
        raise

    return ParallelParquetExportResult(
        run_id=run_component,
        source_table=source_table,
        target_table=target_table,
        file_path=final_path,
        row_count=extracted_count,
        file_size_bytes=final_path.stat().st_size,
        worker_count=active_workers,
        fetch_size=resolved_fetch_size,
        source_count_before=source_count_before,
        source_count_after=source_count_after,
        query_seconds=query_seconds,
        fetch_seconds=fetch_seconds,
        serialization_seconds=sum(result.serialization_seconds for result in results),
        parquet_write_seconds=sum(result.parquet_write_seconds for result in results),
        total_seconds=perf_counter() - started_at,
    )
