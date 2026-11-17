from __future__ import annotations

import csv
import logging
import math
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from threading import Lock
from time import perf_counter
from typing import Any, Sequence
from uuid import uuid4

from etl.parquet_export import (
    _mapping_for_export,
    _raw_value_as_text,
    _safe_path_component,
)
from etl.source_extract import (
    SourceExtractionStats,
    build_full_table_query,
    count_source_rows,
    stream_source_batches,
)

logger = logging.getLogger(__name__)
CSV_NULL_MARKER = "__ETL_CSV_NULL_9C487D4E__"
MAX_AUTO_FETCH_SIZE = 100_000


@dataclass(frozen=True)
class CsvExportResult:
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
    csv_write_seconds: float
    total_seconds: float


@dataclass(frozen=True)
class CsvWorkerResult:
    worker_number: int
    file_path: Path
    row_count: int
    expected_rows: int
    query_seconds: float
    fetch_seconds: float
    serialization_seconds: float
    csv_write_seconds: float


@dataclass(frozen=True)
class ParallelCsvExportResult:
    run_id: str
    source_table: str
    target_table: str
    file_paths: tuple[Path, ...]
    row_count: int
    file_size_bytes: int
    worker_count: int
    fetch_size: int
    source_count_before: int
    source_count_after: int
    query_seconds: float
    fetch_seconds: float
    serialization_seconds: float
    csv_write_seconds: float
    total_seconds: float


@dataclass(frozen=True)
class _KeyRange:
    lower: int
    upper: int | None
    expected_rows: int


def _resolve_fetch_size(
    source_row_count: int,
    worker_count: int,
    requested_fetch_size: int | None,
) -> int:
    if requested_fetch_size is not None:
        if requested_fetch_size <= 0:
            raise ValueError("Fetch size must be greater than zero")
        return requested_fetch_size
    active_workers = min(worker_count, max(1, source_row_count))
    rows_per_worker = math.ceil(source_row_count / active_workers)
    return max(1, min(MAX_AUTO_FETCH_SIZE, rows_per_worker))


def _export_without_primary_key(
    source_connection: Any,
    mapping_rows: Sequence[dict[str, str]],
    export_root: str | Path,
    run_id: str,
    requested_fetch_size: int | None,
    worker_count: int,
    started_at: float,
) -> ParallelCsvExportResult:
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

    source_count_before = count_source_rows(source_connection, source_table)
    fetch_size = _resolve_fetch_size(
        source_count_before,
        worker_count,
        requested_fetch_size,
    )
    final_path = run_directory / f"{table_component.lower()}.csv"
    if final_path.exists():
        raise FileExistsError(
            f"Refusing to overwrite completed CSV export: {final_path}"
        )

    task_count = max(1, math.ceil(source_count_before / fetch_size))
    effective_workers = min(worker_count, task_count)
    source_indexes = {column: index for index, column in enumerate(source_columns)}
    row_mapping = [
        (source_indexes[row["source_column"].strip()], target_index)
        for target_index, row in enumerate(source_rows)
    ]
    logger.warning(
        "No mapped primary key found for %s; reading with one 4D cursor in "
        "%s-row tasks and using %s CSV worker(s)",
        source_table,
        fetch_size,
        effective_workers,
    )

    chunk_paths: list[Path] = []
    chunk_results: list[tuple[int, Path, int, float, float]] = []
    extraction: SourceExtractionStats | None = None
    source_count_after = 0
    final_partial = run_directory / f".{table_component.lower()}.{uuid4().hex}.partial"
    task_number = 0

    def write_chunk(
        batch: Sequence[Sequence[Any]],
        partial_path: Path,
    ) -> tuple[int, float, float]:
        serialization_started = perf_counter()
        serialized_rows: list[list[str]] = []
        for row in batch:
            if len(row) != len(source_columns):
                raise ValueError(
                    f"4D returned {len(row)} column(s) for {source_table}; "
                    f"expected {len(source_columns)}"
                )
            serialized: list[str] = []
            for source_index, target_index in row_mapping:
                value = _raw_value_as_text(row[source_index])
                if value == CSV_NULL_MARKER:
                    raise ValueError(
                        f"Source value in "
                        f"{source_table}.{target_columns[target_index]} "
                        "collides with the reserved CSV NULL marker"
                    )
                serialized.append(CSV_NULL_MARKER if value is None else value)
            serialized_rows.append(serialized)
        serialization_seconds = perf_counter() - serialization_started

        csv_write_seconds = 0.0
        with partial_path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output, dialect="excel", lineterminator="\n")
            write_started = perf_counter()
            writer.writerows(serialized_rows)
            csv_write_seconds = perf_counter() - write_started
            output.flush()
        return len(serialized_rows), serialization_seconds, csv_write_seconds

    try:
        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            pending: list[tuple[int, Path, Any]] = []

            def distribute_batch(batch: Sequence[Sequence[Any]]) -> None:
                nonlocal task_number
                task_number += 1
                partial_path = run_directory / (
                    f".{table_component.lower()}_{uuid4().hex}_"
                    f"task_{task_number:06d}.partial"
                )
                chunk_paths.append(partial_path)
                pending.append(
                    (
                        task_number,
                        partial_path,
                        executor.submit(write_chunk, batch, partial_path),
                    )
                )
                if len(pending) >= effective_workers:
                    completed_task, completed_path, future = pending.pop(0)
                    rows_written, serialization_seconds, csv_write_seconds = (
                        future.result()
                    )
                    chunk_results.append(
                        (
                            completed_task,
                            completed_path,
                            rows_written,
                            serialization_seconds,
                            csv_write_seconds,
                        )
                    )

            extraction = stream_source_batches(
                source_connection,
                build_full_table_query(mapping_rows),
                fetch_size,
                distribute_batch,
            )
            for completed_task, completed_path, future in pending:
                rows_written, serialization_seconds, csv_write_seconds = (
                    future.result()
                )
                chunk_results.append(
                    (
                        completed_task,
                        completed_path,
                        rows_written,
                        serialization_seconds,
                        csv_write_seconds,
                    )
                )

        if extraction is None:
            raise RuntimeError("4D extraction did not complete")
        extracted_count = sum(result[2] for result in chunk_results)
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

        for _, chunk_path, expected_rows, _, _ in chunk_results:
            chunk_rows = 0
            with chunk_path.open("r", encoding="utf-8", newline="") as source:
                reader = csv.reader(source, strict=True)
                for row in reader:
                    if len(row) != len(target_columns):
                        raise ValueError(
                            f"CSV task file {chunk_path} has a row with "
                            f"{len(row)} fields; expected {len(target_columns)}"
                        )
                    chunk_rows += 1
            if chunk_rows != expected_rows:
                raise RuntimeError(
                    f"CSV task file row count mismatch: "
                    f"extracted={expected_rows}, csv={chunk_rows}"
                )

        with final_partial.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output, dialect="excel", lineterminator="\n")
            writer.writerow(target_columns)
            for _, chunk_path, _, _, _ in sorted(chunk_results):
                with chunk_path.open("r", encoding="utf-8", newline="") as source:
                    reader = csv.reader(source, strict=True)
                    for row in reader:
                        writer.writerow(row)
            output.flush()
        final_rows = _validate_csv_file(final_partial, target_columns)
        if final_rows != extracted_count:
            raise RuntimeError(
                f"Final CSV row count mismatch for {source_table}: "
                f"extracted={extracted_count}, csv={final_rows}"
            )
        if final_path.exists():
            raise FileExistsError(
                f"Refusing to overwrite completed CSV export: {final_path}"
            )
        for chunk_path in chunk_paths:
            chunk_path.unlink(missing_ok=True)
        final_partial.rename(final_path)
    except BaseException:
        for path in (*chunk_paths, final_partial):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logger.exception("Could not remove incomplete CSV file %s", path)
        raise

    return ParallelCsvExportResult(
        run_id=run_component,
        source_table=source_table,
        target_table=target_table,
        file_paths=(final_path,),
        row_count=extracted_count,
        file_size_bytes=final_path.stat().st_size,
        worker_count=effective_workers,
        fetch_size=fetch_size,
        source_count_before=source_count_before,
        source_count_after=source_count_after,
        query_seconds=extraction.query_seconds,
        fetch_seconds=extraction.fetch_seconds,
        serialization_seconds=sum(result[3] for result in chunk_results),
        csv_write_seconds=sum(result[4] for result in chunk_results),
        total_seconds=perf_counter() - started_at,
    )


def _quote_4d_identifier(identifier: str) -> str:
    value = identifier.strip()
    if not value or "\x00" in value:
        raise ValueError(f"Invalid 4D identifier: {identifier!r}")
    return "[" + value.replace("]", "]]") + "]"


def _primary_key_source_column(
    source_rows: Sequence[dict[str, str]],
) -> str | None:
    primary_keys = [
        row
        for row in source_rows
        if re.match(
            r"^\s*['\"]?\s*(?:PK\b|PRIMARY\s+KEY\b)",
            row.get("constraint", ""),
            re.IGNORECASE,
        )
    ]
    if not primary_keys:
        return None
    for primary_key in primary_keys:
        source_type = primary_key.get("source_data_type", "").strip().upper()
        if re.fullmatch(
            r"(?:TINYINT|SMALLINT|INT|INTEGER|INT16|INT32|INT64|BIGINT|LONGINT|"
            r"UINT16|UINT32|UINT64|NUMBER|NUMERIC|DECIMAL)"
            r"(?:\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?",
            source_type,
        ):
            return primary_key["source_column"].strip()
    return None


def _integer_key(value: Any, label: str) -> int:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"4D returned an invalid primary key {label}: {value!r}")
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"4D returned a non-numeric primary key {label}: {value!r}") from exc
    if not decimal_value.is_finite() or decimal_value != decimal_value.to_integral_value():
        raise ValueError(
            f"Parallel CSV extraction requires integral primary keys; "
            f"{label} value is {value!r}"
        )
    return int(decimal_value)


def _query_key_ranges(
    connection: Any,
    source_table: str,
    primary_key: str,
    worker_count: int,
) -> tuple[int, list[_KeyRange]]:
    table_sql = _quote_4d_identifier(source_table)
    key_sql = _quote_4d_identifier(primary_key)
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_sql}")
        result = cursor.fetchone()
        if not result or result[0] is None:
            raise RuntimeError(f"4D did not return a row count for {source_table}")
        source_count = int(result[0])
        if source_count == 0:
            return 0, [_KeyRange(0, None, 0)]

        cursor.execute(f"SELECT MIN({key_sql}) FROM {table_sql}")
        minimum_result = cursor.fetchone()
        cursor.execute(f"SELECT MAX({key_sql}) FROM {table_sql}")
        maximum_result = cursor.fetchone()
        if (
            not minimum_result
            or minimum_result[0] is None
            or not maximum_result
            or maximum_result[0] is None
        ):
            raise RuntimeError(
                f"4D returned NULL primary-key bounds for nonempty table {source_table}"
            )
        minimum = _integer_key(minimum_result[0], "minimum")
        maximum = _integer_key(maximum_result[0], "maximum")
        if maximum < minimum:
            raise RuntimeError(
                f"4D returned reversed primary-key bounds for {source_table}: "
                f"{minimum}..{maximum}"
            )

        boundaries: list[int] = []
        for worker_index in range(1, min(worker_count, source_count)):
            target_rows_before_boundary = (
                source_count * worker_index // min(worker_count, source_count)
            ) + 1
            lower = minimum
            upper = maximum
            while lower < upper:
                midpoint = (lower + upper) // 2
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table_sql} "
                    f"WHERE {key_sql} <= {midpoint}"
                )
                midpoint_result = cursor.fetchone()
                if not midpoint_result or midpoint_result[0] is None:
                    raise RuntimeError(
                        f"4D did not return a key-distribution count for "
                        f"{source_table} at {midpoint}"
                    )
                if int(midpoint_result[0]) < target_rows_before_boundary:
                    lower = midpoint + 1
                else:
                    upper = midpoint
            if lower > minimum and (
                not boundaries or lower > boundaries[-1]
            ):
                boundaries.append(lower)

        ranges: list[tuple[int, int | None]] = []
        range_lower = minimum
        for boundary in boundaries:
            ranges.append((range_lower, boundary))
            range_lower = boundary
        ranges.append((range_lower, None))

        expected_counts: list[_KeyRange] = []
        for lower, upper in ranges:
            predicate = f"{key_sql} >= {lower}"
            if upper is not None:
                predicate += f" AND {key_sql} < {upper}"
            cursor.execute(f"SELECT COUNT(*) FROM {table_sql} WHERE {predicate}")
            count_result = cursor.fetchone()
            if not count_result or count_result[0] is None:
                raise RuntimeError(
                    f"4D did not return a row count for {source_table} key range "
                    f"{lower}..{upper}"
                )
            expected_counts.append(_KeyRange(lower, upper, int(count_result[0])))

        if sum(item.expected_rows for item in expected_counts) != source_count:
            raise RuntimeError(
                f"4D primary-key ranges do not reconcile for {source_table}: "
                f"source={source_count}, ranges="
                f"{sum(item.expected_rows for item in expected_counts)}"
            )
        return source_count, expected_counts
    finally:
        cursor.close()


def export_table_to_csv_parallel(
    source_connection: Any,
    mapping_rows: Sequence[dict[str, str]],
    export_root: str | Path,
    run_id: str,
    connection_factory: Any,
    fetch_size: int | None = None,
    worker_count: int = 3,
) -> ParallelCsvExportResult:
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
    primary_key = _primary_key_source_column(source_rows)
    if primary_key is None:
        return _export_without_primary_key(
            source_connection,
            mapping_rows,
            export_root,
            run_id,
            fetch_size,
            worker_count,
            started_at,
        )

    run_component = _safe_path_component(run_id, "Run ID")
    table_component = _safe_path_component(target_table, "Target table")
    run_directory = Path(export_root) / table_component / run_component
    run_directory.mkdir(parents=True, exist_ok=True)

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
    final_path = run_directory / f"{table_component.lower()}.csv"
    if final_path.exists():
        raise FileExistsError(
            f"Refusing to overwrite completed CSV export: {final_path}"
        )

    query = build_full_table_query(mapping_rows)
    source_indexes = {column: index for index, column in enumerate(source_columns)}
    key_index = source_indexes[primary_key]
    row_mapping = [
        (source_indexes[row["source_column"].strip()], target_index)
        for target_index, row in enumerate(source_rows)
    ]
    partial_paths = tuple(
        run_directory / f".{table_component.lower()}_{uuid4().hex}_{index + 1:03d}.partial"
        for index in range(len(key_ranges))
    )
    connection_lock = Lock()
    worker_connections: list[Any] = []

    def extract_worker(
        worker_number: int,
        key_range: _KeyRange,
        partial_path: Path,
    ) -> CsvWorkerResult:
        key_sql = _quote_4d_identifier(primary_key)
        predicate = f"{key_sql} >= {key_range.lower}"
        if key_range.upper is not None:
            predicate += f" AND {key_sql} < {key_range.upper}"
        worker_query = f"{query} WHERE {predicate} ORDER BY {key_sql}"

        worker_connection = connection_factory()
        with connection_lock:
            if worker_connection is source_connection or any(
                worker_connection is existing for existing in worker_connections
            ):
                raise RuntimeError(
                    "Each CSV worker must use a distinct 4D connection"
                )
            worker_connections.append(worker_connection)
        cursor = None
        query_seconds = 0.0
        fetch_seconds = 0.0
        serialization_seconds = 0.0
        csv_write_seconds = 0.0
        row_count = 0
        try:
            cursor = worker_connection.cursor()
            query_started = perf_counter()
            cursor.execute(worker_query)
            query_seconds = perf_counter() - query_started
            with partial_path.open("w", encoding="utf-8", newline="") as output:
                writer = csv.writer(output, dialect="excel", lineterminator="\n")
                writer.writerow(target_columns)
                while True:
                    fetch_started = perf_counter()
                    batch = cursor.fetchmany(resolved_fetch_size)
                    fetch_seconds += perf_counter() - fetch_started
                    if not batch:
                        break
                    serialize_started = perf_counter()
                    serialized_rows = []
                    for row in batch:
                        if len(row) != len(source_columns):
                            raise ValueError(
                                f"4D worker {worker_number} returned {len(row)} "
                                f"columns; expected {len(source_columns)}"
                            )
                        key = _integer_key(row[key_index], "row")
                        if key < key_range.lower or (
                            key_range.upper is not None and key >= key_range.upper
                        ):
                            raise RuntimeError(
                                f"4D returned primary key {key} outside worker "
                                f"{worker_number} range "
                                f"[{key_range.lower}, {key_range.upper})"
                            )
                        values = []
                        for source_index, target_index in row_mapping:
                            value = _raw_value_as_text(row[source_index])
                            if value == CSV_NULL_MARKER:
                                raise ValueError(
                                    f"Source value for target column "
                                    f"{target_columns[target_index]} "
                                    "collides with the reserved CSV NULL marker"
                                )
                            values.append(CSV_NULL_MARKER if value is None else value)
                        serialized_rows.append(values)
                    serialization_seconds += perf_counter() - serialize_started
                    write_started = perf_counter()
                    writer.writerows(serialized_rows)
                    csv_write_seconds += perf_counter() - write_started
                    row_count += len(batch)
                output.flush()
            if row_count != key_range.expected_rows:
                raise RuntimeError(
                    f"4D worker {worker_number} extracted {row_count} rows, "
                    f"expected {key_range.expected_rows}"
                )
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                finally:
                    worker_connection.close()
            else:
                worker_connection.close()
        return CsvWorkerResult(
            worker_number=worker_number,
            file_path=partial_path,
            row_count=row_count,
            expected_rows=key_range.expected_rows,
            query_seconds=query_seconds,
            fetch_seconds=fetch_seconds,
            serialization_seconds=serialization_seconds,
            csv_write_seconds=csv_write_seconds,
        )

    final_partial = run_directory / f".{table_component.lower()}.{uuid4().hex}.partial"
    try:
        with ThreadPoolExecutor(max_workers=len(key_ranges)) as executor:
            futures = [
                executor.submit(extract_worker, index + 1, key_range, partial_path)
                for index, (key_range, partial_path) in enumerate(
                    zip(key_ranges, partial_paths, strict=True)
                )
            ]
            worker_results = [future.result() for future in futures]

        extracted_count = sum(result.row_count for result in worker_results)
        if extracted_count != source_count_before:
            raise RuntimeError(
                f"Parallel extraction row count mismatch for {source_table}: "
                f"source={source_count_before}, extracted={extracted_count}"
            )
        source_count_after = count_source_rows(source_connection, source_table)
        if source_count_after != extracted_count:
            raise RuntimeError(
                f"4D row count changed during parallel extraction for {source_table}: "
                f"extracted={extracted_count}, count after={source_count_after}"
            )

        for result in worker_results:
            file_count = _validate_csv_file(result.file_path, target_columns)
            if file_count != result.row_count:
                raise RuntimeError(
                    f"CSV worker {result.worker_number} file row count mismatch: "
                    f"extracted={result.row_count}, csv={file_count}"
                )
        with final_partial.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output, dialect="excel", lineterminator="\n")
            writer.writerow(target_columns)
            for result in worker_results:
                with result.file_path.open(
                    "r", encoding="utf-8-sig", newline=""
                ) as source:
                    reader = csv.reader(source, strict=True)
                    next(reader)
                    writer.writerows(reader)
            output.flush()
        merged_count = _validate_csv_file(final_partial, target_columns)
        if merged_count != extracted_count:
            raise RuntimeError(
                f"Final CSV row count mismatch for {source_table}: "
                f"extracted={extracted_count}, csv={merged_count}"
            )
        if final_path.exists():
            raise FileExistsError(
                f"Refusing to overwrite completed CSV export: {final_path}"
            )
        for partial_path in partial_paths:
            partial_path.unlink(missing_ok=True)
        final_partial.rename(final_path)
    except BaseException:
        for path in (*partial_paths, final_partial):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logger.exception("Could not remove incomplete CSV file %s", path)
        raise

    return ParallelCsvExportResult(
        run_id=run_component,
        source_table=source_table,
        target_table=target_table,
        file_paths=(final_path,),
        row_count=sum(result.row_count for result in worker_results),
        file_size_bytes=final_path.stat().st_size,
        worker_count=len(key_ranges),
        fetch_size=resolved_fetch_size,
        source_count_before=source_count_before,
        source_count_after=source_count_after,
        query_seconds=sum(result.query_seconds for result in worker_results),
        fetch_seconds=sum(result.fetch_seconds for result in worker_results),
        serialization_seconds=sum(
            result.serialization_seconds for result in worker_results
        ),
        csv_write_seconds=sum(result.csv_write_seconds for result in worker_results),
        total_seconds=perf_counter() - started_at,
    )


def _validate_csv_file(
    file_path: Path,
    expected_columns: Sequence[str],
) -> int:
    with file_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source, strict=True)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise ValueError(f"CSV export is empty: {file_path}") from exc
        if [column.casefold() for column in header] != [
            column.casefold() for column in expected_columns
        ]:
            raise ValueError(
                f"CSV columns do not match the mapping: expected "
                f"{list(expected_columns)}, found {header}"
            )
        rows = 0
        for row in reader:
            if len(row) != len(expected_columns):
                raise ValueError(
                    f"CSV row {rows + 2} has {len(row)} fields; "
                    f"expected {len(expected_columns)}"
                )
            rows += 1
    return rows


def export_table_to_csv(
    source_connection: Any,
    mapping_rows: Sequence[dict[str, str]],
    export_root: str | Path,
    run_id: str,
    fetch_size: int = 50_000,
) -> CsvExportResult:
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
    final_path = run_directory / f"{table_component.lower()}.csv"
    if final_path.exists():
        raise FileExistsError(f"Refusing to overwrite completed CSV export: {final_path}")

    query = build_full_table_query(mapping_rows)
    source_count_before = count_source_rows(source_connection, source_table)
    source_indexes = {column: index for index, column in enumerate(source_columns)}
    row_mapping = [
        (source_indexes[row["source_column"].strip()], target_index)
        for target_index, row in enumerate(source_rows)
    ]
    partial_path = run_directory / f".{table_component.lower()}.{uuid4().hex}.partial"
    serialization_seconds = 0.0
    csv_write_seconds = 0.0
    writer: Any | None = None

    def write_batch(batch: Sequence[Sequence[Any]]) -> None:
        nonlocal serialization_seconds, csv_write_seconds
        started = perf_counter()
        serialized_rows: list[list[str]] = []
        for row in batch:
            if len(row) != len(source_columns):
                raise ValueError(
                    f"4D returned {len(row)} column(s) for {source_table}; "
                    f"expected {len(source_columns)}"
                )
            serialized: list[str] = []
            for source_index, target_index in row_mapping:
                value = _raw_value_as_text(row[source_index])
                if value == CSV_NULL_MARKER:
                    raise ValueError(
                        f"Source value in {source_table}.{target_columns[target_index]} "
                        f"collides with the reserved CSV NULL marker"
                    )
                serialized.append(CSV_NULL_MARKER if value is None else value)
            serialized_rows.append(serialized)
        serialization_seconds += perf_counter() - started

        started = perf_counter()
        if writer is None:
            raise RuntimeError("CSV writer was not initialized")
        writer.writerows(serialized_rows)
        csv_write_seconds += perf_counter() - started

    try:
        with partial_path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output, dialect="excel", lineterminator="\n")
            writer.writerow(target_columns)
            extraction: SourceExtractionStats = stream_source_batches(
                source_connection,
                query,
                fetch_size,
                write_batch,
            )
            output.flush()

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
        csv_rows = _validate_csv_file(partial_path, target_columns)
        if csv_rows != extraction.rows_extracted:
            raise RuntimeError(
                f"CSV row count mismatch for {source_table}: "
                f"extracted={extraction.rows_extracted}, csv={csv_rows}"
            )
        if final_path.exists():
            raise FileExistsError(
                f"Refusing to overwrite completed CSV export: {final_path}"
            )
        partial_path.rename(final_path)
    except BaseException:
        try:
            partial_path.unlink(missing_ok=True)
        except OSError:
            logger.exception("Could not remove incomplete CSV file %s", partial_path)
        raise

    return CsvExportResult(
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
        csv_write_seconds=csv_write_seconds,
        total_seconds=perf_counter() - started_at,
    )
