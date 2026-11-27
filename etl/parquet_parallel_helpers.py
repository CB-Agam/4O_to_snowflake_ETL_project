from __future__ import annotations

import math
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence


MAX_AUTO_FETCH_SIZE = 100_000


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
            f"Parallel Parquet extraction requires integral primary keys; "
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
            if lower > minimum and (not boundaries or lower > boundaries[-1]):
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
