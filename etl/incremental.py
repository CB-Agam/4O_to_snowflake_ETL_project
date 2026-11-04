from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import time
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Sequence
from uuid import uuid4

import pandas as pd

from etl.load import build_source_query, load_dataframe_to_snowflake

logger = logging.getLogger(__name__)

_TIMESTAMP_TYPE_NAMES = {"TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "DATETIME"}
_BATCH_LOG_NAME = "ETL_INCREMENTAL_BATCH_LOG"
_STATE_NAME = "ETL_INCREMENTAL_TABLE_STATE"
_ROW_STATE_NAME = "ETL_SOURCE_ROW_STATE"
_LOCK_TTL_HOURS = 24
_KEY_FETCH_SIZE = 250


def _quote_identifier(identifier: str) -> str:
    if not identifier:
        raise ValueError("SQL identifiers cannot be blank")
    return '"' + identifier.replace('"', '""') + '"'


def _qualified_name(database: str, schema: str, table_name: str) -> str:
    return ".".join(
        (_quote_identifier(database), _quote_identifier(schema), _quote_identifier(table_name))
    )


def _table_columns(rows: Sequence[dict[str, str]]) -> list[str]:
    columns = [
        str(row["target_column"]).strip()
        for row in rows
        if str(row.get("target_column", "")).strip()
    ]
    if not columns:
        raise ValueError("Incremental loading requires mapped target columns")
    folded = [column.casefold() for column in columns]
    if len(folded) != len(set(folded)):
        raise ValueError("Target-column mappings contain duplicate column names")
    return columns


def _source_target_columns(rows: Sequence[dict[str, str]]) -> list[str]:
    columns = [
        str(row["target_column"]).strip()
        for row in rows
        if str(row.get("source_table", "")).strip()
        and str(row.get("source_column", "")).strip()
        and str(row.get("target_column", "")).strip()
    ]
    if not columns:
        raise ValueError("Incremental loading requires complete source-to-target mappings")
    folded = [column.casefold() for column in columns]
    if len(folded) != len(set(folded)):
        raise ValueError("Source mappings contain duplicate target column names")
    return columns


def _primary_key_columns(rows: Sequence[dict[str, str]]) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if re.search(
            r"(^|[^A-Za-z0-9])(?:PK|PRIMARY\s+KEY)([^A-Za-z0-9]|$)",
            row.get("constraint", ""),
            re.I,
        )
    ]


def _change_timestamp_columns(
    rows: Sequence[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    created = []
    modified = []
    for row in rows:
        source_column = row.get("source_column", "").strip()
        source_type = row.get("source_data_type", "").strip().upper().split("(", 1)[0]
        if (
            not row.get("source_table", "").strip()
            or not source_column
            or not row.get("target_column", "").strip()
            or source_type not in _TIMESTAMP_TYPE_NAMES
        ):
            continue
        normalized_name = source_column.casefold()
        if "created" in normalized_name:
            created.append(row)
        if "modified" in normalized_name:
            modified.append(row)
    return created, modified


def _valid_change_timestamp_columns(
    rows: Sequence[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    created, modified = _change_timestamp_columns(rows)
    if len(created) > 1 or len(modified) != 1:
        return [], []
    return created, modified


def select_change_strategy(rows: Sequence[dict[str, str]]) -> dict[str, Any]:
    """Choose timestamp/key mode only when the workbook identifies both unambiguously."""
    keys = _primary_key_columns(rows)
    created, modified = _valid_change_timestamp_columns(rows)
    if len(keys) == 1 and len(modified) == 1:
        return {
            "strategy": "KEY_TIMESTAMP",
            "key_source_column": keys[0]["source_column"].strip(),
            "key_target_column": keys[0]["target_column"].strip(),
            "key_target_columns": [keys[0]["target_column"].strip()],
            "created_source_column": (
                created[0]["source_column"].strip() if created else None
            ),
            "created_target_column": (
                created[0]["target_column"].strip() if created else None
            ),
            "modified_source_column": modified[0]["source_column"].strip(),
            "modified_target_column": modified[0]["target_column"].strip(),
        }
    if keys:
        return {
            "strategy": "KEY_HASH",
            "key_source_column": None,
            "key_target_column": None,
            "key_target_columns": [
                row["target_column"].strip() for row in keys
            ],
            "created_source_column": None,
            "created_target_column": None,
            "modified_source_column": None,
            "modified_target_column": None,
        }
    return {
        "strategy": "ROW_HASH",
        "key_source_column": None,
        "key_target_column": None,
        "key_target_columns": [],
        "created_source_column": None,
        "created_target_column": None,
        "modified_source_column": None,
        "modified_target_column": None,
    }


def _json_value(value: Any) -> Any:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bytes):
        return {"__bytes__": value.hex()}
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_value(value.item())
        except (TypeError, ValueError):
            pass
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _row_hash(values: Sequence[Any]) -> str:
    return hashlib.sha256(_canonical_json(list(values)).encode("utf-8")).hexdigest()


def _natural_key_hash(values: Sequence[Any]) -> str:
    for value in values:
        _source_key(value)
    return _row_hash(values)


def _source_key(value: Any) -> str:
    if value is None or pd.isna(value):
        raise ValueError("A declared 4D primary key contains a NULL value")
    return _canonical_json(value)


def _normalize_timestamp(value: Any, key: str) -> datetime:
    if value is None or pd.isna(value):
        raise ValueError(f"Source modification timestamp {key!r} contains NULL")
    if not isinstance(value, (datetime, date, pd.Timestamp)):
        raise ValueError(
            f"Source modification field {key!r} returned non-timestamp value "
            f"{value!r} ({type(value).__name__})"
        )
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("UTC").tz_localize(None)
    return stamp.to_pydatetime()


def _normalize_optional_timestamp(value: Any, key: str) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    return _normalize_timestamp(value, key)


def _source_projection_query(source_table: str, columns: Sequence[str]) -> str:
    return build_source_query(source_table, columns)


def _hash_state_rows(
    dataframe: pd.DataFrame,
    table_name: str,
    target_columns: Sequence[str],
    timestamp_column: str | None = None,
    created_column: str | None = None,
) -> list[tuple[str, str, str | None, datetime | None, datetime | None]]:
    state: list[tuple[str, str, str | None, datetime | None, datetime | None]] = []
    occurrence_counts: Counter[str] = Counter()
    for record in dataframe.itertuples(index=False, name=None):
        values = dict(zip(target_columns, record, strict=True))
        row_hash = _row_hash(record)
        occurrence_counts[row_hash] += 1
        modified_at = (
            _normalize_optional_timestamp(values[timestamp_column], timestamp_column)
            if timestamp_column
            else None
        )
        created_at = (
            _normalize_optional_timestamp(values[created_column], created_column)
            if created_column
            else None
        )
        state.append(
            (
                table_name,
                f"{row_hash}:{occurrence_counts[row_hash]}",
                row_hash,
                modified_at,
                created_at,
            )
        )
    return state


def _rows_as_state(
    dataframe: pd.DataFrame,
    table_name: str,
    target_columns: Sequence[str],
    strategy: dict[str, Any],
) -> list[tuple[str, str, str | None, datetime | None, datetime | None]]:
    state: list[tuple[str, str, str | None, datetime | None, datetime | None]] = []
    if strategy["strategy"] == "KEY_TIMESTAMP":
        key_column = strategy["key_target_column"]
        timestamp_column = strategy["modified_target_column"]
        created_column = strategy["created_target_column"]
        keys: set[str] = set()
        for record in dataframe.itertuples(index=False, name=None):
            values = dict(zip(target_columns, record, strict=True))
            key = _source_key(values[key_column])
            if key in keys:
                logger.warning(
                    "Source key %r is not unique in %s; falling back to duplicate-safe row-hash state.",
                    key_column,
                    table_name,
                )
                return _hash_state_rows(
                    dataframe,
                    table_name,
                    target_columns,
                    timestamp_column,
                    created_column,
                )
            keys.add(key)
            modified_at = _normalize_optional_timestamp(
                values[timestamp_column],
                timestamp_column,
            )
            created_at = (
                _normalize_optional_timestamp(values[created_column], created_column)
                if created_column
                else None
            )
            state.append((table_name, key, _row_hash(record), modified_at, created_at))
        return state

    if strategy["strategy"] == "KEY_HASH":
        key_columns = strategy["key_target_columns"]
        keys: set[str] = set()
        for record in dataframe.itertuples(index=False, name=None):
            values = dict(zip(target_columns, record, strict=True))
            key = _natural_key_hash(tuple(values[column] for column in key_columns))
            if key in keys:
                logger.warning(
                    "Declared key columns %r are not unique in %s; falling back to duplicate-safe row-hash state.",
                    key_columns,
                    table_name,
                )
                return _hash_state_rows(dataframe, table_name, target_columns)
            keys.add(key)
            state.append((table_name, key, _row_hash(record), None, None))
        return state

    return _hash_state_rows(dataframe, table_name, target_columns)


def _timestamp_watermark(
    state_rows: Sequence[
        tuple[str, str, str | None, datetime | None]
        | tuple[str, str, str | None, datetime | None, datetime | None]
    ],
) -> datetime | None:
    return max(
        (
            timestamp
            for row in state_rows
            for timestamp in row[3:]
            if timestamp is not None
        ),
        default=None,
    )


def _saved_timestamps(
    state_value: tuple[str | None, datetime | None]
    | tuple[str | None, datetime | None, datetime | None],
) -> tuple[str | None, datetime | None, datetime | None]:
    if len(state_value) == 2:
        row_hash, modified_at = state_value
        return row_hash, modified_at, None
    row_hash, modified_at, created_at = state_value
    return row_hash, modified_at, created_at


def _needs_created_timestamp_refresh(
    old_state: dict[
        str,
        tuple[str | None, datetime | None]
        | tuple[str | None, datetime | None, datetime | None],
    ],
    new_state: Sequence[
        tuple[str, str, str | None, datetime | None]
        | tuple[str, str, str | None, datetime | None, datetime | None]
    ],
) -> bool:
    for row in new_state:
        if len(row) == 5:
            _, source_key, _, _, created_at = row
            old_created = _saved_timestamps(
                old_state.get(source_key, (None, None))
            )[2]
            if old_created is None and created_at is not None:
                return True
    return False


def _validate_timestamp_metadata(
    fourd_connection,
    source_table: str,
    key_column: str,
    modified_column: str,
) -> None:
    cursor = fourd_connection.cursor()
    try:
        columns = list(cursor.columns(table=source_table))
    finally:
        cursor.close()

    source_columns = {
        str(column.column_name).strip().casefold(): column
        for column in columns
        if getattr(column, "column_name", None)
    }
    for name in (key_column, modified_column):
        if name.casefold() not in source_columns:
            raise ValueError(
                f"4D metadata for {source_table!r} does not contain configured column {name!r}"
            )

    type_name = str(
        getattr(source_columns[modified_column.casefold()], "type_name", "")
    ).strip().upper()
    if type_name.split("(", 1)[0] not in _TIMESTAMP_TYPE_NAMES:
        raise ValueError(
            f"4D modification column {modified_column!r} has ODBC type {type_name!r}; "
            "a reliable timestamp type is required"
        )


def _fetch_full_rows_for_keys(
    fourd_connection,
    source_table: str,
    source_columns: Sequence[str],
    key_column: str,
    keys: Sequence[Any],
    key_target_column: str,
    target_columns: Sequence[str],
) -> pd.DataFrame:
    if not keys:
        return pd.DataFrame(columns=target_columns)
    source_query = _source_projection_query(source_table, source_columns)
    chunks: list[pd.DataFrame] = []
    for start in range(0, len(keys), _KEY_FETCH_SIZE):
        key_chunk = keys[start : start + _KEY_FETCH_SIZE]
        query = source_query + " WHERE " + " OR ".join(
            f"[{key_column.replace(']', ']]')}] = ?" for _ in key_chunk
        )
        chunk = pd.read_sql(query, fourd_connection, params=list(key_chunk))
        if not chunk.empty:
            chunk.columns = target_columns
            chunks.append(chunk)
    if not chunks:
        return pd.DataFrame(columns=target_columns)
    result = pd.concat(chunks, ignore_index=True)
    keys_in_result = [_source_key(value) for value in result[key_target_column]]
    if len(keys_in_result) != len(set(keys_in_result)):
        raise ValueError(
            f"4D returned duplicate rows for declared key {key_target_column!r}"
        )
    return result


def _discover_timestamp_changes(
    fourd_connection,
    table_name: str,
    rows: Sequence[dict[str, str]],
    state_by_key: dict[
        str,
        tuple[str | None, datetime | None]
        | tuple[str | None, datetime | None, datetime | None],
    ],
    watermark: datetime | None,
    overlap_seconds: int,
) -> tuple[
    pd.DataFrame,
    list[tuple[str, str, str | None, datetime | None, datetime | None]],
    int,
    int,
    int,
    int,
]:
    strategy = select_change_strategy(rows)
    if strategy["strategy"] != "KEY_TIMESTAMP":
        raise ValueError("Timestamp/key discovery called for a table without that strategy")

    source_rows = [
        row for row in rows
        if row.get("source_table", "").strip()
        and row.get("source_column", "").strip()
        and row.get("target_column", "").strip()
    ]
    source_tables = {row["source_table"].strip() for row in source_rows}
    if len(source_tables) != 1:
        raise ValueError(f"Target {table_name} must map to exactly one 4D source table")
    source_table = next(iter(source_tables))
    source_columns = list(dict.fromkeys(row["source_column"].strip() for row in source_rows))
    target_columns = [row["target_column"].strip() for row in source_rows]
    if len(source_columns) != len(target_columns):
        raise ValueError(f"Duplicate source-column mappings for {table_name}")
    key_source = strategy["key_source_column"]
    key_target = strategy["key_target_column"]
    created_source = strategy["created_source_column"]
    created_target = strategy["created_target_column"]
    modified_source = strategy["modified_source_column"]
    modified_target = strategy["modified_target_column"]
    required_scan_columns = [
        key_source,
        *([created_source] if created_source else []),
        modified_source,
    ]
    if any(column not in source_columns for column in required_scan_columns):
        raise ValueError(
            "Key and created/modified timestamps must be included in the source mapping"
        )

    for timestamp_column in filter(None, (created_source, modified_source)):
        _validate_timestamp_metadata(
            fourd_connection,
            source_table,
            key_source,
            timestamp_column,
        )
    scan_query = build_source_query(source_table, required_scan_columns)
    scan_df = pd.read_sql(scan_query, fourd_connection)
    if len(scan_df.columns) != len(required_scan_columns):
        raise ValueError(
            f"4D key/timestamp scan returned unexpected columns for {source_table}"
        )

    source_key_values = scan_df.iloc[:, 0].tolist()
    timestamp_values = [
        scan_df.iloc[:, column_index].tolist()
        for column_index in range(1, len(required_scan_columns))
    ]
    keyed_timestamps: dict[
        str,
        tuple[Any, datetime | None, datetime | None],
    ] = {}
    for row_index, raw_key in enumerate(source_key_values):
        key = _source_key(raw_key)
        if key in keyed_timestamps:
            logger.warning(
                "Declared 4D key %r is not unique in source table %s; falling back to duplicate-safe row-hash incremental state.",
                key_source,
                source_table,
            )
            changed_df, new_state, source_count, deleted_count = _discover_hash_changes(
                fourd_connection,
                table_name,
                rows,
                state_by_key,
            )
            return (
                changed_df,
                new_state,
                source_count,
                0,
                0,
                deleted_count,
            )
        if created_source:
            created_raw = timestamp_values[0][row_index]
            modified_raw = timestamp_values[1][row_index]
        else:
            created_raw = None
            modified_raw = timestamp_values[0][row_index]
        keyed_timestamps[key] = (
            raw_key,
            _normalize_optional_timestamp(modified_raw, modified_source),
            (
                _normalize_optional_timestamp(created_raw, created_source)
                if created_source
                else None
            ),
        )

    threshold = (
        watermark - timedelta(seconds=overlap_seconds)
        if watermark is not None
        else None
    )
    keys_to_fetch = []
    for key, (raw_key, modified_at, created_at) in keyed_timestamps.items():
        old_hash, old_modified, old_created = _saved_timestamps(
            state_by_key.get(key, (None, None))
        )
        is_new_or_changed_timestamp = (
            old_hash is None
            or modified_at != old_modified
            or created_at != old_created
        )
        timestamps = [modified_at]
        if created_source:
            timestamps.append(created_at)
        is_in_overlap = (
            threshold is None
            or any(timestamp is None or timestamp >= threshold for timestamp in timestamps)
        )
        if is_new_or_changed_timestamp or is_in_overlap:
            keys_to_fetch.append(raw_key)

    changed_rows = _fetch_full_rows_for_keys(
        fourd_connection,
        source_table,
        source_columns,
        key_source,
        keys_to_fetch,
        key_target,
        target_columns,
    )
    changed_by_key: dict[str, tuple[str, datetime | None, datetime | None]] = {}
    changed_keys: set[str] = set()
    updated_count = 0
    inserted_count = 0
    for record in changed_rows.itertuples(index=False, name=None):
        values = dict(zip(target_columns, record, strict=True))
        key = _source_key(values[key_target])
        source_modified = _normalize_optional_timestamp(
            values[modified_target],
            modified_target,
        )
        source_created = (
            _normalize_optional_timestamp(values[created_target], created_target)
            if created_target
            else None
        )
        row_hash = _row_hash(record)
        old_hash, _, _ = _saved_timestamps(state_by_key.get(key, (None, None)))
        if (
            old_hash != row_hash
        ):
            changed_keys.add(key)
            if old_hash is None:
                inserted_count += 1
            else:
                updated_count += 1
        changed_by_key[key] = (row_hash, source_modified, source_created)

    # Preserve hashes for unchanged source keys; only candidate rows need full-row reads.
    new_state = []
    for key, (_, modified_at, created_at) in keyed_timestamps.items():
        old_hash, old_modified, old_created = _saved_timestamps(
            state_by_key.get(key, (None, None))
        )
        new_hash, _, _ = changed_by_key.get(
            key,
            (old_hash, modified_at, created_at),
        )
        if new_hash is None:
            raise RuntimeError(f"Missing saved row hash for unchanged key in {table_name}")
        new_state.append((table_name, key, new_hash, modified_at, created_at))

    if changed_rows.empty:
        changed_rows = pd.DataFrame(columns=target_columns)
    else:
        changed_rows = changed_rows[
            changed_rows[key_target].map(lambda value: _source_key(value) in changed_keys)
        ].reset_index(drop=True)

    deleted_count = len(set(state_by_key) - set(keyed_timestamps))
    return (
        changed_rows,
        new_state,
        len(keyed_timestamps),
        inserted_count,
        updated_count,
        deleted_count,
    )


def _discover_hash_changes(
    fourd_connection,
    table_name: str,
    rows: Sequence[dict[str, str]],
    old_state: dict[str, tuple[str | None, datetime | None]],
) -> tuple[
    pd.DataFrame,
    list[tuple[str, str, str | None, datetime | None, datetime | None]],
    int,
    int,
]:
    source_rows = [
        row for row in rows
        if row.get("source_table", "").strip()
        and row.get("source_column", "").strip()
        and row.get("target_column", "").strip()
    ]
    if not source_rows:
        raise ValueError(f"No complete source mappings for {table_name}")
    source_tables = {row["source_table"].strip() for row in source_rows}
    if len(source_tables) != 1:
        raise ValueError(f"Target {table_name} must map to exactly one 4D source table")
    source_table = next(iter(source_tables))
    source_columns = list(dict.fromkeys(row["source_column"].strip() for row in source_rows))
    target_columns = [row["target_column"].strip() for row in source_rows]
    source_query = _source_projection_query(source_table, source_columns)
    full_df = pd.read_sql(source_query, fourd_connection)
    if len(full_df.columns) != len(target_columns):
        raise ValueError(f"4D extraction returned unexpected columns for {source_table}")
    full_df.columns = target_columns

    occurrence_counts: Counter[str] = Counter()
    changed_indexes = []
    new_state = []
    for index, record in enumerate(full_df.itertuples(index=False, name=None)):
        row_hash = _row_hash(record)
        occurrence_counts[row_hash] += 1
        state_key = f"{row_hash}:{occurrence_counts[row_hash]}"
        new_state.append((table_name, state_key, row_hash, None, None))
        if state_key not in old_state:
            changed_indexes.append(index)
    changed_df = full_df.iloc[changed_indexes].reset_index(drop=True)
    deleted_count = len(set(old_state) - {item[1] for item in new_state})
    return changed_df, new_state, len(full_df), deleted_count


def _discover_key_hash_changes(
    fourd_connection,
    table_name: str,
    rows: Sequence[dict[str, str]],
    old_state: dict[str, tuple[str | None, datetime | None]],
) -> tuple[
    pd.DataFrame,
    list[tuple[str, str, str | None, datetime | None, datetime | None]],
    int,
    int,
    int,
    int,
]:
    source_rows = [
        row for row in rows
        if row.get("source_table", "").strip()
        and row.get("source_column", "").strip()
        and row.get("target_column", "").strip()
    ]
    source_tables = {row["source_table"].strip() for row in source_rows}
    if not source_rows or len(source_tables) != 1:
        raise ValueError(f"Target {table_name} must map to exactly one 4D source table")

    source_columns = list(dict.fromkeys(row["source_column"].strip() for row in source_rows))
    target_columns = [row["target_column"].strip() for row in source_rows]
    key_target_columns = [
        row["target_column"].strip()
        for row in _primary_key_columns(source_rows)
        if row.get("target_column", "").strip()
    ]
    if not key_target_columns:
        raise ValueError(f"Key-hash strategy for {table_name} requires a declared primary key")

    source_table = next(iter(source_tables))
    full_df = pd.read_sql(
        _source_projection_query(source_table, source_columns),
        fourd_connection,
    )
    if len(full_df.columns) != len(target_columns):
        raise ValueError(f"4D extraction returned unexpected columns for {source_table}")
    full_df.columns = target_columns

    keyed_rows: set[str] = set()
    changed_indexes: list[int] = []
    new_state: list[tuple[str, str, str | None, datetime | None, datetime | None]] = []
    inserted_count = 0
    updated_count = 0
    for index, record in enumerate(full_df.itertuples(index=False, name=None)):
        values = dict(zip(target_columns, record, strict=True))
        nk_hash = _natural_key_hash(
            tuple(values[column] for column in key_target_columns)
        )
        if nk_hash in keyed_rows:
            logger.warning(
                "Declared primary key columns %r are not unique in source table %s; falling back to duplicate-safe row-hash state.",
                key_target_columns,
                source_table,
            )
            changed_df, new_state, source_count, deleted_count = _discover_hash_changes(
                fourd_connection,
                table_name,
                rows,
                old_state,
            )
            return (
                changed_df,
                new_state,
                source_count,
                0,
                0,
                deleted_count,
            )
        hash_diff = _row_hash(record)
        keyed_rows.add(nk_hash)
        new_state.append((table_name, nk_hash, hash_diff, None, None))
        old_hash = old_state.get(nk_hash, (None, None))[0]
        if old_hash != hash_diff:
            changed_indexes.append(index)
            if old_hash is None:
                inserted_count += 1
            else:
                updated_count += 1

    changed_df = full_df.iloc[changed_indexes].reset_index(drop=True)
    deleted_count = len(set(old_state) - keyed_rows)
    return (
        changed_df,
        new_state,
        len(full_df),
        inserted_count,
        updated_count,
        deleted_count,
    )


def _ensure_helper_tables(cursor, database: str, schema: str) -> dict[str, str]:
    state = _ensure_state_table(cursor, database, schema)
    row_state = _qualified_name(database, schema, _ROW_STATE_NAME)
    batch_log = _qualified_name(database, schema, _BATCH_LOG_NAME)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {row_state} (
            TABLE_NAME VARCHAR(255) NOT NULL,
            SOURCE_KEY VARCHAR NOT NULL,
            ROW_HASH VARCHAR(64) NOT NULL,
            SOURCE_MODIFIED_AT TIMESTAMP_NTZ,
            SOURCE_CREATED_AT TIMESTAMP_NTZ
        )
        """
    )
    cursor.execute(
        f"ALTER TABLE {row_state} ADD COLUMN IF NOT EXISTS SOURCE_CREATED_AT TIMESTAMP_NTZ"
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {batch_log} (
            BATCH_ID VARCHAR(36) NOT NULL,
            RUN_ID VARCHAR(36) NOT NULL,
            TABLE_NAME VARCHAR(255) NOT NULL,
            STRATEGY VARCHAR(40) NOT NULL,
            STATUS VARCHAR(20) NOT NULL,
            SOURCE_SCAN_COUNT NUMBER(38, 0),
            EXTRACTED_ROW_COUNT NUMBER(38, 0),
            CHANGED_ROW_COUNT NUMBER(38, 0),
            INSERTED_ROW_COUNT NUMBER(38, 0),
            UPDATED_ROW_COUNT NUMBER(38, 0),
            DELETED_ROW_COUNT NUMBER(38, 0),
            HISTORY_ROW_COUNT NUMBER(38, 0),
            CHANGE_STATUS VARCHAR(20),
            OLD_WATERMARK TIMESTAMP_NTZ,
            NEW_WATERMARK TIMESTAMP_NTZ,
            START_TIME TIMESTAMP_NTZ NOT NULL,
            END_TIME TIMESTAMP_NTZ,
            DURATION_SECONDS FLOAT,
            ERROR_MESSAGE VARCHAR
        )
        """
    )
    cursor.execute(
        f"ALTER TABLE {batch_log} ADD COLUMN IF NOT EXISTS CHANGE_STATUS VARCHAR(20)"
    )
    return {"state": state, "row_state": row_state, "batch_log": batch_log}


def _ensure_state_table(cursor, database: str, schema: str) -> str:
    state = _qualified_name(database, schema, _STATE_NAME)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {state} (
            TABLE_NAME VARCHAR(255) NOT NULL,
            STRATEGY VARCHAR(40) NOT NULL,
            INITIALIZED BOOLEAN NOT NULL DEFAULT FALSE,
            LAST_WATERMARK TIMESTAMP_NTZ,
            LAST_SUCCESS_BATCH_ID VARCHAR(36),
            LAST_SOURCE_COUNT NUMBER(38, 0),
            LOCK_OWNER VARCHAR(36),
            LOCK_EXPIRES_AT TIMESTAMP_NTZ
        )
        """
    )
    return state


def _read_table_state(cursor, state_table: str, table_name: str) -> dict[str, Any] | None:
    row = cursor.execute(
        f"""
        SELECT STRATEGY, INITIALIZED, LAST_WATERMARK, LAST_SUCCESS_BATCH_ID,
               LAST_SOURCE_COUNT
        FROM {state_table}
        WHERE TABLE_NAME = %s
        """,
        (table_name,),
    ).fetchone()
    if row is None:
        return None
    return {
        "strategy": row[0],
        "initialized": bool(row[1]),
        "watermark": row[2],
        "last_batch_id": row[3],
        "source_count": row[4],
    }


def _read_row_state(
    cursor,
    row_state_table: str,
    table_name: str,
) -> dict[str, tuple[str, datetime | None, datetime | None]]:
    rows = cursor.execute(
        f"""
        SELECT SOURCE_KEY, ROW_HASH, SOURCE_MODIFIED_AT, SOURCE_CREATED_AT
        FROM {row_state_table}
        WHERE TABLE_NAME = %s
        """,
        (table_name,),
    ).fetchall()
    result: dict[str, tuple[str, datetime | None, datetime | None]] = {}
    for key, row_hash, modified_at, created_at in rows:
        if key in result:
            raise RuntimeError(f"Duplicate saved source state for {table_name}")
        result[str(key)] = (str(row_hash), modified_at, created_at)
    return result


def _acquire_lock(cursor, connection, state_table: str, table_name: str, strategy: str, batch_id: str) -> None:
    cursor.execute(
        f"""
        MERGE INTO {state_table} target
        USING (SELECT %s AS TABLE_NAME, %s AS STRATEGY) source
          ON target.TABLE_NAME = source.TABLE_NAME
        WHEN NOT MATCHED THEN INSERT (TABLE_NAME, STRATEGY, INITIALIZED)
             VALUES (source.TABLE_NAME, source.STRATEGY, FALSE)
        """,
        (table_name, strategy),
    )
    count = cursor.execute(
        f"SELECT COUNT(*) FROM {state_table} WHERE TABLE_NAME = %s",
        (table_name,),
    ).fetchone()[0]
    if count != 1:
        raise RuntimeError(
            f"Incremental state for {table_name} has {count} rows; expected exactly one. "
            "Resolve state-table duplicates before running."
        )

    cursor.execute(
        f"""
        UPDATE {state_table}
        SET LOCK_OWNER = %s,
            LOCK_EXPIRES_AT = DATEADD('hour', %s, CURRENT_TIMESTAMP())
        WHERE TABLE_NAME = %s
          AND STRATEGY = %s
          AND (LOCK_OWNER IS NULL OR LOCK_EXPIRES_AT < CURRENT_TIMESTAMP())
        """,
        (batch_id, _LOCK_TTL_HOURS, table_name, strategy),
    )
    if cursor.rowcount != 1:
        connection.rollback()
        raise RuntimeError(
            f"Another incremental run holds the lock for {table_name}, or its state "
            "uses a different change-detection strategy"
        )
    connection.commit()


def acquire_table_run_lock(
    cursor,
    connection,
    database: str,
    schema: str,
    table_name: str,
    metadata_rows: Sequence[dict[str, str]],
) -> dict[str, str]:
    state_table = _ensure_state_table(cursor, database, schema)
    owner = str(uuid4())
    strategy = select_change_strategy(metadata_rows)["strategy"]
    _acquire_lock(cursor, connection, state_table, table_name, strategy, owner)
    return {"state_table": state_table, "owner": owner, "strategy": strategy}


def renew_table_run_lock(cursor, connection, lock: dict[str, str], table_name: str) -> None:
    _renew_lock(cursor, connection, lock["state_table"], table_name, lock["owner"])


def release_table_run_lock(cursor, connection, lock: dict[str, str], table_name: str) -> None:
    _release_lock(cursor, connection, lock["state_table"], table_name, lock["owner"])


def _release_lock(cursor, connection, state_table: str, table_name: str, batch_id: str) -> None:
    cursor.execute(
        f"""
        UPDATE {state_table}
        SET LOCK_OWNER = NULL, LOCK_EXPIRES_AT = NULL
        WHERE TABLE_NAME = %s AND LOCK_OWNER = %s
        """,
        (table_name, batch_id),
    )
    connection.commit()


def _renew_lock(cursor, connection, state_table: str, table_name: str, batch_id: str) -> None:
    cursor.execute(
        f"""
        UPDATE {state_table}
        SET LOCK_EXPIRES_AT = DATEADD('hour', %s, CURRENT_TIMESTAMP())
        WHERE TABLE_NAME = %s
          AND LOCK_OWNER = %s
          AND LOCK_EXPIRES_AT >= CURRENT_TIMESTAMP()
        """,
        (_LOCK_TTL_HOURS, table_name, batch_id),
    )
    if cursor.rowcount != 1:
        connection.rollback()
        raise RuntimeError(f"Incremental lock expired or changed owner for {table_name}")
    connection.commit()


def _start_batch(cursor, batch_log: str, batch_id: str, run_id: str, table_name: str, strategy: str, watermark: datetime | None) -> None:
    cursor.execute(
        f"""
        INSERT INTO {batch_log}
            (BATCH_ID, RUN_ID, TABLE_NAME, STRATEGY, STATUS, OLD_WATERMARK, START_TIME)
        VALUES (%s, %s, %s, %s, 'RUNNING', %s, CURRENT_TIMESTAMP())
        """,
        (batch_id, run_id, table_name, strategy, watermark),
    )


def _finish_batch(
    cursor,
    batch_log: str,
    batch_id: str,
    status: str,
    counts: dict[str, int | None],
    new_watermark: datetime | None,
    duration_seconds: float,
    error_message: str | None = None,
    change_status: str | None = None,
) -> None:
    cursor.execute(
        f"""
        UPDATE {batch_log}
        SET STATUS = %s,
            SOURCE_SCAN_COUNT = %s,
            EXTRACTED_ROW_COUNT = %s,
            CHANGED_ROW_COUNT = %s,
            INSERTED_ROW_COUNT = %s,
            UPDATED_ROW_COUNT = %s,
            DELETED_ROW_COUNT = %s,
            HISTORY_ROW_COUNT = %s,
            CHANGE_STATUS = %s,
            NEW_WATERMARK = %s,
            END_TIME = CURRENT_TIMESTAMP(),
            DURATION_SECONDS = %s,
            ERROR_MESSAGE = %s
        WHERE BATCH_ID = %s
        """,
        (
            status,
            counts.get("source_scan"),
            counts.get("extracted"),
            counts.get("changed"),
            counts.get("inserted"),
            counts.get("updated"),
            counts.get("deleted"),
            counts.get("history"),
            change_status,
            new_watermark,
            duration_seconds,
            error_message,
            batch_id,
        ),
    )


def _replace_row_state(
    cursor,
    row_state_table: str,
    table_name: str,
    rows: Sequence[
        tuple[str, str, str | None, datetime | None]
        | tuple[str, str, str | None, datetime | None, datetime | None]
    ],
) -> None:
    cursor.execute(f"DELETE FROM {row_state_table} WHERE TABLE_NAME = %s", (table_name,))
    if rows:
        normalized_rows = [
            (*row, None) if len(row) == 4 else row
            for row in rows
        ]
        cursor.executemany(
            f"""
            INSERT INTO {row_state_table}
                (TABLE_NAME, SOURCE_KEY, ROW_HASH, SOURCE_MODIFIED_AT, SOURCE_CREATED_AT)
            VALUES (%s, %s, %s, %s, %s)
            """,
            normalized_rows,
        )


def _count_rows(cursor, qualified_table: str) -> int:
    return int(cursor.execute(f"SELECT COUNT(*) FROM {qualified_table}").fetchone()[0])


def _ensure_history_table(cursor, current_table: str, history_table: str) -> None:
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {history_table} LIKE {current_table}")
    cursor.execute(
        f"ALTER TABLE {history_table} "
        "ADD COLUMN IF NOT EXISTS HISTORY_BATCH_ID VARCHAR(36)"
    )
    cursor.execute(
        f"ALTER TABLE {history_table} "
        "ADD COLUMN IF NOT EXISTS ARCHIVED_AT TIMESTAMP_NTZ"
    )


def incremental_load_table(
    fourd_connection,
    snowflake_connection,
    snowflake_cursor,
    database: str,
    schema: str,
    table_name: str,
    metadata_rows: Sequence[dict[str, str]],
    run_id: str,
    overlap_seconds: int = 86400,
    table_lock: dict[str, str] | None = None,
) -> dict[str, Any]:
    if overlap_seconds < 0:
        raise ValueError("Incremental overlap seconds cannot be negative")
    strategy = select_change_strategy(metadata_rows)
    table_columns = _table_columns(metadata_rows)
    source_target_columns = _source_target_columns(metadata_rows)
    current_table = _qualified_name(database, schema, table_name)
    history_table = _qualified_name(database, schema, f"{table_name}_H")
    batch_id = str(uuid4())
    lock_owner = table_lock["owner"] if table_lock else batch_id
    started = time.monotonic()
    locked = False
    batch_started = False
    operation_error: BaseException | None = None
    counts: dict[str, int | None] = {
        "source_scan": 0,
        "extracted": 0,
        "changed": 0,
        "inserted": None,
        "updated": None,
        "deleted": 0,
        "history": 0,
    }
    old_watermark: datetime | None = None
    paths: dict[str, str] = {}
    stage_table = _qualified_name(database, schema, f"ETL_STAGE_{batch_id.replace('-', '')[:24].upper()}")
    stage_column_names = {column.casefold() for column in source_target_columns}
    stage_target_columns = [
        column
        for column in table_columns
        if column.casefold() in stage_column_names
    ]

    try:
        if not snowflake_cursor.execute(
            f"SELECT COUNT(*) FROM {current_table}"
        ).fetchone():
            raise RuntimeError(f"Could not validate current table {table_name}")
        _ensure_history_table(snowflake_cursor, current_table, history_table)
        paths = _ensure_helper_tables(snowflake_cursor, database, schema)
        if table_lock is None:
            _acquire_lock(
                snowflake_cursor,
                snowflake_connection,
                paths["state"],
                table_name,
                strategy["strategy"],
                lock_owner,
            )
            locked = True
        elif (
            table_lock["state_table"] != paths["state"]
            or table_lock["strategy"] != strategy["strategy"]
        ):
            raise RuntimeError(f"Invalid or incompatible per-table lock for {table_name}")
        table_state = _read_table_state(snowflake_cursor, paths["state"], table_name)
        if table_state is None:
            raise RuntimeError(f"Could not initialize incremental state for {table_name}")
        old_watermark = table_state["watermark"]
        if table_state["strategy"] != strategy["strategy"]:
            raise RuntimeError(
                f"Change strategy for {table_name} changed from {table_state['strategy']} "
                f"to {strategy['strategy']}; reset/reconcile its incremental state explicitly"
            )
        _start_batch(
            snowflake_cursor,
            paths["batch_log"],
            batch_id,
            run_id,
            table_name,
            strategy["strategy"],
            old_watermark,
        )
        snowflake_connection.commit()
        batch_started = True

        old_state = _read_row_state(snowflake_cursor, paths["row_state"], table_name)
        if not table_state["initialized"]:
            source_rows = [
                row for row in metadata_rows
                if row.get("source_table", "").strip()
                and row.get("source_column", "").strip()
                and row.get("target_column", "").strip()
            ]
            source_tables = {row["source_table"].strip() for row in source_rows}
            if not source_rows or len(source_tables) != 1:
                raise ValueError(f"No single complete source mapping for {table_name}")
            source_columns = list(dict.fromkeys(row["source_column"].strip() for row in source_rows))
            target_columns = [row["target_column"].strip() for row in source_rows]
            initial_df = pd.read_sql(
                build_source_query(next(iter(source_tables)), source_columns),
                fourd_connection,
            )
            if len(initial_df.columns) != len(target_columns):
                raise ValueError(f"Initial extraction returned unexpected columns for {table_name}")
            initial_df.columns = target_columns
            new_state = _rows_as_state(
                initial_df,
                table_name,
                source_target_columns,
                strategy,
            )
            if strategy["strategy"] == "KEY_TIMESTAMP":
                _validate_timestamp_metadata(
                    fourd_connection,
                    next(iter(source_tables)),
                    strategy["key_source_column"],
                    strategy["modified_source_column"],
                )
                key_values = [_source_key(value) for value in initial_df[strategy["key_target_column"]]]
                if len(key_values) != len(set(key_values)):
                    logger.warning(
                        "Declared primary key %r is not unique in source table %s; falling back to duplicate-safe row-hash state.",
                        strategy["key_target_column"],
                        next(iter(source_tables)),
                    )
                    new_state = _hash_state_rows(
                        initial_df,
                        table_name,
                        source_target_columns,
                        strategy["modified_target_column"],
                        strategy["created_target_column"],
                    )
            changed_df = initial_df
            counts["source_scan"] = len(initial_df)
            counts["extracted"] = len(initial_df)
            counts["changed"] = len(initial_df)
            if strategy["strategy"] in {"KEY_TIMESTAMP", "KEY_HASH"}:
                counts["inserted"] = len(initial_df)
                counts["updated"] = 0
            new_watermark = _timestamp_watermark(new_state)
            is_initial_load = True
        elif strategy["strategy"] == "KEY_TIMESTAMP":
            changed_df, new_state, source_count, inserted_count, updated_count, deleted_count = (
                _discover_timestamp_changes(
                    fourd_connection,
                    table_name,
                    metadata_rows,
                    old_state,
                    old_watermark,
                    overlap_seconds,
                )
            )
            counts["source_scan"] = source_count
            counts["extracted"] = len(changed_df)
            counts["changed"] = len(changed_df)
            counts["inserted"] = inserted_count
            counts["updated"] = updated_count
            counts["deleted"] = deleted_count
            new_watermark = max(
                (row[3] for row in new_state if row[3] is not None),
                default=old_watermark,
            )
            is_initial_load = False
        elif strategy["strategy"] == "KEY_HASH":
            (
                changed_df,
                new_state,
                source_count,
                inserted_count,
                updated_count,
                deleted_count,
            ) = _discover_key_hash_changes(
                fourd_connection,
                table_name,
                metadata_rows,
                old_state,
            )
            counts["source_scan"] = source_count
            counts["extracted"] = source_count
            counts["changed"] = len(changed_df)
            counts["inserted"] = inserted_count
            counts["updated"] = updated_count
            counts["deleted"] = deleted_count
            new_watermark = old_watermark
            is_initial_load = False
        else:
            changed_df, new_state, source_count, deleted_count = _discover_hash_changes(
                fourd_connection,
                table_name,
                metadata_rows,
                old_state,
            )
            counts["source_scan"] = source_count
            counts["extracted"] = source_count
            counts["changed"] = len(changed_df)
            counts["deleted"] = deleted_count
            new_watermark = old_watermark
            is_initial_load = False

        _renew_lock(
            snowflake_cursor,
            snowflake_connection,
            paths["state"],
            table_name,
            lock_owner,
        )

        if not table_state["initialized"]:
            is_changed = True
        else:
            is_changed = bool(counts["changed"] or counts["deleted"])
        if not is_changed:
            if _needs_created_timestamp_refresh(old_state, new_state):
                _replace_row_state(
                    snowflake_cursor,
                    paths["row_state"],
                    table_name,
                    new_state,
                )
            _finish_batch(
                snowflake_cursor,
                paths["batch_log"],
                batch_id,
                "SUCCESS",
                counts,
                old_watermark,
                time.monotonic() - started,
                change_status="NO_CHANGE",
            )
            snowflake_connection.commit()
            return {
                "table": table_name,
                "batch_id": batch_id,
                "strategy": strategy["strategy"],
                "status": "NO_CHANGE",
                "rows_loaded": 0,
                **counts,
                "watermark": old_watermark,
            }

        snowflake_cursor.execute(
            f"CREATE TEMPORARY TABLE {stage_table} LIKE {current_table}"
        )
        load_dataframe_to_snowflake(
            snowflake_cursor,
            f"ETL_STAGE_{batch_id.replace('-', '')[:24].upper()}",
            stage_target_columns,
            changed_df,
            schema_name=f"{_quote_identifier(database)}.{_quote_identifier(schema)}",
        )
        _renew_lock(
            snowflake_cursor,
            snowflake_connection,
            paths["state"],
            table_name,
            lock_owner,
        )
        history_rows = 0 if is_initial_load else _count_rows(snowflake_cursor, current_table)
        counts["history"] = history_rows

        snowflake_cursor.execute("BEGIN")
        if history_rows:
            history_columns = [*_quote_list(table_columns), '"HISTORY_BATCH_ID"', '"ARCHIVED_AT"']
            source_columns = [*_quote_list(table_columns)]
            snowflake_cursor.execute(
                f"""
                INSERT INTO {history_table} ({', '.join(history_columns)})
                SELECT {', '.join(source_columns)}, %s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                FROM {current_table}
                """,
                (batch_id,),
            )
        snowflake_cursor.execute(f"DELETE FROM {current_table}")
        if counts["changed"]:
            snowflake_cursor.execute(
                f"""
                INSERT INTO {current_table} ({', '.join(_quote_list(table_columns))})
                SELECT {', '.join(_quote_list(table_columns))}
                FROM {stage_table}
                """
            )
        _replace_row_state(snowflake_cursor, paths["row_state"], table_name, new_state)
        snowflake_cursor.execute(
            f"""
            UPDATE {paths['state']}
            SET INITIALIZED = TRUE,
                LAST_WATERMARK = %s,
                LAST_SUCCESS_BATCH_ID = %s,
                LAST_SOURCE_COUNT = %s
            WHERE TABLE_NAME = %s AND LOCK_OWNER = %s
            """,
            (new_watermark, batch_id, counts["source_scan"], table_name, lock_owner),
        )
        _finish_batch(
            snowflake_cursor,
            paths["batch_log"],
            batch_id,
            "SUCCESS",
            counts,
            new_watermark,
            time.monotonic() - started,
            change_status="CHANGED",
        )
        snowflake_cursor.execute("COMMIT")
        logger.info(
            "Incremental batch %s for %s succeeded: scanned=%s extracted=%s changed=%s "
            "inserted=%s updated=%s deleted=%s archived=%s watermark=%s",
            batch_id,
            table_name,
            counts["source_scan"],
            counts["extracted"],
            counts["changed"],
            counts["inserted"],
            counts["updated"],
            counts["deleted"],
            counts["history"],
            new_watermark,
        )
        return {
            "table": table_name,
            "batch_id": batch_id,
            "strategy": strategy["strategy"],
            "status": "SUCCESS",
            "rows_loaded": counts["changed"],
            "watermark": new_watermark,
            **counts,
        }
    except BaseException as exc:
        operation_error = exc
        try:
            snowflake_connection.rollback()
        except Exception:
            logger.exception("Could not roll back incremental load for %s", table_name)
        if batch_started and paths:
            try:
                _finish_batch(
                    snowflake_cursor,
                    paths["batch_log"],
                    batch_id,
                    "INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else "FAILED",
                    counts,
                    old_watermark,
                    time.monotonic() - started,
                    str(exc),
                    change_status="INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else "FAILED",
                )
                snowflake_connection.commit()
            except Exception:
                logger.exception("Could not record failed incremental batch for %s", table_name)
        raise
    finally:
        cleanup_errors: list[Exception] = []
        if locked and paths:
            try:
                _release_lock(
                    snowflake_cursor,
                    snowflake_connection,
                    paths["state"],
                    table_name,
                    lock_owner,
                )
            except Exception as exc:
                logger.exception("Could not release incremental lock for %s", table_name)
                cleanup_errors.append(exc)
        try:
            snowflake_cursor.execute(f"DROP TABLE IF EXISTS {stage_table}")
            snowflake_connection.commit()
        except Exception as exc:
            logger.exception("Could not remove incremental staging table for %s", table_name)
            cleanup_errors.append(exc)
        if cleanup_errors and operation_error is None:
            raise RuntimeError(
                f"Incremental load for {table_name} completed with cleanup failures"
            ) from cleanup_errors[0]


def _quote_list(columns: Sequence[str]) -> list[str]:
    return [_quote_identifier(column) for column in columns]
