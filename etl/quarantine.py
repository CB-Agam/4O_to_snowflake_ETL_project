from __future__ import annotations

import json
import re
from typing import Any, Sequence


def quote_identifier(identifier: str) -> str:
    value = str(identifier)
    if not value or "\x00" in value:
        raise ValueError(f"Invalid Snowflake identifier: {value!r}")
    return '"' + value.replace('"', '""') + '"'


def qualified_name(database: str, schema: str, table: str) -> str:
    return ".".join(quote_identifier(value) for value in (database, schema, table))


def ensure_quarantine_table(cursor, database: str, schema: str) -> str:
    table = qualified_name(database, schema, "ETL_DATA_QUALITY_ERRORS")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            ERROR_ID VARCHAR(36) DEFAULT UUID_STRING(),
            RUN_ID VARCHAR(36),
            TARGET_TABLE VARCHAR(255) NOT NULL,
            SOURCE_TABLE VARCHAR(255),
            SOURCE_ROW_KEY VARCHAR(2000),
            ERROR_COLUMN VARCHAR(255),
            ERROR_TYPE VARCHAR(100) NOT NULL,
            ERROR_MESSAGE VARCHAR NOT NULL,
            RAW_ROW VARIANT NOT NULL,
            BATCH_NUMBER NUMBER(38,0),
            STATUS VARCHAR(20) DEFAULT 'OPEN',
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            RESOLVED_AT TIMESTAMP_NTZ
        )
        """
    )
    return table


def infer_error_column(message: str) -> str | None:
    patterns = (
        r"(?:failed on column|column)\s+([A-Za-z0_][A-Za-z0-9_$]*)",
        r"invalid value.*?(?:column|field)\s+([A-Za-z0_][A-Za-z0-9_$]*)",
    )
    for pattern in patterns:
        match = re.search(pattern, message, re.I)
        if match:
            return match.group(1)
    return None


def infer_error_type(message: str) -> str:
    lowered = message.casefold()
    if "year" in lowered or "date" in lowered or "timestamp" in lowered:
        return "INVALID_TIMESTAMP"
    if "numeric value" in lowered or "number" in lowered or "float" in lowered:
        return "INVALID_NUMERIC_VALUE"
    if "string is too long" in lowered or "character" in lowered and "long" in lowered:
        return "VALUE_TOO_LONG"
    if "null" in lowered:
        return "NULL_OR_INVALID_VALUE"
    return "SOURCE_DATA_CONVERSION_ERROR"


def source_row_key(record: dict[str, Any], target_columns: Sequence[str]) -> str | None:
    preferred = ("ID", "KEY", "POSTING_ID", "CONTACT_ID", "ACCOUNT_ID")
    by_name = {str(name).upper(): name for name in record}
    candidates = [name for name in preferred if name in by_name]
    candidates.extend(
        name for name in target_columns
        if str(name).upper().endswith("_ID") and str(name).upper() not in candidates
    )
    for name in candidates:
        actual = by_name.get(str(name).upper())
        if actual is not None and record.get(actual) is not None:
            return str(record[actual])
    return None


def record_quarantine(
    cursor,
    database: str,
    schema: str,
    run_id: str,
    target_table: str,
    source_table: str | None,
    batch_number: int,
    record: dict[str, Any],
    error: BaseException | str,
    target_columns: Sequence[str] = (),
    error_column: str | None = None,
) -> None:
    ensure_quarantine_table(cursor, database, schema)
    table = qualified_name(database, schema, "ETL_DATA_QUALITY_ERRORS")
    message = str(error)
    cursor.execute(
        f"""
        INSERT INTO {table}
            (RUN_ID, TARGET_TABLE, SOURCE_TABLE, SOURCE_ROW_KEY, ERROR_COLUMN,
             ERROR_TYPE, ERROR_MESSAGE, RAW_ROW, BATCH_NUMBER)
        SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s
        """,
        (
            run_id,
            target_table,
            source_table,
            source_row_key(record, target_columns),
            error_column or infer_error_column(message),
            infer_error_type(message),
            message,
            json.dumps(record, ensure_ascii=False, default=str),
            batch_number,
        ),
    )
