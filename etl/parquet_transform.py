from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

from etl.ddl import build_create_table_sql
from etl.incremental import acquire_table_run_lock, release_table_run_lock
from etl.parquet_snowflake import (
    _mapped_target_columns,
    _qualified_name,
    _quote_identifier,
    _safe_stage_component,
)
from etl.quarantine import ensure_quarantine_table

logger = logging.getLogger(__name__)
_MANAGED_TIMESTAMP_COLUMNS = {"CREATED_TIMESTAMP", "UPDATED_TIMESTAMP"}
_CURRENT_TIMESTAMP_NTZ = "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"


@dataclass(frozen=True)
class ParquetTransformResult:
    run_id: str
    target_table: str
    raw_table: str
    file_name: str
    source_rows: int
    inserted_rows: int
    rejected_rows: int
    skipped_as_duplicate: bool


@dataclass(frozen=True)
class _ColumnConversion:
    column: str
    snowflake_type: str
    expression: str
    invalid_condition: str | None
    error_type: str | None


def _normalize_type(value: str, source_data_type: str) -> str:
    normalized = re.sub(r"/\*.*?\*/", "", str(value), flags=re.DOTALL).strip().upper()
    if not normalized:
        normalized = "VARCHAR"
    if (
        source_data_type.strip().upper() == "CLOB"
        and re.fullmatch(r"VARCHAR\s*\(\s*\d+\s*\)", normalized)
    ):
        return "VARCHAR"

    patterns = (
        r"VARCHAR(?:\s*\(\s*\d+\s*\))?",
        r"CHAR(?:ACTER)?(?:\s*\(\s*\d+\s*\))?",
        r"STRING(?:\s*\(\s*\d+\s*\))?",
        r"TEXT",
        r"NUMBER(?:\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?",
        r"NUMERIC(?:\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?",
        r"DECIMAL(?:\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?",
        r"(?:INT|INTEGER|BIGINT|SMALLINT|TINYINT|BYTEINT)",
        r"(?:FLOAT|FLOAT4|FLOAT8|DOUBLE(?:\s+PRECISION)?|REAL)",
        r"(?:BOOLEAN|BOOL)",
        r"DATE",
        r"TIME(?:\s*\(\s*\d+\s*\))?",
        r"TIMESTAMP(?:_(?:NTZ|LTZ|TZ))?(?:\s*\(\s*\d+\s*\))?",
        r"VARIANT",
        r"BINARY(?:\s*\(\s*\d+\s*\))?",
    )
    if not any(re.fullmatch(pattern, normalized) for pattern in patterns):
        raise ValueError(f"Unsupported Snowflake datatype in mapping: {value!r}")
    return normalized


def _conversion_for_column(
    column: str,
    snowflake_type: str,
    source_data_type: str = "",
) -> _ColumnConversion:
    quoted = f"r.{_quote_identifier(column)}"
    normalized_type = _normalize_type(snowflake_type, source_data_type)

    string_type = re.fullmatch(
        r"(?:VARCHAR|CHAR|CHARACTER|STRING)\s*(?:\(\s*(\d+)\s*\))?|TEXT",
        normalized_type,
    )
    if string_type:
        max_length = string_type.group(1) if string_type.lastindex else None
        if max_length:
            invalid = (
                f"({quoted} IS NOT NULL AND LENGTH({quoted}) > {int(max_length)})"
            )
            return _ColumnConversion(
                column,
                normalized_type,
                quoted,
                invalid,
                "VALUE_TOO_LONG",
            )
        return _ColumnConversion(column, normalized_type, quoted, None, None)

    number_match = re.fullmatch(
        r"(?:NUMBER|NUMERIC|DECIMAL)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?",
        normalized_type,
    )
    if number_match:
        precision = int(number_match.group(1) or 38)
        scale = int(number_match.group(2) or 0)
        parsed = f"TRY_TO_DECIMAL({quoted}, {precision}, {scale})"
        invalid = f"({quoted} IS NOT NULL AND {parsed} IS NULL)"
        return _ColumnConversion(
            column,
            normalized_type,
            parsed,
            invalid,
            "INVALID_NUMERIC_VALUE",
        )
    if normalized_type in {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"}:
        parsed = f"TRY_TO_DECIMAL({quoted}, 38, 0)"
        invalid = f"({quoted} IS NOT NULL AND {parsed} IS NULL)"
        return _ColumnConversion(
            column,
            normalized_type,
            parsed,
            invalid,
            "INVALID_NUMERIC_VALUE",
        )
    if normalized_type in {"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"}:
        parsed = f"TRY_TO_DOUBLE({quoted})"
        invalid = f"({quoted} IS NOT NULL AND {parsed} IS NULL)"
        return _ColumnConversion(
            column,
            normalized_type,
            parsed,
            invalid,
            "INVALID_NUMERIC_VALUE",
        )
    if normalized_type in {"BOOLEAN", "BOOL"}:
        parsed = f"TRY_TO_BOOLEAN({quoted})"
        invalid = f"({quoted} IS NOT NULL AND {parsed} IS NULL)"
        return _ColumnConversion(
            column,
            normalized_type,
            parsed,
            invalid,
            "INVALID_BOOLEAN_VALUE",
        )
    if normalized_type == "DATE":
        parsed = (
            f"COALESCE(TRY_TO_DATE({quoted}, 'DD/MM/YYYY'), "
            f"TRY_TO_DATE({quoted}, 'YYYY/MM/DD'), "
            f"TRY_TO_DATE({quoted}, 'YYYY-MM-DD'), TRY_TO_DATE({quoted}))"
        )
        invalid = f"({quoted} IS NOT NULL AND {parsed} IS NULL)"
        return _ColumnConversion(
            column,
            normalized_type,
            parsed,
            invalid,
            "INVALID_DATE",
        )
    if normalized_type.startswith("TIMESTAMP"):
        timestamp_kind = normalized_type.split("(", 1)[0].split()[0]
        if timestamp_kind == "TIMESTAMP":
            timestamp_kind = "TIMESTAMP_NTZ"
        formats = (
            "DD/MM/YYYY HH24:MI:SS.FF9",
            "DD/MM/YYYY HH24:MI:SS",
            "YYYY/MM/DD HH24:MI:SS.FF9",
            "YYYY/MM/DD HH24:MI:SS",
            'YYYY-MM-DD"T"HH24:MI:SS.FF9',
            'YYYY-MM-DD"T"HH24:MI:SS',
            "YYYY-MM-DD HH24:MI:SS.FF9",
            "YYYY-MM-DD HH24:MI:SS",
            "YYYY-MM-DD",
        )
        parse_attempts = [
            f"TRY_TO_{timestamp_kind}({quoted}, '{date_format}')"
            for date_format in formats
        ]
        parse_attempts.append(f"TRY_TO_{timestamp_kind}({quoted})")
        parsed = f"COALESCE({', '.join(parse_attempts)})"
        invalid = f"({quoted} IS NOT NULL AND {parsed} IS NULL)"
        return _ColumnConversion(
            column,
            normalized_type,
            parsed,
            invalid,
            "INVALID_TIMESTAMP",
        )
    if normalized_type.startswith("TIME"):
        parsed = (
            f"COALESCE(TRY_TO_TIME({quoted}, 'HH24:MI:SS.FF9'), "
            f"TRY_TO_TIME({quoted}, 'HH24:MI:SS'), TRY_TO_TIME({quoted}))"
        )
        invalid = f"({quoted} IS NOT NULL AND {parsed} IS NULL)"
        return _ColumnConversion(
            column,
            normalized_type,
            parsed,
            invalid,
            "INVALID_TIME",
        )
    if normalized_type == "VARIANT":
        parsed = f"TRY_PARSE_JSON({quoted})"
        invalid = f"({quoted} IS NOT NULL AND {parsed} IS NULL)"
        return _ColumnConversion(
            column,
            normalized_type,
            parsed,
            invalid,
            "INVALID_VARIANT",
        )
    if normalized_type.startswith("BINARY"):
        parsed = f"TRY_TO_BINARY(SUBSTR({quoted}, 8), 'BASE64')"
        invalid = (
            f"({quoted} IS NOT NULL AND "
            f"(LEFT({quoted}, 7) <> 'base64:' OR {parsed} IS NULL))"
        )
        return _ColumnConversion(
            column,
            normalized_type,
            parsed,
            invalid,
            "INVALID_BINARY",
        )
    raise ValueError(f"Unsupported Snowflake datatype in mapping: {snowflake_type!r}")


def _source_key_expression(mapping_rows: Sequence[dict[str, str]]) -> str:
    key_columns = [
        row["target_column"].strip()
        for row in mapping_rows
        if row.get("target_column", "").strip()
        and "pk" in row.get("constraint", "").casefold()
        and row["target_column"].strip().upper() not in _MANAGED_TIMESTAMP_COLUMNS
    ]
    if not key_columns:
        key_columns = [
            row["target_column"].strip()
            for row in mapping_rows
            if row.get("target_column", "").strip()
            and row["target_column"].strip().upper().endswith("_ID")
            and row["target_column"].strip().upper() not in _MANAGED_TIMESTAMP_COLUMNS
        ]
    if not key_columns:
        return "NULL"
    values = ", ".join(
        f"TO_VARCHAR(r.{_quote_identifier(column)})"
        for column in key_columns
    )
    return f"COALESCE({values})" if len(key_columns) > 1 else values


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _table_exists(cursor: Any, database: str, schema: str, table: str) -> bool:
    information_schema = ".".join(
        (_quote_identifier(database), '"INFORMATION_SCHEMA"', '"TABLES"')
    )
    result = cursor.execute(
        f"SELECT COUNT(*) FROM {information_schema} "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND TABLE_TYPE = 'BASE TABLE'",
        (schema, table),
    ).fetchone()
    return bool(result and int(result[0]) > 0)


def _invalid_for_temp_alias(
    conversion: _ColumnConversion,
    converted_alias: str,
) -> str | None:
    if not conversion.invalid_condition:
        return None
    raw_column = f"p.{_quote_identifier(conversion.column)}"
    converted_column = f"p.{_quote_identifier(converted_alias)}"
    if conversion.error_type == "VALUE_TOO_LONG":
        match = re.search(r" > (\d+)\)$", conversion.invalid_condition)
        if match is None:
            raise ValueError(f"Could not determine VARCHAR limit for {conversion.column}")
        return f"({raw_column} IS NOT NULL AND LENGTH({raw_column}) > {int(match.group(1))})"
    if conversion.error_type == "INVALID_BINARY":
        binary_type = re.fullmatch(
            r"BINARY(?:\s*\(\s*(\d+)\s*\))?",
            _normalize_type(conversion.snowflake_type, ""),
        )
        length_check = (
            f" OR OCTET_LENGTH({converted_column}) > {int(binary_type.group(1))}"
            if binary_type and binary_type.group(1)
            else ""
        )
        return (
            f"({raw_column} IS NOT NULL AND "
            f"(LEFT({raw_column}, 7) <> 'base64:' OR {converted_column} IS NULL"
            f"{length_check}))"
        )
    return f"({raw_column} IS NOT NULL AND {converted_column} IS NULL)"


def transform_parquet_raw_to_target(
    snowflake_connection: Any,
    database: str,
    schema: str,
    mapping_rows: Sequence[dict[str, str]],
    run_id: str,
    file_name: str,
) -> ParquetTransformResult:
    target_table, _ = _mapped_target_columns(mapping_rows)
    source_table = next(
        row["source_table"].strip()
        for row in mapping_rows
        if row.get("source_table", "").strip()
        and row.get("target_column", "").strip()
    )
    raw_table_name = f"{target_table}_RAW"
    raw_table = _qualified_name(database, schema, raw_table_name)
    target = _qualified_name(database, schema, target_table)
    ledger = _qualified_name(database, schema, "ETL_PARQUET_FILE_LOADS")
    run_value = str(run_id).strip()
    file_value = Path(file_name).name

    source_rows = [
        row
        for row in mapping_rows
        if row.get("source_table", "").strip()
        and row.get("source_column", "").strip()
        and row.get("target_column", "").strip()
    ]
    conversions = [
        _conversion_for_column(
            row["target_column"].strip(),
            row.get("snowflake_datatype", "") or "VARCHAR",
            row.get("source_data_type", ""),
        )
        for row in source_rows
    ]
    source_key_sql = _source_key_expression(mapping_rows)
    mapped_source_target_columns = [
        conversion.column for conversion in conversions
    ]
    managed_columns = [
        row["target_column"].strip()
        for row in mapping_rows
        if row.get("target_column", "").strip().upper() in _MANAGED_TIMESTAMP_COLUMNS
    ]
    raw_filter = (
        f"r.{_quote_identifier('ETL_RUN_ID')} = %s "
        f"AND r.{_quote_identifier('ETL_SOURCE_FILE')} = %s"
    )

    cursor = snowflake_connection.cursor()
    lock: dict[str, str] | None = None
    publish_table: str | None = None
    publish_table_contains_previous_target = False
    validation_table: str | None = None
    transaction_started = False
    try:
        cursor.execute(
            f"ALTER TABLE {ledger} ADD COLUMN IF NOT EXISTS TRANSFORM_STATUS VARCHAR(20)"
        )
        cursor.execute(
            f"ALTER TABLE {ledger} ADD COLUMN IF NOT EXISTS TRANSFORMED_ROWS NUMBER(38,0)"
        )
        cursor.execute(
            f"ALTER TABLE {ledger} ADD COLUMN IF NOT EXISTS REJECTED_ROWS NUMBER(38,0)"
        )
        cursor.execute(
            f"ALTER TABLE {ledger} ADD COLUMN IF NOT EXISTS TRANSFORM_ERROR VARCHAR"
        )
        lock_name = (
            f"{_safe_stage_component(target_table, 'Target table')[:230]}_PARQUET_RAW"
        )
        lock = acquire_table_run_lock(
            cursor,
            snowflake_connection,
            database,
            schema,
            lock_name,
            mapping_rows,
        )

        ledger_row = cursor.execute(
            f"""
            SELECT STATUS, LOADED_ROWS, TRANSFORM_STATUS,
                   TRANSFORMED_ROWS, REJECTED_ROWS
            FROM {ledger}
            WHERE RUN_ID = %s AND TABLE_NAME = %s AND FILE_NAME = %s
            """,
            (run_value, target_table, file_value),
        ).fetchone()
        if ledger_row is None or str(ledger_row[0]).upper() != "SUCCESS":
            raise RuntimeError(
                f"Parquet file {file_value} has not completed its Snowflake raw load"
            )
        if str(ledger_row[2] or "").upper() == "SUCCESS":
            return ParquetTransformResult(
                run_id=run_value,
                target_table=target_table,
                raw_table=raw_table_name,
                file_name=file_value,
                source_rows=int(ledger_row[1] or 0),
                inserted_rows=int(ledger_row[3] or 0),
                rejected_rows=int(ledger_row[4] or 0),
                skipped_as_duplicate=True,
            )

        quarantine_table = ensure_quarantine_table(cursor, database, schema)
        raw_count = cursor.execute(
            f"SELECT COUNT(*) FROM {raw_table} r WHERE {raw_filter}",
            (run_value, file_value),
        ).fetchone()
        source_rows_count = int(raw_count[0]) if raw_count else 0
        expected_raw_rows = int(ledger_row[1] or 0)
        if source_rows_count != expected_raw_rows:
            raise RuntimeError(
                f"Raw landing row count mismatch for {target_table}: "
                f"ledger={expected_raw_rows}, raw={source_rows_count}"
            )

        publish_table_name = f"ETL_PARQUET_PUBLISH_{uuid4().hex[:16].upper()}"
        publish_table = _qualified_name(database, schema, publish_table_name)
        publish_mapping_rows = [
            {
                **row,
                "target_table": publish_table_name,
            }
            if row.get("target_column", "").strip()
            else dict(row)
            for row in mapping_rows
        ]
        cursor.execute(
            build_create_table_sql(
                publish_table_name,
                publish_mapping_rows,
                schema_name=".".join(
                    (_quote_identifier(database), _quote_identifier(schema))
                ),
            )
        )
        validation_table_name = f"ETL_PARQUET_VALIDATE_{uuid4().hex[:16].upper()}"
        validation_table = _qualified_name(database, schema, validation_table_name)
        value_aliases = [
            f"__ETL_VALUE_{index:04d}" for index in range(len(conversions))
        ]
        parsed_values = ", ".join(
            f"{conversion.expression} AS {_quote_identifier(value_alias)}"
            for conversion, value_alias in zip(conversions, value_aliases, strict=True)
        )
        parsed_table = (
            f"(SELECT r.*, {parsed_values} FROM {raw_table} r "
            f"WHERE {raw_filter}) p"
        )
        validation_values = []
        conversion_invalid_aliases: dict[str, str] = {}
        for index, conversion in enumerate(conversions):
            if not conversion.invalid_condition:
                continue
            invalid_alias = f"__ETL_INVALID_{index:04d}"
            condition = _invalid_for_temp_alias(conversion, value_aliases[index])
            if condition is None:
                continue
            conversion_invalid_aliases[conversion.column] = invalid_alias
            validation_values.append(
                f"COALESCE({condition}, FALSE) AS {_quote_identifier(invalid_alias)}"
            )
        cursor.execute(
            f"CREATE TEMPORARY TABLE {validation_table} AS "
            f"SELECT p.*"
            f"{', ' + ', '.join(validation_values) if validation_values else ''} "
            f"FROM {parsed_table}",
            (run_value, file_value),
        )

        invalid_conditions = [
            f"r.{_quote_identifier(invalid_alias)}"
            for invalid_alias in conversion_invalid_aliases.values()
        ]
        valid_condition = (
            " AND ".join(f"NOT {condition}" for condition in invalid_conditions)
            if invalid_conditions
            else "TRUE"
        )
        valid_count_result = cursor.execute(
            f"SELECT COUNT(*) FROM {validation_table} r "
            f"WHERE {valid_condition}"
        ).fetchone()
        inserted_rows = int(valid_count_result[0]) if valid_count_result else 0
        rejected_rows = source_rows_count - inserted_rows

        cursor.execute("BEGIN")
        transaction_started = True
        cursor.execute(
            f"DELETE FROM {quarantine_table} WHERE RUN_ID = %s AND TARGET_TABLE = %s",
            (run_value, target_table),
        )
        if conversion_invalid_aliases:
            error_objects = []
            for index, conversion in enumerate(conversions):
                invalid_alias = conversion_invalid_aliases.get(conversion.column)
                if invalid_alias is None:
                    continue
                message = (
                    f"{_sql_literal(f'Invalid value for {conversion.column} ({conversion.snowflake_type}): ')} "
                    f"|| COALESCE(TO_VARCHAR(r.{_quote_identifier(conversion.column)}), '<NULL>')"
                )
                error_objects.append(
                    "IFF("
                    f"r.{_quote_identifier(invalid_alias)}, "
                    "OBJECT_CONSTRUCT("
                    f"'column', {_sql_literal(conversion.column)}, "
                    f"'error_type', {_sql_literal(conversion.error_type or 'SOURCE_DATA_CONVERSION_ERROR')}, "
                    f"'message', {message}), NULL)"
                )
            error_array = f"ARRAY_CONSTRUCT_COMPACT({', '.join(error_objects)})"
            error_row_sql = (
                "OBJECT_CONSTRUCT_KEEP_NULL("
                + ", ".join(
                    f"{_sql_literal(conversion.column)}, "
                    f"r.{_quote_identifier(conversion.column)}"
                    for conversion in conversions
                )
                + ")"
            )
            cursor.execute(
                f"""
                INSERT INTO {quarantine_table}
                    (RUN_ID, TARGET_TABLE, SOURCE_TABLE, SOURCE_ROW_KEY,
                     ERROR_COLUMN, ERROR_TYPE, ERROR_MESSAGE, RAW_ROW,
                     BATCH_NUMBER, STATUS)
                SELECT %s, %s, %s, {source_key_sql},
                       f.value:"column"::VARCHAR,
                       f.value:"error_type"::VARCHAR,
                       f.value:"message"::VARCHAR,
                       {error_row_sql}, NULL, 'OPEN'
                FROM {validation_table} r,
                     LATERAL FLATTEN(INPUT => {error_array}) f
                """,
                (run_value, target_table, source_table),
            )

        typed_select_values = [
            f"r.{_quote_identifier(value_alias)}" for value_alias in value_aliases
        ]
        typed_select_values.extend(_CURRENT_TIMESTAMP_NTZ for _ in managed_columns)
        insert_values = ", ".join(typed_select_values)
        target_insert_columns = ", ".join(
            _quote_identifier(column)
            for column in [*mapped_source_target_columns, *managed_columns]
        )
        cursor.execute(
            f"INSERT INTO {publish_table} ({target_insert_columns}) "
            f"SELECT {insert_values} FROM {validation_table} r "
            f"WHERE {valid_condition}"
        )
        affected_rows = getattr(cursor, "rowcount", -1)
        if affected_rows >= 0 and affected_rows != inserted_rows:
            raise RuntimeError(
                f"Typed target insert count mismatch for {target_table}: "
                f"expected {inserted_rows}, inserted {affected_rows}"
            )

        snowflake_connection.commit()
        transaction_started = False
        if _table_exists(cursor, database, schema, target_table):
            cursor.execute(f"ALTER TABLE {target} SWAP WITH {publish_table}")
            publish_table_contains_previous_target = True
            history_suffix = f"_HIST_{uuid4().hex[:12].upper()}"
            history_name = f"{target_table[:255 - len(history_suffix)]}{history_suffix}"
            history_table = _qualified_name(database, schema, history_name)
            cursor.execute(f"ALTER TABLE {publish_table} RENAME TO {history_table}")
            publish_table_contains_previous_target = False
            publish_table = None
        else:
            cursor.execute(f"ALTER TABLE {publish_table} RENAME TO {target}")
            publish_table = None

        cursor.execute("BEGIN")
        transaction_started = True
        cursor.execute(
            f"""
            UPDATE {ledger}
            SET TRANSFORM_STATUS = 'SUCCESS', TRANSFORMED_ROWS = %s,
                REJECTED_ROWS = %s, TRANSFORM_ERROR = NULL
            WHERE RUN_ID = %s AND TABLE_NAME = %s AND FILE_NAME = %s
            """,
            (
                inserted_rows,
                rejected_rows,
                run_value,
                target_table,
                file_value,
            ),
        )
        snowflake_connection.commit()
        transaction_started = False
        return ParquetTransformResult(
            run_id=run_value,
            target_table=target_table,
            raw_table=raw_table_name,
            file_name=file_value,
            source_rows=source_rows_count,
            inserted_rows=inserted_rows,
            rejected_rows=rejected_rows,
            skipped_as_duplicate=False,
        )
    except Exception as exc:
        if transaction_started:
            snowflake_connection.rollback()
        try:
            cursor.execute(
                f"""
                UPDATE {ledger}
                SET TRANSFORM_STATUS = 'FAILED', TRANSFORM_ERROR = %s
                WHERE RUN_ID = %s AND TABLE_NAME = %s AND FILE_NAME = %s
                """,
                (str(exc), run_value, target_table, file_value),
            )
            snowflake_connection.commit()
        except Exception:
            logger.exception(
                "Could not record failed Parquet transformation for %s",
                target_table,
            )
        raise
    finally:
        if validation_table is not None:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {validation_table}")
            except Exception:
                logger.exception(
                    "Could not remove validation table for %s",
                    target_table,
                )
        if publish_table is not None:
            if publish_table_contains_previous_target:
                logger.warning(
                    "Previous target snapshot for %s remains at %s after publish",
                    target_table,
                    publish_table,
                )
            else:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {publish_table}")
                except Exception:
                    logger.exception(
                        "Could not remove publish table for %s",
                        target_table,
                    )
        if lock is not None:
            try:
                release_table_run_lock(
                    cursor,
                    snowflake_connection,
                    lock,
                    f"{_safe_stage_component(target_table, 'Target table')[:230]}_PARQUET_RAW",
                )
            except Exception:
                logger.exception("Could not release Parquet transform lock for %s", target_table)
        cursor.close()
