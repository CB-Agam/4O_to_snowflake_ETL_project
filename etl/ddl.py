from __future__ import annotations

import re
from typing import Dict, List


_BOUNDED_VARCHAR = re.compile(r"^VARCHAR\s*\(\s*\d+\s*\)$", re.IGNORECASE)
_ETL_TIMESTAMP_COLUMNS = {"CREATED_TIMESTAMP", "UPDATED_TIMESTAMP"}
_ETL_TIMESTAMP_TYPE = "TIMESTAMP_NTZ"
_CURRENT_TIMESTAMP_NTZ = "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"


def _quote_identifier(identifier: str) -> str:
    identifier = str(identifier).strip()
    if not identifier:
        raise ValueError("Snowflake identifier cannot be blank")
    return '"' + identifier.replace('"', '""') + '"'


def _qualified_name(schema_name: str | None, table_name: str) -> str:
    table_name = str(table_name).strip()
    if not schema_name:
        return _quote_identifier(table_name)
    parts = [part.strip() for part in str(schema_name).split(".") if part.strip()]
    return ".".join(_quote_identifier(part) for part in [*parts, table_name])


def is_primary_key(value: str | None) -> bool:
    if value is None:
        return False
    return "pk" in str(value).strip().lower()


def _snowflake_column_type(row: Dict[str, str]) -> str:
    data_type = row.get("snowflake_datatype") or "VARCHAR"
    if (
        row.get("source_data_type", "").strip().upper() == "CLOB"
        and _BOUNDED_VARCHAR.fullmatch(data_type.strip())
    ):
        return "VARCHAR"
    return data_type


def build_create_table_sql(table_name: str, rows: List[Dict[str, str]], schema_name: str | None = None) -> str:
    if not rows:
        raise ValueError(f"No metadata rows provided for table: {table_name}")

    table_name = str(table_name).strip()
    column_definitions: List[str] = []
    pk_columns = [row["target_column"] for row in rows if is_primary_key(row.get("constraint"))]

    for row in rows:
        column_name = row.get("target_column")
        if not column_name:
            continue

        is_etl_timestamp = column_name.strip().upper() in _ETL_TIMESTAMP_COLUMNS
        data_type = _ETL_TIMESTAMP_TYPE if is_etl_timestamp else _snowflake_column_type(row)
        quoted_name = _quote_identifier(column_name)
        definition = f"{quoted_name} {data_type}"
        if is_etl_timestamp:
            definition = f"{definition} DEFAULT {_CURRENT_TIMESTAMP_NTZ} NOT NULL"
        if is_primary_key(row.get("constraint")) and len(pk_columns) == 1:
            definition = f"{definition} PRIMARY KEY"
        column_definitions.append(definition)

    if len(pk_columns) > 1:
        column_definitions.append(
            f"PRIMARY KEY ({', '.join(_quote_identifier(column) for column in pk_columns)})"
        )

    qualified_name = _qualified_name(schema_name, table_name)
    return f"CREATE TABLE IF NOT EXISTS {qualified_name} (\n  {',\n  '.join(column_definitions)}\n);"


def build_widen_clob_columns_sql(
    table_name: str,
    rows: List[Dict[str, str]],
    schema_name: str | None = None,
) -> List[str]:
    qualified_name = _qualified_name(schema_name, table_name)

    statements = []
    for row in rows:
        if (
            row.get("source_data_type", "").strip().upper() == "CLOB"
            and _BOUNDED_VARCHAR.fullmatch(row.get("snowflake_datatype", "").strip())
            and row.get("target_column")
        ):
            statements.append(
                f"ALTER TABLE {qualified_name} "
                f"ALTER COLUMN {_quote_identifier(row['target_column'])} SET DATA TYPE VARCHAR"
            )
    return statements


def build_drop_status_column_sql(table_name: str, schema_name: str | None = None) -> str:
    qualified_name = _qualified_name(schema_name, table_name)
    return f"ALTER TABLE {qualified_name} DROP COLUMN IF EXISTS {_quote_identifier('STATUS')}"


def build_timestamp_column_sql(
    table_name: str,
    rows: List[Dict[str, str]],
    schema_name: str | None = None,
) -> List[str]:
    qualified_name = _qualified_name(schema_name, table_name)

    timestamp_columns = [
        row["target_column"]
        for row in rows
        if row.get("target_column", "").strip().upper() in _ETL_TIMESTAMP_COLUMNS
    ]
    statements = []
    for column_name in timestamp_columns:
        quoted_column = _quote_identifier(column_name)
        statements.append(
            f"UPDATE {qualified_name} "
            f"SET {quoted_column} = {_CURRENT_TIMESTAMP_NTZ} "
            f"WHERE {quoted_column} IS NULL"
        )
    return statements


def table_exists_in_snowflake(cursor, schema_name: str, table_name: str) -> bool:
    sql = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
    """
    result = cursor.execute(sql, (schema_name, table_name)).fetchone()
    return (result[0] if result else 0) > 0
