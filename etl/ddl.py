from __future__ import annotations

import re
from typing import Dict, List


_BOUNDED_VARCHAR = re.compile(r"^VARCHAR\s*\(\s*\d+\s*\)$", re.IGNORECASE)
_ETL_TIMESTAMP_COLUMNS = {"CREATED_TIMESTAMP", "UPDATED_TIMESTAMP"}
_ETL_TIMESTAMP_TYPE = "TIMESTAMP_NTZ"
_CURRENT_TIMESTAMP_NTZ = "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"


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
        definition = f"{column_name} {data_type}"
        if is_etl_timestamp:
            definition = f"{definition} DEFAULT {_CURRENT_TIMESTAMP_NTZ} NOT NULL"
        if is_primary_key(row.get("constraint")) and len(pk_columns) == 1:
            definition = f"{definition} PRIMARY KEY"
        column_definitions.append(definition)

    if len(pk_columns) > 1:
        column_definitions.append(f"PRIMARY KEY ({', '.join(pk_columns)})")

    qualified_name = table_name
    if schema_name:
        qualified_name = f"{schema_name}.{table_name}"

    return f"CREATE TABLE IF NOT EXISTS {qualified_name} (\n  {',\n  '.join(column_definitions)}\n);"


def build_widen_clob_columns_sql(
    table_name: str,
    rows: List[Dict[str, str]],
    schema_name: str | None = None,
) -> List[str]:
    qualified_name = table_name
    if schema_name:
        qualified_name = f"{schema_name}.{table_name}"

    statements = []
    for row in rows:
        if (
            row.get("source_data_type", "").strip().upper() == "CLOB"
            and _BOUNDED_VARCHAR.fullmatch(row.get("snowflake_datatype", "").strip())
            and row.get("target_column")
        ):
            statements.append(
                f"ALTER TABLE {qualified_name} "
                f"ALTER COLUMN {row['target_column']} SET DATA TYPE VARCHAR"
            )
    return statements


def build_drop_status_column_sql(table_name: str, schema_name: str | None = None) -> str:
    qualified_name = table_name
    if schema_name:
        qualified_name = f"{schema_name}.{table_name}"
    return f"ALTER TABLE {qualified_name} DROP COLUMN IF EXISTS STATUS"


def build_timestamp_column_sql(
    table_name: str,
    rows: List[Dict[str, str]],
    schema_name: str | None = None,
) -> List[str]:
    qualified_name = table_name
    if schema_name:
        qualified_name = f"{schema_name}.{table_name}"

    timestamp_columns = [
        row["target_column"]
        for row in rows
        if row.get("target_column", "").strip().upper() in _ETL_TIMESTAMP_COLUMNS
    ]
    statements = []
    for column_name in timestamp_columns:
        statements.append(
            f"UPDATE {qualified_name} "
            f"SET {column_name} = {_CURRENT_TIMESTAMP_NTZ} "
            f"WHERE {column_name} IS NULL"
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
