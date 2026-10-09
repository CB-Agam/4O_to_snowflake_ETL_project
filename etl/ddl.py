from __future__ import annotations

from typing import Dict, List


def is_primary_key(value: str | None) -> bool:
    if value is None:
        return False
    return "pk" in str(value).strip().lower()


def build_create_table_sql(table_name: str, rows: List[Dict[str, str]], schema_name: str | None = None) -> str:
    if not rows:
        raise ValueError(f"No metadata rows provided for table: {table_name}")

    table_name = str(table_name).strip()
    column_definitions: List[str] = []
    pk_columns = [row["target_column"] for row in rows if is_primary_key(row.get("constraint"))]

    for row in rows:
        column_name = row.get("target_column")
        data_type = row.get("snowflake_datatype") or "VARCHAR"
        if not column_name:
            continue

        definition = f"{column_name} {data_type}"
        if is_primary_key(row.get("constraint")) and len(pk_columns) == 1:
            definition = f"{definition} PRIMARY KEY"
        column_definitions.append(definition)

    if len(pk_columns) > 1:
        column_definitions.append(f"PRIMARY KEY ({', '.join(pk_columns)})")

    qualified_name = table_name
    if schema_name:
        qualified_name = f"{schema_name}.{table_name}"

    return f"CREATE TABLE IF NOT EXISTS {qualified_name} (\n  {',\n  '.join(column_definitions)}\n);"


def table_exists_in_snowflake(cursor, schema_name: str, table_name: str) -> bool:
    sql = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
    """
    result = cursor.execute(sql, (schema_name, table_name)).fetchone()
    return (result[0] if result else 0) > 0
