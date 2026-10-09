from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence

import pandas as pd

from etl.ddl import build_create_table_sql, is_primary_key


def get_table_rows(metadata: Dict[str, List[Dict[str, str]]], table_name: str) -> List[Dict[str, str]]:
    normalized_name = str(table_name).strip().upper().replace(" ", "_").replace("-", "_")
    return metadata.get(normalized_name, [])


def build_source_query(source_table: str, source_columns: Sequence[str]) -> str:
    columns = ", ".join(str(column).strip() for column in source_columns if str(column).strip())
    if not columns:
        raise ValueError(f"No source columns provided for table: {source_table}")
    return f"SELECT {columns} FROM {source_table};"


def build_insert_sql(table_name: str, target_columns: Sequence[str], schema_name: str | None = None) -> str:
    qualified_name = table_name
    if schema_name:
        qualified_name = f"{schema_name}.{table_name}"
    columns_sql = ", ".join(target_columns)
    placeholders = ", ".join(["%s"] * len(target_columns))
    return f"INSERT INTO {qualified_name} ({columns_sql}) VALUES ({placeholders})"


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime()
        except Exception:
            return value
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def load_dataframe_to_snowflake(
    snowflake_cursor,
    table_name: str,
    target_columns: Sequence[str],
    df: pd.DataFrame,
    schema_name: str | None = None,
) -> int:
    if df.empty:
        return 0

    ordered_columns = list(target_columns)
    df = df.copy()
    df = df.loc[:, ordered_columns]
    rows = [tuple(_clean_value(value) for value in row) for row in df.itertuples(index=False, name=None)]

    if not rows:
        return 0

    sql = build_insert_sql(table_name, ordered_columns, schema_name=schema_name)
    snowflake_cursor.executemany(sql, rows)
    return len(rows)


def create_missing_tables_from_metadata(cursor, metadata: Dict[str, List[Dict[str, str]]], schema_name: str) -> List[str]:
    created_tables: List[str] = []
    for table_name, rows in metadata.items():
        if not rows:
            continue

        if table_exists_in_snowflake(cursor, schema_name, table_name):
            continue

        ddl = build_create_table_sql(table_name, rows, schema_name=schema_name)
        cursor.execute(ddl)
        created_tables.append(table_name)
    return created_tables


def table_exists_in_snowflake(cursor, schema_name: str, table_name: str) -> bool:
    sql = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
    """
    result = cursor.execute(sql, (schema_name, table_name)).fetchone()
    return (result[0] if result else 0) > 0
