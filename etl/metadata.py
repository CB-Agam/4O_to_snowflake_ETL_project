from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List

import pandas as pd


def _normalize_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_table_name(value: Any) -> str:
    return _normalize_text(value).upper().replace(" ", "_").replace("-", "_")


def _pick_column_name(columns: Iterable[str], *candidates: str) -> str | None:
    normalized = {str(column).strip().lower(): column for column in columns}
    for candidate in candidates:
        key = str(candidate).strip().lower()
        if key in normalized:
            return normalized[key]
    return None


def table_metadata_from_dataframe(df: pd.DataFrame) -> Dict[str, List[Dict[str, str]]]:
    if df is None or df.empty:
        return {}

    columns = [str(column).strip() for column in df.columns]
    source_table_name = _pick_column_name(columns, "source table", "source_table", "Source table", "table")
    source_column_name = _pick_column_name(columns, "source column", "source_column", "Source column", "column")
    target_table_name = _pick_column_name(columns, "target table", "target_table", "Target table")
    target_column_name = _pick_column_name(columns, "target column", "target_column", "Target column")
    snowflake_datatype_name = _pick_column_name(
        columns,
        "snowflake datatype",
        "snowflake_datatype",
        "Snowflake Datatype",
        "datatype",
    )
    constraint_name = _pick_column_name(columns, "constraint", "Constraint")

    if not all([source_table_name, source_column_name, target_table_name, target_column_name]):
        raise ValueError(
            "Excel sheet is missing one or more required columns: "
            "Source table, Source column, Target table, Target column"
        )

    grouped_rows: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    for _, row in df.iterrows():
        source_table = _normalize_text(row.get(source_table_name))
        source_column = _normalize_text(row.get(source_column_name))
        target_table = _normalize_text(row.get(target_table_name))
        target_column = _normalize_text(row.get(target_column_name))

        if not source_table and not target_table and not source_column and not target_column:
            continue

        snowflake_type = _normalize_text(row.get(snowflake_datatype_name)) if snowflake_datatype_name else "VARCHAR"
        constraint = _normalize_text(row.get(constraint_name)) if constraint_name else ""

        grouped_rows[_normalize_table_name(target_table)].append(
            {
                "source_table": source_table,
                "source_column": source_column,
                "target_table": _normalize_table_name(target_table),
                "target_column": target_column,
                "snowflake_datatype": snowflake_type or "VARCHAR",
                "constraint": constraint,
            }
        )

    ordered = {}
    for table_name in grouped_rows:
        ordered[table_name] = grouped_rows[table_name]
    return ordered


def load_metadata_from_excel(excel_path: str, sheet_name: str = "Snowflake_table_names") -> Dict[str, List[Dict[str, str]]]:
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    return table_metadata_from_dataframe(df)
