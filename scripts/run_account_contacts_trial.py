from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from connections.snowflake_connection import connect_to_snowflake
from etl.ddl import build_create_table_sql
from etl.load import load_dataframe_to_snowflake
from etl.metadata import load_metadata_from_excel

logger = logging.getLogger(__name__)
SOURCE_TABLE = "ACCOUNT_CONTACTS"
MAPPING_WORKBOOK = PROJECT_ROOT / "4D_to_Snowflake (1-50 tables mapping).xlsx"
MAPPING_SHEET = "Account Contacts"
TRIAL_TABLE = "ACCOUNT_CONTACTS_TRIAL"
HISTORY_TABLE = f"{TRIAL_TABLE}_H"
CONTROL_TABLE = f"{TRIAL_TABLE}_CONTROL"
EXPECTED_FIXTURE_ROWS = 24
EXPECTED_INITIAL_ROWS = 23
_MANAGED_TIMESTAMP_COLUMNS = {"CREATED_TIMESTAMP", "UPDATED_TIMESTAMP"}


def _quote_identifier(identifier: str) -> str:
    if not identifier:
        raise ValueError("Snowflake identifiers cannot be blank")
    return '"' + identifier.replace('"', '""') + '"'


def _qualified_name(database: str, schema: str, table_name: str) -> str:
    return ".".join(
        (
            _quote_identifier(database),
            _quote_identifier(schema),
            _quote_identifier(table_name),
        )
    )


def prepare_trial_batches(
    dataframe: pd.DataFrame,
    today: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if len(dataframe) != EXPECTED_FIXTURE_ROWS:
        raise ValueError(
            f"Expected exactly {EXPECTED_FIXTURE_ROWS} sample records, got {len(dataframe)}"
        )
    columns_by_folded_name = {str(column).strip().casefold(): column for column in dataframe.columns}
    required = {"contact_id", "modified_on"}
    missing = required - set(columns_by_folded_name)
    if missing:
        raise ValueError(
            "Fixture is missing required columns: " + ", ".join(sorted(missing))
        )

    contact_column = columns_by_folded_name["contact_id"]
    modified_column = columns_by_folded_name["modified_on"]
    parsed_timestamps = pd.to_datetime(dataframe[modified_column], errors="coerce")
    latest_timestamp = parsed_timestamps.max()
    if pd.isna(latest_timestamp):
        raise ValueError("Fixture has no parseable MODIFIED_ON date to identify the latest row")
    latest_positions = [
        position
        for position, value in enumerate(parsed_timestamps.tolist())
        if not pd.isna(value) and value == latest_timestamp
    ]
    if len(latest_positions) != 1:
        raise ValueError(
            "Expected exactly one row with the latest MODIFIED_ON timestamp, "
            f"found {len(latest_positions)}"
        )

    latest_position = latest_positions[0]
    latest_contact_id = dataframe.iloc[latest_position][contact_column]
    contact_ids = dataframe[contact_column].astype(str).str.strip()
    if contact_ids.eq("").any() or not str(latest_contact_id).strip():
        raise ValueError("Fixture contains a blank CONTACT_ID")
    if contact_ids.duplicated().any():
        raise ValueError("Fixture CONTACT_ID values are not unique")

    day_zero = dataframe.drop(dataframe.index[latest_position]).reset_index(drop=True)
    day_one = dataframe.iloc[[latest_position]].copy().reset_index(drop=True)
    effective_today = today or datetime.now()
    day_one[modified_column] = effective_today.strftime("%Y-%m-%d %H:%M:%S.%f")
    return day_zero, day_one


def _load_fixture(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not path.is_file():
        raise FileNotFoundError(
            f"Sample CSV not found: {path}. Save the supplied 24-row table as a CSV first."
        )
    dataframe = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    return prepare_trial_batches(dataframe)


def _table_columns(cursor, qualified_table: str) -> list[str]:
    cursor.execute(f"DESC TABLE {qualified_table}")
    columns = [str(row[0]) for row in cursor.fetchall()]
    if not columns:
        raise RuntimeError(f"Could not read columns for {qualified_table}")
    return columns


def _prepare_trial_objects(
    cursor,
    database: str,
    schema: str,
    mapping_rows: list[dict[str, str]],
) -> dict[str, str]:
    trial = _qualified_name(database, schema, TRIAL_TABLE)
    history = _qualified_name(database, schema, HISTORY_TABLE)
    control = _qualified_name(database, schema, CONTROL_TABLE)

    target_rows = [row for row in mapping_rows if row.get("target_column")]
    if not target_rows:
        raise ValueError(f"No target column mappings found for {SOURCE_TABLE}")
    schema_name = ".".join(
        (_quote_identifier(database), _quote_identifier(schema))
    )
    cursor.execute(
        build_create_table_sql(
            _quote_identifier(TRIAL_TABLE),
            target_rows,
            schema_name=schema_name,
        )
    )
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {history} LIKE {trial}")
    cursor.execute(
        f"ALTER TABLE {history} ADD COLUMN IF NOT EXISTS HISTORY_BATCH_ID VARCHAR(36)"
    )
    cursor.execute(
        f"ALTER TABLE {history} ADD COLUMN IF NOT EXISTS ARCHIVED_AT TIMESTAMP_NTZ"
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {control} (
            PHASE VARCHAR(10) NOT NULL,
            BATCH_ID VARCHAR(36) NOT NULL,
            ROW_COUNT NUMBER(38, 0) NOT NULL,
            COMPLETED_AT TIMESTAMP_NTZ NOT NULL
        )
        """
    )
    return {"trial": trial, "history": history, "control": control}


def _row_count(cursor, table_name: str) -> int:
    return int(cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def _phase_completed(cursor, control_table: str, phase: str) -> bool:
    result = cursor.execute(
        f"SELECT COUNT(*) FROM {control_table} WHERE PHASE = %s",
        (phase,),
    ).fetchone()
    return bool(result and result[0])


def _validated_frame(
    dataframe: pd.DataFrame,
    table_columns: list[str],
    mapping_rows: list[dict[str, str]],
) -> tuple[list[str], pd.DataFrame]:
    csv_columns = {str(column).strip().casefold(): column for column in dataframe.columns}
    target_by_folded_name = {column.casefold(): column for column in table_columns}
    missing = set(target_by_folded_name) - set(csv_columns) - {
        column.casefold() for column in _MANAGED_TIMESTAMP_COLUMNS
    }
    extra = set(csv_columns) - set(target_by_folded_name)
    if missing or extra:
        details = []
        if missing:
            details.append("missing: " + ", ".join(sorted(missing)))
        if extra:
            details.append("not in trial target schema: " + ", ".join(sorted(extra)))
        raise ValueError("Fixture columns do not match ACCOUNT_CONTACTS: " + "; ".join(details))

    target_source_columns = [
        target_by_folded_name[column.casefold()]
        for column in table_columns
        if column.casefold() in csv_columns
    ]
    prepared = dataframe.rename(
        columns={
            csv_columns[column.casefold()]: column
            for column in target_source_columns
        }
    )
    snowflake_types = {
        str(row.get("target_column", "")).strip().casefold(): str(
            row.get("snowflake_datatype", "")
        ).strip().upper()
        for row in mapping_rows
        if row.get("target_column")
    }
    for column in target_source_columns:
        data_type = snowflake_types.get(column.casefold(), "")
        if data_type.startswith(
            ("NUMBER", "INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "BOOLEAN", "DATE", "TIME", "TIMESTAMP")
        ):
            prepared[column] = prepared[column].map(
                lambda value: None
                if isinstance(value, str) and not value.strip()
                else value
            )
    return target_source_columns, prepared


def run_trial_phase(phase: str, fixture_path: Path) -> dict[str, int | str]:
    normalized_phase = phase.strip().upper()
    if normalized_phase not in {"DAY0", "DAY1"}:
        raise ValueError("Trial phase must be DAY0 or DAY1")
    day_zero, day_one = _load_fixture(fixture_path)
    metadata = load_metadata_from_excel(str(MAPPING_WORKBOOK), sheet_name=MAPPING_SHEET)
    mapping_rows = metadata.get(SOURCE_TABLE, [])
    if not mapping_rows:
        raise ValueError(
            f"No {SOURCE_TABLE} mappings found in worksheet {MAPPING_SHEET!r} "
            f"of {MAPPING_WORKBOOK}"
        )
    connection = connect_to_snowflake()
    cursor = connection.cursor()
    batch_id = str(uuid4())
    transaction_started = False
    try:
        objects = _prepare_trial_objects(
            cursor,
            settings.snowflake_database,
            settings.snowflake_schema,
            mapping_rows,
        )
        columns = _table_columns(cursor, objects["trial"])
        source_frame = day_zero if normalized_phase == "DAY0" else day_one
        source_columns, prepared_frame = _validated_frame(
            source_frame,
            columns,
            mapping_rows,
        )

        current_count = _row_count(cursor, objects["trial"])
        history_count = _row_count(cursor, objects["history"])
        day_zero_done = _phase_completed(cursor, objects["control"], "DAY0")
        day_one_done = _phase_completed(cursor, objects["control"], "DAY1")

        if normalized_phase == "DAY0":
            if day_zero_done or day_one_done or current_count or history_count:
                raise RuntimeError(
                    "Trial tables are not empty or a trial phase already completed. "
                    "Use a fresh development schema or explicitly clear only the "
                    "ACCOUNT_CONTACTS_TRIAL* objects before restarting."
                )
            expected_rows = EXPECTED_INITIAL_ROWS
        else:
            if not day_zero_done:
                raise RuntimeError("Run Day 0 successfully before running Day 1")
            if day_one_done:
                raise RuntimeError(
                    "Day 1 has already completed; refusing to archive the same batch again"
                )
            if current_count != EXPECTED_INITIAL_ROWS or history_count:
                raise RuntimeError(
                    "Day 1 requires exactly 23 current trial rows and an empty history table; "
                    f"found current={current_count}, history={history_count}"
                )
            expected_rows = 1

        if len(prepared_frame) != expected_rows:
            raise RuntimeError(
                f"{normalized_phase} fixture split returned {len(prepared_frame)} rows; "
                f"expected {expected_rows}"
            )

        stage_table_name = f"ETL_STAGE_ACCOUNT_CONTACTS_TRIAL_{batch_id.replace('-', '')[:12].upper()}"
        stage = _qualified_name(
            settings.snowflake_database,
            settings.snowflake_schema,
            stage_table_name,
        )
        cursor.execute(f"CREATE TEMPORARY TABLE {stage} LIKE {objects['trial']}")
        staged_rows = load_dataframe_to_snowflake(
            cursor,
            stage_table_name,
            source_columns,
            prepared_frame,
            schema_name=(
                f"{_quote_identifier(settings.snowflake_database)}."
                f"{_quote_identifier(settings.snowflake_schema)}"
            ),
        )
        if staged_rows != expected_rows:
            raise RuntimeError(f"Staged {staged_rows} rows; expected {expected_rows}")

        column_list = ", ".join(_quote_identifier(column) for column in columns)

        cursor.execute("BEGIN")
        transaction_started = True
        if normalized_phase == "DAY1":
            history_targets = ", ".join(
                [column_list, '"HISTORY_BATCH_ID"', '"ARCHIVED_AT"']
            )
            cursor.execute(
                f"""
                INSERT INTO {objects['history']} ({history_targets})
                SELECT {column_list}, %s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                FROM {objects['trial']}
                """,
                (batch_id,),
            )
            cursor.execute(f"DELETE FROM {objects['trial']}")

        cursor.execute(
            f"""
            INSERT INTO {objects['trial']} ({column_list})
            SELECT {column_list}
            FROM {stage}
            """
        )
        cursor.execute(
            f"""
            INSERT INTO {objects['control']} (PHASE, BATCH_ID, ROW_COUNT, COMPLETED_AT)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
            """,
            (normalized_phase, batch_id, expected_rows),
        )
        cursor.execute("COMMIT")
        transaction_started = False

        return {
            "phase": normalized_phase,
            "batch_id": batch_id,
            "current_rows": _row_count(cursor, objects["trial"]),
            "history_rows": _row_count(cursor, objects["history"]),
        }
    except BaseException:
        if transaction_started:
            connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Safely simulate the ACCOUNT_CONTACTS Day 0/Day 1 current/history flow "
            "using an exported 24-row CSV and isolated trial tables."
        )
    )
    parser.add_argument(
        "--phase",
        required=True,
        choices=("day0", "day1"),
        help="Run day0 (load 23 rows) or day1 (archive 23 and load the latest row).",
    )
    parser.add_argument(
        "--fixture",
        default=str(PROJECT_ROOT / "ACCOUNT_CONTACTS_trial.csv"),
        help="Path to the CSV exported from the supplied 24-row sample.",
    )
    return parser.parse_args(argv)


def main() -> int:
    args = _parse_args()
    result = run_trial_phase(args.phase, Path(args.fixture))
    print(
        f"{result['phase']} succeeded: "
        f"ACCOUNT_CONTACTS_TRIAL={result['current_rows']} rows, "
        f"ACCOUNT_CONTACTS_TRIAL_H={result['history_rows']} rows, "
        f"batch_id={result['batch_id']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
