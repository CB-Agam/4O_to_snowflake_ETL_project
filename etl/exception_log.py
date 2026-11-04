from __future__ import annotations

import hashlib
import json
import logging
import traceback
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

logger = logging.getLogger(__name__)


def _quote_identifier(identifier: str) -> str:
    if not identifier.strip():
        raise ValueError("Snowflake database and schema names cannot be blank")
    return '"' + identifier.replace('"', '""') + '"'


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    if hasattr(value, "item"):
        return _json_default(value.item())
    return {"__type__": type(value).__name__, "__repr__": repr(value)}


class SnowflakeExceptionLogger:
    def __init__(self, connection, database: str, schema: str):
        self.connection = connection
        self.qualified_table = ".".join(
            (
                _quote_identifier(database),
                _quote_identifier(schema),
                '"ETL_EXCEPTION_LOG"',
            )
        )

    def ensure_table(self) -> None:
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.qualified_table} (
                    ERROR_ID VARCHAR(64) NOT NULL,
                    PIPELINE_NAME VARCHAR(255) NOT NULL,
                    PIPELINE_RUN_ID VARCHAR(36),
                    SOURCE_TABLE VARCHAR(255) NOT NULL,
                    TARGET_TABLE VARCHAR(255) NOT NULL,
                    ROW_NUMBER NUMBER(38, 0) NOT NULL,
                    RECORD_DATA VARIANT,
                    ERROR_MESSAGE VARCHAR,
                    ERROR_TYPE VARCHAR(255),
                    ERROR_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    BATCH_ID VARCHAR(36),
                    RETRY_STATUS VARCHAR(20) DEFAULT 'PENDING'
                )
                """
            )
            cursor.execute(
                f"ALTER TABLE {self.qualified_table} "
                "ADD COLUMN IF NOT EXISTS PIPELINE_RUN_ID VARCHAR(36)"
            )
        finally:
            cursor.close()

    def log_row_failure(
        self,
        *,
        pipeline_name: str,
        source_table: str,
        target_table: str,
        row_number: int,
        record: dict[str, Any],
        error: BaseException,
        batch_id: str,
        pipeline_run_id: str | None = None,
    ) -> None:
        record_json = json.dumps(
            record,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
            default=_json_default,
        )
        identity = "\0".join(
            (
                pipeline_name,
                source_table,
                target_table,
                str(row_number),
                record_json,
                type(error).__name__,
                str(error),
            )
        )
        error_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        error_message = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
        if not error_message.strip():
            error_message = f"{type(error).__name__}: {error}"

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"""
                MERGE INTO {self.qualified_table} target
                USING (
                    SELECT %s AS ERROR_ID
                ) source
                ON target.ERROR_ID = source.ERROR_ID
                WHEN NOT MATCHED THEN INSERT (
                    ERROR_ID, PIPELINE_NAME, PIPELINE_RUN_ID, SOURCE_TABLE,
                    TARGET_TABLE, ROW_NUMBER, RECORD_DATA, ERROR_MESSAGE, ERROR_TYPE,
                    ERROR_TIMESTAMP, BATCH_ID, RETRY_STATUS
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s,
                    CURRENT_TIMESTAMP(), %s, 'PENDING'
                )
                """,
                (
                    error_id,
                    error_id,
                    pipeline_name,
                    pipeline_run_id,
                    source_table,
                    target_table,
                    row_number,
                    record_json,
                    error_message,
                    type(error).__name__,
                    batch_id,
                ),
            )
        except Exception:
            logger.exception(
                "Could not write row exception for %s row %s in %s",
                source_table,
                row_number,
                target_table,
            )
            raise
        finally:
            cursor.close()

    def log_step_failure(
        self,
        *,
        pipeline_name: str,
        source_table: str,
        target_table: str,
        step_name: str,
        error: BaseException,
        batch_id: str,
        pipeline_run_id: str | None = None,
    ) -> None:
        self.log_row_failure(
            pipeline_name=pipeline_name,
            source_table=source_table,
            target_table=target_table,
            row_number=0,
            record={
                "__failure_scope__": "PIPELINE_STEP",
                "__step__": step_name,
                "__row_data_available__": False,
            },
            error=error,
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
        )
