from __future__ import annotations

import logging
import traceback
from collections.abc import Callable
from typing import TypeVar
from uuid import uuid4


_T = TypeVar("_T")
logger = logging.getLogger(__name__)


def _quote_snowflake_identifier(identifier: str) -> str:
    if not identifier.strip():
        raise ValueError("Snowflake database and schema names cannot be blank")
    return '"' + identifier.replace('"', '""') + '"'


class SnowflakeAuditLogger:
    def __init__(self, connection, database: str, schema: str):
        self.connection = connection
        self.run_id = str(uuid4())
        qualified_table = ".".join(
            (
                _quote_snowflake_identifier(database),
                _quote_snowflake_identifier(schema),
                '"ETL_PIPELINE_LOG"',
            )
        )
        self.qualified_table = qualified_table

    def ensure_table(self) -> None:
        sql = f"""
            CREATE TABLE IF NOT EXISTS {self.qualified_table} (
                S_NO NUMBER(38, 0) AUTOINCREMENT START 1 INCREMENT 1,
                RUN_ID VARCHAR(36) NOT NULL,
                TABLE_NAME VARCHAR(255),
                STEP VARCHAR(100) NOT NULL,
                STATUS VARCHAR(20) NOT NULL,
                MESSAGE VARCHAR,
                ERROR_MESSAGE VARCHAR,
                TIME_STAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()

    def record(
        self,
        step: str,
        status: str,
        table_name: str | None = None,
        message: str | None = None,
        error_message: str | None = None,
    ) -> None:
        sql = f"""
            INSERT INTO {self.qualified_table}
                (RUN_ID, TABLE_NAME, STEP, STATUS, MESSAGE, ERROR_MESSAGE)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                sql,
                (self.run_id, table_name, step, status, message, error_message),
            )
        finally:
            cursor.close()

    def run_step(
        self,
        step: str,
        action: Callable[[], _T],
        table_name: str | None = None,
    ) -> _T:
        self.record(step, "STARTED", table_name, f"{step} started")
        try:
            result = action()
        except Exception as step_error:
            error_message = traceback.format_exc()
            try:
                self.record(
                    step,
                    "FAILURE",
                    table_name,
                    f"{step} failed: {step_error}",
                    error_message,
                )
            except Exception as audit_error:
                logger.exception("Could not store failed step %s in Snowflake audit table", step)
                raise step_error from audit_error
            raise

        self.record(step, "SUCCESS", table_name, f"{step} completed successfully")
        return result
