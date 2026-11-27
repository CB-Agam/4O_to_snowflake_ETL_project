from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


TABLE_STATUS_TABLE = "ETL_TABLE_STATUS"


@dataclass(frozen=True)
class TableStatusRow:
    batch_id: str
    table_name: str
    status: str
    failed_at_step: str | None
    parquet_path: str | None
    expected_rows: int | None


def _quote_identifier(identifier: str) -> str:
    value = str(identifier).strip()
    if not value or "\x00" in value:
        raise ValueError(f"Invalid Snowflake identifier: {identifier!r}")
    return '"' + value.replace('"', '""') + '"'


def _qualified_table(database: str, schema: str) -> str:
    return ".".join(
        _quote_identifier(part)
        for part in (database, schema, TABLE_STATUS_TABLE)
    )


class TableStatusStore:
    def __init__(self, connection: Any, database: str, schema: str):
        self.connection = connection
        self.qualified_table = _qualified_table(database, schema)

    def ensure_table(self) -> None:
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.qualified_table} (
                    BATCH_ID VARCHAR(255) NOT NULL,
                    TABLE_NAME VARCHAR(255) NOT NULL,
                    STATUS VARCHAR(20) NOT NULL,
                    FAILED_AT_STEP VARCHAR(20),
                    PARQUET_PATH VARCHAR(2048),
                    ARCHIVE_PATH VARCHAR(2048),
                    EXPECTED_ROWS NUMBER(38, 0),
                    LOADED_ROWS NUMBER(38, 0),
                    EXTRACTION_ATTEMPTS NUMBER(38, 0) NOT NULL DEFAULT 0,
                    UPLOAD_ATTEMPTS NUMBER(38, 0) NOT NULL DEFAULT 0,
                    ERROR_MESSAGE VARCHAR,
                    CREATED_AT TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                    EXTRACTION_STARTED_AT TIMESTAMP_NTZ,
                    EXTRACTION_COMPLETED_AT TIMESTAMP_NTZ,
                    UPLOAD_STARTED_AT TIMESTAMP_NTZ,
                    UPLOAD_COMPLETED_AT TIMESTAMP_NTZ
                )
                """
            )
        finally:
            cursor.close()

    def register_batch(self, batch_id: str, table_names: list[str]) -> None:
        cursor = self.connection.cursor()
        try:
            for table_name in table_names:
                cursor.execute(
                    f"""
                    MERGE INTO {self.qualified_table} AS target
                    USING (SELECT %s AS BATCH_ID, %s AS TABLE_NAME) AS source
                      ON target.BATCH_ID = source.BATCH_ID
                     AND target.TABLE_NAME = source.TABLE_NAME
                    WHEN NOT MATCHED THEN
                      INSERT (BATCH_ID, TABLE_NAME, STATUS)
                      VALUES (source.BATCH_ID, source.TABLE_NAME, 'PENDING')
                    """,
                    (batch_id, table_name),
                )
            self.connection.commit()
        finally:
            cursor.close()

    def begin_extraction(self, batch_id: str, table_name: str) -> bool:
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"""
                UPDATE {self.qualified_table}
                SET STATUS = 'EXTRACTING',
                    FAILED_AT_STEP = NULL,
                    ERROR_MESSAGE = NULL,
                    EXTRACTION_ATTEMPTS = EXTRACTION_ATTEMPTS + 1,
                    EXTRACTION_STARTED_AT = CURRENT_TIMESTAMP(),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE BATCH_ID = %s AND TABLE_NAME = %s
                  AND (
                    STATUS = 'PENDING'
                    OR (STATUS = 'FAILED' AND FAILED_AT_STEP = 'EXTRACTION')
                  )
                """,
                (batch_id, table_name),
            )
            changed = cursor.rowcount
            self.connection.commit()
            if changed == 1:
                return True
        finally:
            cursor.close()

        row = self.get_row(batch_id, table_name)
        if row.status in {"READY", "UPLOADING", "UPLOADED"}:
            return False
        if row.status == "FAILED" and row.failed_at_step == "UPLOAD":
            return False
        if row.status == "EXTRACTING":
            raise RuntimeError(
                f"Extraction is already in progress for {table_name} in batch {batch_id}"
            )
        raise RuntimeError(
            f"Cannot start extraction for {table_name} in batch {batch_id} "
            f"from status {row.status}"
        )

    def mark_ready(
        self,
        batch_id: str,
        table_name: str,
        parquet_path: Path,
        expected_rows: int,
    ) -> None:
        self._update(
            f"""
            UPDATE {self.qualified_table}
            SET STATUS = 'READY',
                FAILED_AT_STEP = NULL,
                PARQUET_PATH = %s,
                EXPECTED_ROWS = %s,
                LOADED_ROWS = NULL,
                ERROR_MESSAGE = NULL,
                EXTRACTION_COMPLETED_AT = CURRENT_TIMESTAMP(),
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE BATCH_ID = %s AND TABLE_NAME = %s AND STATUS = 'EXTRACTING'
            """,
            (parquet_path.as_posix(), expected_rows, batch_id, table_name),
            f"Could not mark {table_name} READY for batch {batch_id}",
        )

    def mark_extraction_failed(
        self,
        batch_id: str,
        table_name: str,
        error_message: str,
    ) -> None:
        self._update(
            f"""
            UPDATE {self.qualified_table}
            SET STATUS = 'FAILED',
                FAILED_AT_STEP = 'EXTRACTION',
                ERROR_MESSAGE = %s,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE BATCH_ID = %s AND TABLE_NAME = %s
              AND STATUS IN ('PENDING', 'EXTRACTING')
            """,
            (error_message, batch_id, table_name),
            f"Could not mark {table_name} extraction FAILED for batch {batch_id}",
        )

    def eligible_batches(self) -> list[str]:
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"""
                SELECT DISTINCT BATCH_ID
                FROM {self.qualified_table}
                WHERE STATUS = 'READY'
                   OR (STATUS = 'FAILED' AND FAILED_AT_STEP = 'UPLOAD')
                ORDER BY BATCH_ID
                """
            )
            return [str(row[0]) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def eligible_rows(self, batch_id: str) -> list[TableStatusRow]:
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"""
                SELECT BATCH_ID, TABLE_NAME, STATUS, FAILED_AT_STEP,
                       PARQUET_PATH, EXPECTED_ROWS
                FROM {self.qualified_table}
                WHERE BATCH_ID = %s
                  AND (
                    STATUS = 'READY'
                    OR (STATUS = 'FAILED' AND FAILED_AT_STEP = 'UPLOAD')
                  )
                ORDER BY UPDATED_AT, TABLE_NAME
                """,
                (batch_id,),
            )
            return [
                TableStatusRow(
                    batch_id=str(row[0]),
                    table_name=str(row[1]),
                    status=str(row[2]),
                    failed_at_step=str(row[3]) if row[3] is not None else None,
                    parquet_path=str(row[4]) if row[4] is not None else None,
                    expected_rows=int(row[5]) if row[5] is not None else None,
                )
                for row in cursor.fetchall()
            ]
        finally:
            cursor.close()

    def begin_upload(self, batch_id: str, table_name: str) -> bool:
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"""
                UPDATE {self.qualified_table}
                SET STATUS = 'UPLOADING',
                    FAILED_AT_STEP = NULL,
                    ERROR_MESSAGE = NULL,
                    UPLOAD_ATTEMPTS = UPLOAD_ATTEMPTS + 1,
                    UPLOAD_STARTED_AT = CURRENT_TIMESTAMP(),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE BATCH_ID = %s AND TABLE_NAME = %s
                  AND (
                    STATUS = 'READY'
                    OR (STATUS = 'FAILED' AND FAILED_AT_STEP = 'UPLOAD')
                  )
                """,
                (batch_id, table_name),
            )
            changed = cursor.rowcount
            self.connection.commit()
            return changed == 1
        finally:
            cursor.close()

    def mark_uploaded(
        self,
        batch_id: str,
        table_name: str,
        loaded_rows: int,
    ) -> None:
        self._update(
            f"""
            UPDATE {self.qualified_table}
            SET STATUS = 'UPLOADED',
                FAILED_AT_STEP = NULL,
                LOADED_ROWS = %s,
                ERROR_MESSAGE = NULL,
                UPLOAD_COMPLETED_AT = CURRENT_TIMESTAMP(),
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE BATCH_ID = %s AND TABLE_NAME = %s AND STATUS = 'UPLOADING'
            """,
            (loaded_rows, batch_id, table_name),
            f"Could not mark {table_name} UPLOADED for batch {batch_id}",
        )

    def mark_upload_failed(
        self,
        batch_id: str,
        table_name: str,
        error_message: str,
    ) -> None:
        self._update(
            f"""
            UPDATE {self.qualified_table}
            SET STATUS = 'FAILED',
                FAILED_AT_STEP = 'UPLOAD',
                ERROR_MESSAGE = %s,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE BATCH_ID = %s AND TABLE_NAME = %s AND STATUS = 'UPLOADING'
            """,
            (error_message, batch_id, table_name),
            f"Could not mark {table_name} upload FAILED for batch {batch_id}",
        )

    def record_archive(
        self,
        batch_id: str,
        table_name: str,
        archive_path: Path,
    ) -> None:
        self._update(
            f"""
            UPDATE {self.qualified_table}
            SET ARCHIVE_PATH = %s, UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE BATCH_ID = %s AND TABLE_NAME = %s AND STATUS = 'UPLOADED'
            """,
            (archive_path.as_posix(), batch_id, table_name),
            f"Could not record archive path for {table_name} in batch {batch_id}",
        )

    def get_row(self, batch_id: str, table_name: str) -> TableStatusRow:
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"""
                SELECT BATCH_ID, TABLE_NAME, STATUS, FAILED_AT_STEP,
                       PARQUET_PATH, EXPECTED_ROWS
                FROM {self.qualified_table}
                WHERE BATCH_ID = %s AND TABLE_NAME = %s
                """,
                (batch_id, table_name),
            )
            rows = cursor.fetchall()
            if len(rows) != 1:
                raise RuntimeError(
                    f"Expected one status row for {table_name} in batch {batch_id}; "
                    f"found {len(rows)}"
                )
            row = rows[0]
            return TableStatusRow(
                batch_id=str(row[0]),
                table_name=str(row[1]),
                status=str(row[2]),
                failed_at_step=str(row[3]) if row[3] is not None else None,
                parquet_path=str(row[4]) if row[4] is not None else None,
                expected_rows=int(row[5]) if row[5] is not None else None,
            )
        finally:
            cursor.close()

    def _update(self, sql: str, parameters: tuple[Any, ...], error_message: str) -> None:
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, parameters)
            changed = cursor.rowcount
            self.connection.commit()
            if changed != 1:
                raise RuntimeError(f"{error_message}; updated {changed} row(s)")
        finally:
            cursor.close()
