from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from etl.parquet_snowflake import load_parquet_to_raw_table


MAPPING = [
    {
        "source_table": "Messages",
        "source_column": "Msg_No",
        "source_data_type": "INT32",
        "target_table": "MESSAGES",
        "target_column": "MSG_NO",
    },
    {
        "source_table": "Messages",
        "source_column": "MsgDate",
        "source_data_type": "TIMESTAMP",
        "target_table": "MESSAGES",
        "target_column": "MSGDATE",
    },
]


def _write_parquet(path: Path, run_id: str = "run-001") -> None:
    schema = pa.schema(
        [pa.field("MSG_NO", pa.string()), pa.field("MSGDATE", pa.string())],
        metadata={
            b"run_id": run_id.encode(),
            b"source_table": b"Messages",
            b"target_table": b"MESSAGES",
        },
    )
    table = pa.Table.from_arrays(
        [
            pa.array(["1", "2"], type=pa.string()),
            pa.array(["bad-date", None], type=pa.string()),
        ],
        schema=schema,
    )
    pq.write_table(table, path, compression="snappy")


def _mock_cursor_for_success(cursor, row_count=2):
    desc_rows = [
        ("MSG_NO", "VARCHAR", None, None, None, None, True),
        ("MSGDATE", "VARCHAR", None, None, None, None, True),
        ("ETL_RUN_ID", "VARCHAR(36)", None, None, None, None, False),
        ("ETL_SOURCE_FILE", "VARCHAR", None, None, None, None, False),
        ("ETL_LOADED_AT", "TIMESTAMP_NTZ", None, None, None, None, True),
    ]
    cursor.fetchall.side_effect = [
        [],
        desc_rows,
        [("UPLOADED", "ok")],
        [(row_count, 0)],
    ]
    cursor.fetchone.return_value = (row_count,)

    def execute(sql, *_args):
        if sql.startswith("DESC TABLE"):
            cursor.description = [("name",), ("type",)]
        elif sql.startswith("PUT "):
            cursor.description = [("status",), ("message",)]
        elif sql.startswith("COPY INTO"):
            cursor.description = [("rows_loaded",), ("errors_seen",)]
        return cursor

    cursor.execute.side_effect = execute
    cursor.execute.return_value = cursor


class ParquetSnowflakeTests(unittest.TestCase):
    @patch("etl.parquet_snowflake.release_table_run_lock")
    @patch("etl.parquet_snowflake.acquire_table_run_lock", return_value={"owner": "lock"})
    def test_loads_parquet_into_varchar_raw_table_and_tracks_file(
        self, acquire_lock, release_lock
    ):
        with tempfile.TemporaryDirectory() as temporary_directory:
            parquet_path = Path(temporary_directory) / "messages.parquet"
            _write_parquet(parquet_path)
            cursor = Mock()
            _mock_cursor_for_success(cursor)
            connection = Mock()
            connection.cursor.return_value = cursor

            result = load_parquet_to_raw_table(
                connection,
                "DW_TEST",
                "RAW",
                MAPPING,
                parquet_path,
                "run-001",
            )

            self.assertEqual(result.loaded_rows, 2)
            self.assertEqual(result.parquet_rows, 2)
            self.assertFalse(result.skipped_as_duplicate)
            self.assertEqual(result.raw_table, "MESSAGES_RAW")
            sql = [call.args[0] for call in cursor.execute.call_args_list]
            self.assertTrue(any('"MSG_NO" VARCHAR' in item for item in sql))
            self.assertTrue(any("FILE_FORMAT=(TYPE=PARQUET)" in item for item in sql))
            put_sql = next(item for item in sql if item.startswith("PUT "))
            copy_sql = next(item for item in sql if item.startswith("COPY INTO "))
            self.assertIn(" @", put_sql)
            self.assertIn("/ AUTO_COMPRESS=FALSE", put_sql)
            self.assertIn(" FROM @", copy_sql)
            self.assertIn("/ FILES = ('messages.parquet')", copy_sql)
            self.assertTrue(any("ETL_PARQUET_FILE_LOADS" in item for item in sql))
            self.assertTrue(any("STATUS = 'SUCCESS'" in item for item in sql))
            ledger_insert = next(
                call
                for call in cursor.execute.call_args_list
                if "INSERT INTO" in call.args[0]
                and "ETL_PARQUET_FILE_LOADS" in call.args[0]
            )
            self.assertEqual(ledger_insert.args[1][2], "messages.parquet")
            self.assertEqual(connection.commit.call_count, 2)
            cursor.close.assert_called_once()
            acquire_lock.assert_called_once()
            release_lock.assert_called_once()

    @patch("etl.parquet_snowflake.release_table_run_lock")
    @patch("etl.parquet_snowflake.acquire_table_run_lock", return_value={"owner": "lock"})
    def test_loads_into_temporary_raw_table_without_creating_target_raw_table(
        self, _acquire_lock, _release_lock
    ):
        with tempfile.TemporaryDirectory() as temporary_directory:
            parquet_path = Path(temporary_directory) / "messages.parquet"
            _write_parquet(parquet_path)
            cursor = Mock()
            _mock_cursor_for_success(cursor)
            cursor.fetchall.side_effect = [
                [("SUCCESS", 2)],
                [("UPLOADED", "ok")],
                [(2, 0)],
            ]
            connection = Mock()
            connection.cursor.return_value = cursor

            result = load_parquet_to_raw_table(
                connection,
                "DW_TEST",
                "RAW",
                MAPPING,
                parquet_path,
                "run-001",
                persistent_raw=False,
            )

            self.assertTrue(result.raw_table.startswith('"DW_TEST"."RAW"."ETL_PARQUET_RAW_'))
            self.assertFalse(result.skipped_as_duplicate)
            sql = [call.args[0] for call in cursor.execute.call_args_list]
            self.assertTrue(
                any(statement.startswith("CREATE TEMPORARY TABLE") and "ETL_PARQUET_RAW_" in statement for statement in sql)
            )
            self.assertFalse(
                any("CREATE TABLE IF NOT EXISTS" in statement and "MESSAGES_RAW" in statement for statement in sql)
            )
            self.assertTrue(
                any(statement.startswith("INSERT INTO") and result.raw_table in statement for statement in sql)
            )
            self.assertTrue(any("STATUS = 'RUNNING'" in statement for statement in sql))

    @patch("etl.parquet_snowflake.release_table_run_lock")
    @patch("etl.parquet_snowflake.acquire_table_run_lock", return_value={"owner": "lock"})
    def test_skips_file_hash_already_marked_successful(
        self, _acquire_lock, release_lock
    ):
        with tempfile.TemporaryDirectory() as temporary_directory:
            parquet_path = Path(temporary_directory) / "messages.parquet"
            _write_parquet(parquet_path)
            cursor = Mock()
            cursor.fetchall.return_value = [("SUCCESS", 2)]
            cursor.execute.return_value = cursor
            connection = Mock()
            connection.cursor.return_value = cursor

            result = load_parquet_to_raw_table(
                connection,
                "DW_TEST",
                "RAW",
                MAPPING,
                parquet_path,
                "run-001",
            )

            self.assertTrue(result.skipped_as_duplicate)
            self.assertEqual(result.loaded_rows, 2)
            sql = [call.args[0] for call in cursor.execute.call_args_list]
            self.assertFalse(any(item.startswith("PUT ") for item in sql))
            self.assertFalse(any(item.startswith("COPY INTO ") for item in sql))
            cursor.close.assert_called_once()
            release_lock.assert_called_once()

    def test_rejects_parquet_run_id_mismatch_before_snowflake_connection_use(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            parquet_path = Path(temporary_directory) / "messages.parquet"
            _write_parquet(parquet_path, run_id="different-run")
            connection = Mock()

            with self.assertRaisesRegex(ValueError, "run ID mismatch"):
                load_parquet_to_raw_table(
                    connection,
                    "DW_TEST",
                    "RAW",
                    MAPPING,
                    parquet_path,
                    "run-001",
                )

            connection.cursor.assert_not_called()

    def test_rejects_mapping_collision_with_raw_metadata(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            parquet_path = Path(temporary_directory) / "messages.parquet"
            _write_parquet(parquet_path)
            mapping = [
                *MAPPING,
                {
                    "source_table": "Messages",
                    "source_column": "Etl_Run_Id",
                    "source_data_type": "CLOB",
                    "target_table": "MESSAGES",
                    "target_column": "ETL_RUN_ID",
                },
            ]
            connection = Mock()

            with self.assertRaisesRegex(ValueError, "collide with raw-load metadata"):
                load_parquet_to_raw_table(
                    connection,
                    "DW_TEST",
                    "RAW",
                    mapping,
                    parquet_path,
                    "run-001",
                )

            connection.cursor.assert_not_called()

    @patch("etl.parquet_snowflake.release_table_run_lock")
    @patch("etl.parquet_snowflake.acquire_table_run_lock", return_value={"owner": "lock"})
    def test_copy_error_does_not_insert_raw_rows_and_marks_file_failed(
        self, _acquire_lock, _release_lock
    ):
        with tempfile.TemporaryDirectory() as temporary_directory:
            parquet_path = Path(temporary_directory) / "messages.parquet"
            _write_parquet(parquet_path)
            cursor = Mock()
            desc_rows = [
                ("MSG_NO", "VARCHAR", None, None, None, None, True),
                ("MSGDATE", "VARCHAR", None, None, None, None, True),
                ("ETL_RUN_ID", "VARCHAR(36)", None, None, None, None, False),
                ("ETL_SOURCE_FILE", "VARCHAR", None, None, None, None, False),
                ("ETL_LOADED_AT", "TIMESTAMP_NTZ", None, None, None, None, True),
            ]
            cursor.fetchall.side_effect = [
                [],
                desc_rows,
                [("UPLOADED", "ok")],
                [(0, 1)],
            ]

            def execute(sql, *_args):
                if sql.startswith("DESC TABLE"):
                    cursor.description = [("name",), ("type",)]
                elif sql.startswith("PUT "):
                    cursor.description = [("status",), ("message",)]
                elif sql.startswith("COPY INTO"):
                    cursor.description = [("rows_loaded",), ("errors_seen",)]
                return cursor

            cursor.execute.side_effect = execute
            cursor.execute.return_value = cursor
            connection = Mock()
            connection.cursor.return_value = cursor

            with self.assertRaisesRegex(RuntimeError, "reported 1 error"):
                load_parquet_to_raw_table(
                    connection,
                    "DW_TEST",
                    "RAW",
                    MAPPING,
                    parquet_path,
                    "run-001",
                )

            sql = [call.args[0] for call in cursor.execute.call_args_list]
            self.assertFalse(any(item.startswith("INSERT INTO") for item in sql))
            self.assertTrue(any("STATUS = 'FAILED'" in item for item in sql))
            self.assertTrue(any(item.startswith("DROP TABLE IF EXISTS") for item in sql))


if __name__ == "__main__":
    unittest.main()
