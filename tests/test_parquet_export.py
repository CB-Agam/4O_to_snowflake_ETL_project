from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pyarrow.parquet as pq

from etl.parquet_export import (
    _normalize_timestamp_text,
    export_table_to_parquet,
)


def _messages_mapping():
    return [
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
            "snowflake_datatype": "TIMESTAMP_NTZ",
        },
        {
            "source_table": "Messages",
            "source_column": "Amount",
            "source_data_type": "DECIMAL",
            "target_table": "MESSAGES",
            "target_column": "AMOUNT",
        },
    ]


class ParquetExportTests(unittest.TestCase):
    def test_writes_all_fetch_chunks_to_one_parquet_file_with_raw_values(self):
        count_before = Mock()
        count_before.fetchone.return_value = (3,)
        source_cursor = Mock()
        source_cursor.fetchmany.side_effect = [
            [
                (1, "00/00/0 00:00:00", Decimal("308905.57999999996")),
                (2, "2007-05-28T00:00:00", None),
            ],
            [(3, "2023/08/25 15:03:00.000", Decimal("1.2500"))],
            [],
        ]
        count_after = Mock()
        count_after.fetchone.return_value = (3,)
        connection = Mock()
        connection.cursor.side_effect = [count_before, source_cursor, count_after]

        with tempfile.TemporaryDirectory() as temporary_directory:
            result = export_table_to_parquet(
                connection,
                _messages_mapping(),
                temporary_directory,
                run_id="run-001",
                fetch_size=2,
            )

            self.assertTrue(result.file_path.is_file())
            self.assertEqual(result.row_count, 3)
            self.assertEqual(result.file_size_bytes, result.file_path.stat().st_size)
            self.assertEqual(result.source_count_before, 3)
            self.assertEqual(result.source_count_after, 3)
            self.assertGreaterEqual(result.query_seconds, 0)
            self.assertGreaterEqual(result.fetch_seconds, 0)
            self.assertGreaterEqual(result.serialization_seconds, 0)
            self.assertGreaterEqual(result.parquet_write_seconds, 0)
            self.assertEqual(
                list(Path(temporary_directory).rglob("*.parquet")),
                [result.file_path],
            )
            source_cursor.fetchmany.assert_any_call(2)
            source_cursor.close.assert_called_once()

            with result.file_path.open("rb") as parquet_source:
                parquet_metadata = pq.read_metadata(parquet_source)
                self.assertEqual(parquet_metadata.num_rows, 3)
                self.assertEqual(
                    parquet_metadata.row_group(0).column(0).compression,
                    "SNAPPY",
                )
            loaded = pq.read_table(result.file_path).to_pylist()
            self.assertEqual(
                loaded,
                [
                    {
                        "MSG_NO": "1",
                        "MSGDATE": None,
                        "AMOUNT": "308905.57999999996",
                    },
                    {
                        "MSG_NO": "2",
                        "MSGDATE": "2007-05-28 00:00:00",
                        "AMOUNT": None,
                    },
                    {
                        "MSG_NO": "3",
                        "MSGDATE": "2023-08-25 15:03:00",
                        "AMOUNT": "1.2500",
                    },
                ],
            )

    def test_normalizes_timestamp_strings_and_preserves_invalid_values(self):
        self.assertEqual(
            _normalize_timestamp_text("11/06/2013 00:00:00", "MODIFIED_ON"),
            "2013-06-11 00:00:00",
        )
        self.assertEqual(
            _normalize_timestamp_text("20/03/2006 12:30:45", "CREATED_ON"),
            "2006-03-20 12:30:45",
        )
        self.assertEqual(
            _normalize_timestamp_text("11/23/2013 00:00:00", "MODIFIED_ON"),
            "2013-11-23 00:00:00",
        )
        self.assertEqual(
            _normalize_timestamp_text("00/00/0 00:00:00", "CREATED_ON"),
            None,
        )

    def test_rejects_source_count_mismatch_and_removes_partial_file(self):
        count_before = Mock()
        count_before.fetchone.return_value = (2,)
        source_cursor = Mock()
        source_cursor.fetchmany.side_effect = [[(1, "date", "1.0")], []]
        count_after = Mock()
        count_after.fetchone.return_value = (2,)
        connection = Mock()
        connection.cursor.side_effect = [count_before, source_cursor, count_after]

        with tempfile.TemporaryDirectory() as temporary_directory:
            with self.assertRaisesRegex(RuntimeError, "count before=2, extracted=1"):
                export_table_to_parquet(
                    connection,
                    _messages_mapping(),
                    temporary_directory,
                    run_id="run-002",
                    fetch_size=1,
                )

            self.assertEqual(list(Path(temporary_directory).rglob("*.parquet")), [])
            self.assertEqual(list(Path(temporary_directory).rglob("*.partial")), [])
            source_cursor.close.assert_called_once()

    def test_removes_partial_file_when_odbc_fetch_fails(self):
        count_before = Mock()
        count_before.fetchone.return_value = (1,)
        source_cursor = Mock()
        source_cursor.fetchmany.side_effect = RuntimeError("ODBC read failed")
        connection = Mock()
        connection.cursor.side_effect = [count_before, source_cursor]

        with tempfile.TemporaryDirectory() as temporary_directory:
            with self.assertRaisesRegex(RuntimeError, "ODBC read failed"):
                export_table_to_parquet(
                    connection,
                    _messages_mapping(),
                    temporary_directory,
                    run_id="run-003",
                    fetch_size=1,
                )

            self.assertEqual(list(Path(temporary_directory).rglob("*.parquet")), [])
            self.assertEqual(list(Path(temporary_directory).rglob("*.partial")), [])
            source_cursor.close.assert_called_once()

    def test_does_not_replace_previously_completed_export_for_same_run(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            final_path = (
                Path(temporary_directory) / "MESSAGES" / "run-004" / "messages.parquet"
            )
            final_path.parent.mkdir(parents=True)
            final_path.write_bytes(b"previous successful export")
            connection = Mock()

            with self.assertRaisesRegex(FileExistsError, "Refusing to overwrite"):
                export_table_to_parquet(
                    connection,
                    _messages_mapping(),
                    temporary_directory,
                    run_id="run-004",
                    fetch_size=1,
                )

            self.assertEqual(final_path.read_bytes(), b"previous successful export")
            connection.cursor.assert_not_called()


if __name__ == "__main__":
    unittest.main()
