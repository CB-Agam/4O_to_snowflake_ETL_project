from __future__ import annotations

import unittest
from unittest.mock import Mock

from etl.source_extract import (
    build_full_table_query,
    count_source_rows,
    stream_source_batches,
)


class SourceExtractionTests(unittest.TestCase):
    def test_build_full_table_query_casts_mapped_temporal_columns_to_text(self):
        query = build_full_table_query(
            [
                {
                    "source_table": "Messages",
                    "source_column": "Msg_No",
                    "source_data_type": "INT32",
                    "target_column": "MSG_NO",
                },
                {
                    "source_table": "Messages",
                    "source_column": "MsgDate",
                    "source_data_type": "TIMESTAMP",
                    "target_column": "MSGDATE",
                },
                {
                    "source_table": "Messages",
                    "source_column": "MsgTime",
                    "source_data_type": "INTERVAL",
                    "target_column": "MSGTIME",
                },
            ]
        )

        self.assertEqual(
            query,
            "SELECT [Msg_No], CAST([MsgDate] AS VARCHAR), "
            "CAST([MsgTime] AS VARCHAR) FROM [Messages]",
        )

    def test_build_full_table_query_rejects_multiple_source_tables(self):
        mappings = [
            {
                "source_table": "First",
                "source_column": "ID",
                "source_data_type": "INT32",
                "target_column": "ID",
            },
            {
                "source_table": "Second",
                "source_column": "NAME",
                "source_data_type": "CLOB",
                "target_column": "NAME",
            },
        ]

        with self.assertRaisesRegex(ValueError, "one 4D source table"):
            build_full_table_query(mappings)

    def test_stream_source_batches_consumes_every_row_and_closes_cursor(self):
        cursor = Mock()
        cursor.fetchmany.side_effect = [
            [(1, "A"), (2, "B")],
            [(3, "C")],
            [],
        ]
        connection = Mock()
        connection.cursor.return_value = cursor
        consumed = []

        stats = stream_source_batches(
            connection,
            "SELECT ID, NAME FROM SOURCE",
            fetch_size=2,
            consume_batch=consumed.extend,
        )

        self.assertEqual(consumed, [(1, "A"), (2, "B"), (3, "C")])
        self.assertEqual(stats.rows_extracted, 3)
        self.assertEqual(cursor.fetchmany.call_args_list[0].args, (2,))
        self.assertEqual(cursor.fetchmany.call_count, 3)
        cursor.close.assert_called_once()

    def test_stream_source_batches_closes_cursor_if_consumer_fails(self):
        cursor = Mock()
        cursor.fetchmany.return_value = [(1,)]
        connection = Mock()
        connection.cursor.return_value = cursor
        consumer_error = RuntimeError("consumer failed")

        with self.assertRaisesRegex(RuntimeError, "consumer failed"):
            stream_source_batches(
                connection,
                "SELECT ID FROM SOURCE",
                fetch_size=10,
                consume_batch=Mock(side_effect=consumer_error),
            )

        cursor.close.assert_called_once()

    def test_stream_source_batches_rejects_invalid_fetch_size_before_connecting(self):
        connection = Mock()

        with self.assertRaisesRegex(ValueError, "greater than zero"):
            stream_source_batches(connection, "SELECT ID FROM SOURCE", 0, Mock())

        connection.cursor.assert_not_called()

    def test_count_source_rows_uses_single_row_fetch_and_closes_cursor(self):
        cursor = Mock()
        cursor.fetchone.return_value = (265568,)
        connection = Mock()
        connection.cursor.return_value = cursor

        count = count_source_rows(connection, "Claims Trans")

        self.assertEqual(count, 265568)
        cursor.execute.assert_called_once_with("SELECT COUNT(*) FROM [Claims Trans]")
        cursor.fetchone.assert_called_once()
        cursor.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
