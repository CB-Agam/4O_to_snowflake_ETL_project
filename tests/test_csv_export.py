from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from etl.csv_export import (
    CSV_NULL_MARKER,
    _primary_key_source_column,
    export_table_to_csv,
    export_table_to_csv_parallel,
)


MAPPING = [
    {
        "source_table": "Messages",
        "source_column": "Msg_No",
        "source_data_type": "INT32",
        "target_table": "MESSAGES",
        "target_column": "MSG_NO",
        "snowflake_datatype": "NUMBER(10,0)",
    },
    {
        "source_table": "Messages",
        "source_column": "Message",
        "source_data_type": "CLOB",
        "target_table": "MESSAGES",
        "target_column": "MESSAGE",
        "snowflake_datatype": "VARCHAR",
    },
    {
        "source_table": "Messages",
        "source_column": "Created On",
        "source_data_type": "TIMESTAMP",
        "target_table": "MESSAGES",
        "target_column": "CREATED_ON",
        "snowflake_datatype": "TIMESTAMP_NTZ",
    },
]


class CsvExportTests(unittest.TestCase):
    def test_streams_and_validates_csv_with_special_characters_and_nulls(self):
        count_before = Mock()
        count_before.fetchone.return_value = (2,)
        source_cursor = Mock()
        source_cursor.fetchmany.side_effect = [
            [
                (1, 'comma, quote " and\nnewline', "21/03/2006 00:00:00"),
                (2, None, None),
            ],
            [],
        ]
        count_after = Mock()
        count_after.fetchone.return_value = (2,)
        connection = Mock()
        connection.cursor.side_effect = [count_before, source_cursor, count_after]

        with tempfile.TemporaryDirectory() as temporary_directory:
            result = export_table_to_csv(
                connection,
                MAPPING,
                temporary_directory,
                run_id="run-csv-001",
                fetch_size=1,
            )

            self.assertEqual(result.row_count, 2)
            self.assertEqual(result.source_count_before, result.source_count_after)
            self.assertTrue(result.file_path.is_file())
            with result.file_path.open(
                "r", encoding="utf-8-sig", newline=""
            ) as source:
                rows = list(csv.reader(source, strict=True))
            self.assertEqual(
                rows,
                [
                    ["MSG_NO", "MESSAGE", "CREATED_ON"],
                    ["1", 'comma, quote " and\nnewline', "21/03/2006 00:00:00"],
                    ["2", CSV_NULL_MARKER, CSV_NULL_MARKER],
                ],
            )
            self.assertEqual(source_cursor.fetchmany.call_count, 2)
            source_cursor.close.assert_called_once()
            self.assertEqual(
                list(Path(temporary_directory).rglob("*.partial")),
                [],
            )

    def test_removes_partial_csv_when_reserved_null_marker_occurs(self):
        count_before = Mock()
        count_before.fetchone.return_value = (1,)
        source_cursor = Mock()
        source_cursor.fetchmany.side_effect = [
            [(1, CSV_NULL_MARKER, "2006-03-21 00:00:00")]
        ]
        connection = Mock()
        connection.cursor.side_effect = [count_before, source_cursor]

        with tempfile.TemporaryDirectory() as temporary_directory:
            with self.assertRaisesRegex(ValueError, "reserved CSV NULL marker"):
                export_table_to_csv(
                    connection,
                    MAPPING,
                    temporary_directory,
                    run_id="run-csv-002",
                    fetch_size=1,
                )
            self.assertEqual(
                list(Path(temporary_directory).rglob("*.partial")),
                [],
            )
            self.assertEqual(
                list(Path(temporary_directory).rglob("*.csv")),
                [],
            )

    def test_rejects_nonpositive_fetch_size(self):
        connection = Mock()
        with self.assertRaisesRegex(ValueError, "greater than zero"):
            export_table_to_csv(connection, MAPPING, "exports", "run", fetch_size=0)
        connection.cursor.assert_not_called()

    def test_primary_key_detection_requires_a_leading_explicit_marker(self):
        mapping = [dict(row) for row in MAPPING]
        mapping[0]["constraint"] = (
            "based on these columns, unable to find the primary key column"
        )
        self.assertIsNone(_primary_key_source_column(mapping))

        mapping[0]["constraint"] = "PK"
        self.assertEqual(_primary_key_source_column(mapping), "Msg_No")

    def test_composite_primary_key_can_use_an_integer_key_component_for_ranges(self):
        mapping = [dict(row) for row in MAPPING]
        mapping[0]["constraint"] = "PK (composite, 1 of 2)"
        mapping[1]["constraint"] = "PK (composite, 2 of 2)"
        self.assertEqual(_primary_key_source_column(mapping), "Msg_No")

    def test_parallel_export_uses_separate_connections_and_streams_each_range(self):
        mapping = [dict(row) for row in MAPPING]
        mapping[0]["constraint"] = "PK"

        bounds_cursor = Mock()
        bounds_cursor.fetchone.side_effect = [(4,), (1,), (4,), (2,), (2,)]
        final_count_cursor = Mock()
        final_count_cursor.fetchone.return_value = (4,)
        source_connection = Mock()
        source_connection.cursor.side_effect = [bounds_cursor, final_count_cursor]

        first_cursor = Mock()
        first_cursor.fetchmany.side_effect = [
            [
                (1, "first", "2006-03-21 00:00:00"),
                (2, "second", "2006-03-21 00:00:00"),
            ],
            [],
        ]
        second_cursor = Mock()
        second_cursor.fetchmany.side_effect = [
            [
                (3, "third", "2006-03-21 00:00:00"),
                (4, "fourth", "2006-03-21 00:00:00"),
            ],
            [],
        ]
        first_connection = Mock()
        first_connection.cursor.return_value = first_cursor
        second_connection = Mock()
        second_connection.cursor.return_value = second_cursor

        with tempfile.TemporaryDirectory() as temporary_directory:
            result = export_table_to_csv_parallel(
                source_connection,
                mapping,
                temporary_directory,
                run_id="parallel-run",
                connection_factory=Mock(
                    side_effect=[first_connection, second_connection]
                ),
                fetch_size=10_000,
                worker_count=2,
            )

            self.assertEqual(result.row_count, 4)
            self.assertEqual(result.worker_count, 2)
            self.assertEqual(result.fetch_size, 10_000)
            self.assertEqual(len(result.file_paths), 1)
            self.assertEqual(result.file_paths[0].name, "messages.csv")
            self.assertTrue(result.file_paths[0].is_file())
            self.assertEqual(source_connection.cursor.call_count, 2)
            self.assertEqual(
                [call.args[0] for call in bounds_cursor.execute.call_args_list],
                [
                    "SELECT COUNT(*) FROM [Messages]",
                    "SELECT MIN([Msg_No]) FROM [Messages]",
                    "SELECT MAX([Msg_No]) FROM [Messages]",
                    "SELECT COUNT(*) FROM [Messages] WHERE [Msg_No] >= 1 AND [Msg_No] < 3",
                    "SELECT COUNT(*) FROM [Messages] WHERE [Msg_No] >= 3",
                ],
            )
            self.assertEqual(first_cursor.fetchmany.call_args_list[0].args, (10_000,))
            self.assertEqual(second_cursor.fetchmany.call_args_list[0].args, (10_000,))
            first_connection.close.assert_called_once()
            second_connection.close.assert_called_once()

            with result.file_paths[0].open(
                "r", encoding="utf-8-sig", newline=""
            ) as source:
                exported = list(csv.reader(source, strict=True))[1:]
            self.assertCountEqual(
                [row[0] for row in exported],
                ["1", "2", "3", "4"],
            )
            self.assertEqual(
                list(Path(temporary_directory).rglob("*.partial")),
                [],
            )

    def test_missing_primary_key_uses_10000_row_tasks_and_one_final_csv(self):
        count_before = Mock()
        count_before.fetchone.return_value = (5,)
        source_cursor = Mock()
        source_cursor.fetchmany.side_effect = [
            [
                (1, "first", "2006-03-21 00:00:00"),
                (2, "second", "2006-03-21 00:00:00"),
            ],
            [
                (3, "third", "2006-03-21 00:00:00"),
                (4, "fourth", "2006-03-21 00:00:00"),
            ],
            [(5, "fifth", "2006-03-21 00:00:00")],
            [],
        ]
        count_after = Mock()
        count_after.fetchone.return_value = (5,)
        source_connection = Mock()
        source_connection.cursor.side_effect = [
            count_before,
            source_cursor,
            count_after,
        ]
        connection_factory = Mock()

        with tempfile.TemporaryDirectory() as temporary_directory:
            result = export_table_to_csv_parallel(
                source_connection,
                MAPPING,
                temporary_directory,
                run_id="no-primary-key-run",
                connection_factory=connection_factory,
                fetch_size=2,
                worker_count=3,
            )

            self.assertEqual(result.row_count, 5)
            self.assertEqual(result.worker_count, 3)
            self.assertEqual(result.fetch_size, 2)
            self.assertEqual(len(result.file_paths), 1)
            self.assertEqual(result.file_paths[0].name, "messages.csv")
            self.assertTrue(result.file_paths[0].is_file())
            connection_factory.assert_not_called()
            self.assertEqual(
                source_cursor.fetchmany.call_args_list[0].args,
                (2,),
            )
            with result.file_paths[0].open(
                "r", encoding="utf-8-sig", newline=""
            ) as source:
                final_rows = list(csv.reader(source, strict=True))
            self.assertEqual(
                [row[0] for row in final_rows[1:]],
                ["1", "2", "3", "4", "5"],
            )
            self.assertEqual(
                list(Path(temporary_directory).rglob("*.partial")),
                [],
            )

    def test_parallel_export_rejects_shared_worker_connection_and_cleans_files(self):
        mapping = [dict(row) for row in MAPPING]
        mapping[0]["constraint"] = "PK"

        bounds_cursor = Mock()
        bounds_cursor.fetchone.side_effect = [(2,), (1,), (2,), (1,), (1,)]
        source_connection = Mock()
        source_connection.cursor.return_value = bounds_cursor

        with tempfile.TemporaryDirectory() as temporary_directory:
            with self.assertRaisesRegex(
                RuntimeError, "Each CSV worker must use a distinct"
            ):
                export_table_to_csv_parallel(
                    source_connection,
                    mapping,
                    temporary_directory,
                    run_id="shared-connection-run",
                    connection_factory=lambda: source_connection,
                    fetch_size=10_000,
                    worker_count=2,
                )
            self.assertEqual(
                list(Path(temporary_directory).rglob("*.partial")),
                [],
            )
            self.assertEqual(
                list(Path(temporary_directory).rglob("*.csv")),
                [],
            )


if __name__ == "__main__":
    unittest.main()
