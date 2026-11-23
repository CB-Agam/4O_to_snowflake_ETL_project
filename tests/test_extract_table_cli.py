from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow.parquet as pq

from etl.parallel_parquet_export import export_table_to_parquet_parallel
from etl.csv_export import export_table_to_csv_parallel
from etl.source_extract import build_full_table_query
from scripts import extract_table_to_csv as extract_cli


def _column(table: str, name: str, type_name: str = "VARCHAR") -> SimpleNamespace:
    return SimpleNamespace(table_name=table, column_name=name, type_name=type_name)


def _result(table: str, output_format: str = "parquet") -> Mock:
    return Mock(
        run_id="audit-run",
        target_table=table,
        row_count=2,
        worker_count=1,
        fetch_size=100,
        query_seconds=0.1,
        fetch_seconds=0.2,
        serialization_seconds=0.3,
        parquet_write_seconds=0.4,
        csv_write_seconds=0.4,
        total_seconds=1.0,
        file_path=Path("data.parquet"),
        file_paths=(Path("data.csv"),),
    )


class ExtractTableCliTests(unittest.TestCase):
    def setUp(self):
        def start(patcher):
            mocked = patcher.start()
            self.addCleanup(patcher.stop)
            return mocked

        start(patch("scripts.extract_table_to_csv.configure_logging"))
        start(patch.object(extract_cli.settings, "snowflake_database", "DB"))
        start(patch.object(extract_cli.settings, "snowflake_schema", "RAW"))
        self.snowflake = start(patch("scripts.extract_table_to_csv.connect_to_snowflake"))
        self.close_snowflake = start(patch("scripts.extract_table_to_csv.close_snowflake_connection"))
        self.audit = start(patch("scripts.extract_table_to_csv.SnowflakeAuditLogger")).return_value
        self.audit.run_id = "audit-run"
        self.audit.record.return_value = 1
        self.connection = Mock()
        self.cursor = Mock()
        self.connection.cursor.return_value = self.cursor
        self.cursor.tables.return_value = [SimpleNamespace(table_name="Messages")]
        self.cursor.columns.return_value = [
            _column("Messages", "Msg_No", "INTEGER"),
            _column("Messages", "Status"),
            _column("Messages", "Original Date", "TIMESTAMP"),
            _column("Messages_Other", "Not This Table"),
        ]
        self.connect_4d = start(patch(
            "scripts.extract_table_to_csv.connect_to_4d", return_value=self.connection
        ))
        self.close_4d = start(patch("scripts.extract_table_to_csv.close_4d_connection"))
        self.export_parquet = start(patch(
            "scripts.extract_table_to_csv.export_table_to_parquet_parallel",
            return_value=_result("MESSAGES"),
        ))
        self.export_csv = start(patch(
            "scripts.extract_table_to_csv.export_table_to_csv_parallel",
            return_value=_result("MESSAGES", "csv"),
        ))

    def test_extracts_every_source_column_without_workbook(self):
        with tempfile.TemporaryDirectory() as directory:
            result = extract_cli.main([
                "--table", "MESSAGES", "--export-dir", directory,
            ])
        self.assertEqual(result, 0)
        self.cursor.tables.assert_called_once_with(tableType="TABLE")
        self.cursor.columns.assert_called_once_with(table="Messages")
        mapping = self.export_parquet.call_args.args[1]
        self.assertEqual(
            [row["source_column"] for row in mapping],
            ["Msg_No", "Status", "Original Date"],
        )
        self.assertEqual(
            [row["target_column"] for row in mapping],
            ["Msg_No", "Status", "Original Date"],
        )
        self.assertTrue(all(row["snowflake_datatype"] == "VARCHAR" for row in mapping))
        self.assertEqual(mapping[2]["source_data_type"], "TIMESTAMP")
        self.assertEqual(
            build_full_table_query(mapping),
            "SELECT [Msg_No], [Status], CAST([Original Date] AS VARCHAR) FROM [Messages]",
        )
        self.assertEqual(self.audit.record.call_args.args[:3],
                         ("PARQUET_EXTRACTION", "RUNNING", "MESSAGES"))
        self.assertEqual(self.audit.finish_step.call_args.args[:2], (1, "SUCCESS"))
        self.close_4d.assert_called_once_with(self.connection)
        self.close_snowflake.assert_called_once()

    def test_discovered_columns_are_written_to_parquet(self):
        table_cursor = Mock()
        table_cursor.tables.return_value = [SimpleNamespace(table_name="Messages")]
        columns_cursor = Mock()
        columns_cursor.columns.return_value = [
            _column("Messages", "Msg_No", "INTEGER"),
            _column("Messages", "Status"),
            _column("Messages", "Original Date", "TIMESTAMP"),
        ]
        count_before, source, count_after = Mock(), Mock(), Mock()
        count_before.fetchone.return_value = (1,)
        count_after.fetchone.return_value = (1,)
        source.fetchmany.side_effect = [[(1, "inactive", "bad-date")], []]
        self.connection.cursor.side_effect = [
            table_cursor, columns_cursor, count_before, source, count_after
        ]
        self.export_parquet.side_effect = export_table_to_parquet_parallel
        with tempfile.TemporaryDirectory() as directory:
            result = extract_cli.main([
                "--table", "Messages", "--export-dir", directory, "--workers", "1",
            ])
            parquet_path = Path(directory) / "MESSAGES" / "audit-run" / "messages.parquet"
            table = pq.read_table(parquet_path)
            self.assertEqual(result, 0)
            self.assertEqual(table.column_names, ["Msg_No", "Status", "Original Date"])
            self.assertEqual(table.to_pylist(), [{
                "Msg_No": "1", "Status": "inactive", "Original Date": "bad-date",
            }])
            self.assertEqual(table.schema.metadata[b"target_table"], b"MESSAGES")

    def test_all_discovers_tables_skips_existing_and_audits_each(self):
        self.cursor.tables.return_value = [
            SimpleNamespace(table_name="Messages"),
            SimpleNamespace(table_name="Alerts"),
        ]
        self.cursor.columns.return_value = [_column("Alerts", "Alert ID", "INTEGER")]
        self.export_parquet.return_value = _result("ALERTS")
        with tempfile.TemporaryDirectory() as directory:
            existing = Path(directory) / "MESSAGES" / "old" / "messages.parquet"
            existing.parent.mkdir(parents=True)
            existing.touch()
            result = extract_cli.main(["--table", "all", "--export-dir", directory])

        self.assertEqual(result, 0)
        self.cursor.columns.assert_called_once_with(table="Alerts")
        self.assertEqual(self.export_parquet.call_args.args[1][0]["source_column"], "Alert ID")
        self.assertEqual(
            [call.args[:3] for call in self.audit.record.call_args_list],
            [
                ("PARQUET_EXTRACTION", "SKIPPED", "MESSAGES"),
                ("PARQUET_EXTRACTION", "RUNNING", "ALERTS"),
            ],
        )

    def test_csv_extraction_writes_csv_audit(self):
        result = extract_cli.main(["--table", "Messages", "--format", "csv"])
        self.assertEqual(result, 0)
        self.export_csv.assert_called_once()
        self.export_parquet.assert_not_called()
        self.assertEqual(self.audit.record.call_args.args[:3],
                         ("CSV_EXTRACTION", "RUNNING", "MESSAGES"))
        self.assertIn('"file_write_seconds": 0.4', self.audit.finish_step.call_args.args[2])

    def test_ambiguous_table_names_fail_before_export(self):
        self.cursor.tables.return_value = [
            SimpleNamespace(table_name="Account Contacts"),
            SimpleNamespace(table_name="Account_Contacts"),
        ]
        self.assertEqual(extract_cli.main(["--table", "all"]), 1)
        self.export_parquet.assert_not_called()

    def test_missing_columns_fails_and_is_audited(self):
        self.cursor.columns.return_value = []
        self.assertEqual(extract_cli.main(["--table", "Messages"]), 1)
        self.assertEqual(self.audit.record.call_args.args[:3],
                         ("PARQUET_EXTRACTION", "FAILED", "Messages"))
        self.export_parquet.assert_not_called()

    def test_extraction_failure_is_recorded(self):
        self.export_parquet.side_effect = RuntimeError("source read failed")
        self.assertEqual(extract_cli.main(["--table", "Messages"]), 1)
        finish = self.audit.finish_step.call_args
        self.assertEqual(finish.args[:2], (1, "FAILED"))
        self.assertIn("source read failed", finish.kwargs["error_message"])

    def test_missing_count_skips_table_and_continues_without_scanning(self):
        for output_format in ("parquet", "csv"):
            with self.subTest(output_format=output_format):
                self.audit.reset_mock()
                table_cursor, columns_cursor = Mock(), Mock()
                table_cursor.tables.return_value = [
                    SimpleNamespace(table_name="Messages"),
                    SimpleNamespace(table_name="Alerts"),
                ]
                columns_cursor.columns.side_effect = [
                    [_column("Messages", "Msg_No", "INTEGER")],
                    [_column("Alerts", "Alert ID", "INTEGER")],
                ]
                missing_count, valid_count, source, after_count = (
                    Mock(), Mock(), Mock(), Mock()
                )
                missing_count.fetchone.return_value = None
                valid_count.fetchone.return_value = (1,)
                source.fetchmany.side_effect = [[(7,)], []]
                after_count.fetchone.return_value = (1,)
                self.connection.cursor.side_effect = [
                    table_cursor, columns_cursor, columns_cursor,
                    missing_count, valid_count, source, after_count,
                ]
                if output_format == "parquet":
                    self.export_parquet.side_effect = export_table_to_parquet_parallel
                else:
                    self.export_csv.side_effect = export_table_to_csv_parallel
                with tempfile.TemporaryDirectory() as directory:
                    result = extract_cli.main([
                        "--table", "all", "--format", output_format,
                        "--export-dir", directory, "--workers", "1",
                    ])
                    self.assertEqual(result, 0)
                    self.assertEqual(
                        list((Path(directory) / "MESSAGES").rglob(f"*.{output_format}")), []
                    )
                    self.assertEqual(
                        len(list((Path(directory) / "ALERTS").rglob(f"*.{output_format}"))), 1
                    )
                self.assertEqual(missing_count.execute.call_count, 1)
                missing_count.fetchmany.assert_not_called()
                skipped = self.audit.finish_step.call_args_list[0]
                self.assertEqual(skipped.args[:2], (1, "SKIPPED"))
                self.assertEqual(json.loads(skipped.args[2])["reason"],
                                 "SOURCE_ROW_COUNT_UNAVAILABLE")
                self.assertIsNone(json.loads(skipped.args[2])["row_count"])
                self.assertEqual(self.audit.finish_step.call_args_list[1].args[:2], (1, "SUCCESS"))

    def test_missing_final_count_skips_and_removes_partial_output(self):
        table_cursor, columns_cursor = Mock(), Mock()
        table_cursor.tables.return_value = [SimpleNamespace(table_name="Messages")]
        columns_cursor.columns.return_value = [_column("Messages", "Msg_No", "INTEGER")]
        before, source, after = Mock(), Mock(), Mock()
        before.fetchone.return_value = (1,)
        source.fetchmany.side_effect = [[(1,)], []]
        after.fetchone.return_value = (None,)
        self.connection.cursor.side_effect = [table_cursor, columns_cursor, before, source, after]
        self.export_parquet.side_effect = export_table_to_parquet_parallel
        with tempfile.TemporaryDirectory() as directory:
            result = extract_cli.main([
                "--table", "Messages", "--export-dir", directory, "--workers", "1",
            ])
            self.assertEqual(result, 0)
            self.assertEqual(list(Path(directory).rglob("*.parquet")), [])
            self.assertEqual(list(Path(directory).rglob("*.partial")), [])
        self.assertEqual(self.audit.finish_step.call_args.args[:2], (1, "SKIPPED"))

    def test_snowflake_connection_failure_prevents_extraction(self):
        self.snowflake.side_effect = RuntimeError("Snowflake unavailable")
        self.assertEqual(extract_cli.main(["--table", "Messages"]), 1)
        self.connect_4d.assert_not_called()

    def test_failed_audit_finish_reports_failure(self):
        self.audit.finish_step.side_effect = RuntimeError("audit unavailable")
        self.assertEqual(extract_cli.main(["--table", "Messages"]), 1)
        self.close_snowflake.assert_called_once()


if __name__ == "__main__":
    unittest.main()
