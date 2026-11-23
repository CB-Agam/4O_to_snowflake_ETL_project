from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from scripts import extract_table_to_csv as extract_cli


class ExtractTableCliTests(unittest.TestCase):
    def setUp(self):
        def start(patcher):
            mocked = patcher.start()
            self.addCleanup(patcher.stop)
            return mocked

        start(patch.object(extract_cli.settings, "snowflake_database", "DB"))
        start(patch.object(extract_cli.settings, "snowflake_schema", "RAW"))
        self.snowflake = start(patch("scripts.extract_table_to_csv.connect_to_snowflake"))
        self.close_snowflake = start(patch("scripts.extract_table_to_csv.close_snowflake_connection"))
        self.audit = start(patch("scripts.extract_table_to_csv.SnowflakeAuditLogger"))
        self.audit.return_value.run_id = "audit-run"
        self.audit.return_value.record.return_value = 1

    def test_defaults_to_parquet_and_dispatches_to_parquet_exporter(self):
        mapping_rows = [
            {
                "source_table": "Messages",
                "source_column": "Msg_No",
                "target_table": "MESSAGES",
                "target_column": "MSG_NO",
            }
        ]
        result = Mock(
            run_id="run-1",
            target_table="MESSAGES",
            row_count=2,
            worker_count=2,
            fetch_size=1,
            query_seconds=0.1,
            fetch_seconds=0.2,
            serialization_seconds=0.3,
            parquet_write_seconds=0.4,
            total_seconds=1.0,
            file_path=Path("exports/MESSAGES/run-1/messages.parquet"),
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            workbook_path = Path(temporary_directory) / "mapping.xlsx"
            workbook_path.touch()
            connection = Mock()
            with (
                patch("scripts.extract_table_to_csv.configure_logging"),
                patch(
                    "scripts.extract_table_to_csv.connect_to_4d",
                    return_value=connection,
                ),
                patch("scripts.extract_table_to_csv.close_4d_connection"),
                patch(
                    "scripts.extract_table_to_csv.load_metadata_from_excel",
                    return_value={"MESSAGES": mapping_rows},
                ),
                patch(
                    "scripts.extract_table_to_csv.export_table_to_parquet_parallel",
                    return_value=result,
                ) as export_parquet,
                patch(
                    "scripts.extract_table_to_csv.export_table_to_csv_parallel"
                ) as export_csv,
            ):
                exit_code = extract_cli.main(
                    [
                        "--table",
                        "Messages",
                        "--excel",
                        str(workbook_path),
                        "--sheet",
                        "Messages",
                        "--export-dir",
                        temporary_directory,
                        "--run-id",
                        "run-1",
                    ]
                )

        self.assertEqual(exit_code, 0)
        export_parquet.assert_called_once()
        self.assertIs(export_parquet.call_args.args[0], connection)
        export_csv.assert_not_called()
        self.audit.return_value.ensure_table.assert_called_once()
        self.audit.return_value.record.assert_called_once()
        self.assertEqual(
            self.audit.return_value.record.call_args.args[:3],
            ("PARQUET_EXTRACTION", "RUNNING", "MESSAGES"),
        )
        finished = self.audit.return_value.finish_step.call_args
        self.assertEqual(finished.args[:2], (1, "SUCCESS"))
        self.assertIn('"rows": 2', finished.args[2])
        self.close_snowflake.assert_called_once()

    def test_csv_can_be_requested_explicitly(self):
        args = extract_cli._parse_args(
            ["--table", "Messages", "--format", "csv"]
        )
        self.assertEqual(args.output_format, "csv")

    def test_all_skips_tables_with_existing_parquet_and_exports_remaining(self):
        messages_mapping = [
            {
                "source_table": "Messages",
                "source_column": "Msg_No",
                "target_table": "MESSAGES",
                "target_column": "MSG_NO",
            }
        ]
        alerts_mapping = [
            {
                "source_table": "Alerts",
                "source_column": "Alert_No",
                "target_table": "ALERTS",
                "target_column": "ALERT_NO",
            }
        ]
        result = Mock(
            run_id="audit-run",
            target_table="ALERTS",
            row_count=2,
            worker_count=2,
            fetch_size=1,
            query_seconds=0.1,
            fetch_seconds=0.2,
            serialization_seconds=0.3,
            parquet_write_seconds=0.4,
            total_seconds=1.0,
            file_path=Path("exports/ALERTS/run-1/alerts.parquet"),
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            workbook_path = root / "mapping.xlsx"
            workbook_path.touch()
            export_dir = root / "exports"
            existing_file = export_dir / "MESSAGES" / "old-run" / "messages.parquet"
            existing_file.parent.mkdir(parents=True)
            existing_file.touch()
            connection = Mock()
            with (
                patch("scripts.extract_table_to_csv.configure_logging"),
                patch(
                    "scripts.extract_table_to_csv._load_all_table_names",
                    return_value=["Messages", "Unmapped", "Alerts"],
                ),
                patch(
                    "scripts.extract_table_to_csv.pd.ExcelFile",
                    return_value=MagicMock(
                        __enter__=Mock(
                            return_value=Mock(sheet_names=["Messages", "Alerts"])
                        ),
                        __exit__=Mock(return_value=False),
                    ),
                ),
                patch(
                    "scripts.extract_table_to_csv.load_metadata_from_excel",
                    side_effect=[
                        {"MESSAGES": messages_mapping},
                        {"ALERTS": alerts_mapping},
                    ],
                ),
                patch(
                    "scripts.extract_table_to_csv.connect_to_4d",
                    return_value=connection,
                ),
                patch("scripts.extract_table_to_csv.close_4d_connection"),
                patch(
                    "scripts.extract_table_to_csv.export_table_to_parquet_parallel",
                    return_value=result,
                ) as export_parquet,
            ):
                exit_code = extract_cli.main(
                    [
                        "--table",
                        "all",
                        "--excel",
                        str(workbook_path),
                        "--export-dir",
                        str(export_dir),
                        "--format",
                        "parquet",
                    ]
                )

        self.assertEqual(exit_code, 0)
        export_parquet.assert_called_once()
        self.assertEqual(export_parquet.call_args.args[1], alerts_mapping)
        self.assertEqual(
            [call.args[:3] for call in self.audit.return_value.record.call_args_list],
            [
                ("PARQUET_EXTRACTION", "SKIPPED", "MESSAGES"),
                ("PARQUET_EXTRACTION", "SKIPPED", "Unmapped"),
                ("PARQUET_EXTRACTION", "RUNNING", "ALERTS"),
            ],
        )

    def test_snowflake_connection_failure_prevents_extraction(self):
        self.snowflake.side_effect = RuntimeError("Snowflake unavailable")
        with (
            patch("scripts.extract_table_to_csv.configure_logging"),
            patch("scripts.extract_table_to_csv.connect_to_4d") as source,
        ):
            exit_code = extract_cli.main(["--table", "Messages"])
        self.assertEqual(exit_code, 1)
        source.assert_not_called()

    def test_csv_extraction_writes_csv_audit(self):
        mapping_rows = [{
            "source_table": "Messages", "source_column": "Msg_No",
            "target_table": "MESSAGES", "target_column": "MSG_NO",
        }]
        result = Mock(
            run_id="audit-run", target_table="MESSAGES", row_count=1,
            worker_count=1, fetch_size=100, query_seconds=0.1, fetch_seconds=0.2,
            serialization_seconds=0.3, csv_write_seconds=0.4,
            total_seconds=1.0, file_paths=(Path("messages.csv"),),
        )
        with tempfile.TemporaryDirectory() as directory:
            workbook_path = Path(directory) / "mapping.xlsx"
            workbook_path.touch()
            with (
                patch("scripts.extract_table_to_csv.configure_logging"),
                patch("scripts.extract_table_to_csv.connect_to_4d"),
                patch("scripts.extract_table_to_csv.close_4d_connection"),
                patch("scripts.extract_table_to_csv.load_metadata_from_excel",
                      return_value={"MESSAGES": mapping_rows}),
                patch("scripts.extract_table_to_csv.export_table_to_csv_parallel",
                      return_value=result),
            ):
                exit_code = extract_cli.main([
                    "--table", "Messages", "--sheet", "Messages",
                    "--excel", str(workbook_path), "--format", "csv",
                ])
        self.assertEqual(exit_code, 0)
        self.assertEqual(
            self.audit.return_value.record.call_args.args[:3],
            ("CSV_EXTRACTION", "RUNNING", "MESSAGES"),
        )
        self.assertIn('"file_write_seconds": 0.4',
                      self.audit.return_value.finish_step.call_args.args[2])

    def test_extraction_failure_is_recorded(self):
        mapping_rows = [{
            "source_table": "Messages", "source_column": "Msg_No",
            "target_table": "MESSAGES", "target_column": "MSG_NO",
        }]
        with tempfile.TemporaryDirectory() as directory:
            workbook_path = Path(directory) / "mapping.xlsx"
            workbook_path.touch()
            with (
                patch("scripts.extract_table_to_csv.configure_logging"),
                patch("scripts.extract_table_to_csv.connect_to_4d"),
                patch("scripts.extract_table_to_csv.close_4d_connection"),
                patch("scripts.extract_table_to_csv.load_metadata_from_excel",
                      return_value={"MESSAGES": mapping_rows}),
                patch("scripts.extract_table_to_csv.export_table_to_parquet_parallel",
                      side_effect=RuntimeError("source read failed")),
            ):
                exit_code = extract_cli.main([
                    "--table", "Messages", "--sheet", "Messages",
                    "--excel", str(workbook_path),
                ])
        self.assertEqual(exit_code, 1)
        finish = self.audit.return_value.finish_step.call_args
        self.assertEqual(finish.args[:2], (1, "FAILED"))
        self.assertIn("source read failed", finish.kwargs["error_message"])

    def test_failed_audit_finish_reports_failure(self):
        mapping_rows = [{
            "source_table": "Messages", "source_column": "Msg_No",
            "target_table": "MESSAGES", "target_column": "MSG_NO",
        }]
        result = Mock(
            run_id="audit-run", target_table="MESSAGES", row_count=1,
            worker_count=1, fetch_size=100, query_seconds=0.1, fetch_seconds=0.2,
            serialization_seconds=0.3, parquet_write_seconds=0.4,
            total_seconds=1.0, file_path=Path("messages.parquet"),
        )
        self.audit.return_value.finish_step.side_effect = RuntimeError("audit unavailable")
        with tempfile.TemporaryDirectory() as directory:
            workbook_path = Path(directory) / "mapping.xlsx"
            workbook_path.touch()
            with (
                patch("scripts.extract_table_to_csv.configure_logging"),
                patch("scripts.extract_table_to_csv.connect_to_4d"),
                patch("scripts.extract_table_to_csv.close_4d_connection"),
                patch("scripts.extract_table_to_csv.load_metadata_from_excel",
                      return_value={"MESSAGES": mapping_rows}),
                patch("scripts.extract_table_to_csv.export_table_to_parquet_parallel",
                      return_value=result),
            ):
                exit_code = extract_cli.main([
                    "--table", "Messages", "--sheet", "Messages",
                    "--excel", str(workbook_path),
                ])
        self.assertEqual(exit_code, 1)
        self.close_snowflake.assert_called_once()


if __name__ == "__main__":
    unittest.main()
