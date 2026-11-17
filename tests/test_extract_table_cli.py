from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from scripts import extract_table_to_csv as extract_cli


class ExtractTableCliTests(unittest.TestCase):
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

    def test_csv_can_be_requested_explicitly(self):
        args = extract_cli._parse_args(
            ["--table", "Messages", "--format", "csv"]
        )
        self.assertEqual(args.output_format, "csv")


if __name__ == "__main__":
    unittest.main()
