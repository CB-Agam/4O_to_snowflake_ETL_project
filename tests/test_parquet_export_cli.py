from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from scripts import export_table_to_parquet as export_cli


class ParquetExportCliTests(unittest.TestCase):
    @patch("scripts.export_table_to_parquet.configure_logging")
    @patch("scripts.export_table_to_parquet.close_4d_connection")
    @patch("scripts.export_table_to_parquet.connect_to_4d")
    @patch("scripts.export_table_to_parquet.export_table_to_parquet")
    @patch("scripts.export_table_to_parquet.load_metadata_from_excel")
    def test_export_command_uses_mapping_and_only_the_4d_exporter(
        self,
        load_metadata,
        export_table,
        connect_to_4d,
        close_connection,
        _configure_logging,
    ):
        workbook = Path(export_cli.DEFAULT_EXCEL_PATH)
        if not workbook.is_file():
            self.skipTest("Project mapping workbook is not available")

        mapping_rows = [
            {
                "source_table": "User_Defined_Form_Builds",
                "source_column": "Form ID",
                "target_table": "USER_DEFINED_FORM_BUILDS",
                "target_column": "FORM_ID",
            }
        ]
        load_metadata.return_value = {"USER_DEFINED_FORM_BUILDS": mapping_rows}
        source_connection = Mock()
        connect_to_4d.return_value = source_connection
        export_table.return_value = Mock(
            target_table="USER_DEFINED_FORM_BUILDS",
            row_count=2,
            file_path=Path("exports/USER_DEFINED_FORM_BUILDS/run-1/messages.parquet"),
            file_size_bytes=100,
            query_seconds=0.1,
            fetch_seconds=0.2,
            serialization_seconds=0.3,
            parquet_write_seconds=0.4,
            total_seconds=1.0,
        )

        result = export_cli.main(
            [
                "--table",
                "User_Defined_Form_Builds",
                "--sheet",
                "User_Defined_Form_Builds",
                "--export-dir",
                "exports",
                "--run-id",
                "run-1",
                "--fetch-size",
                "10",
            ]
        )

        self.assertEqual(result, 0)
        load_metadata.assert_called_once_with(
            str(workbook),
            sheet_name="User_Defined_Form_Builds",
        )
        export_table.assert_called_once()
        self.assertIs(export_table.call_args.args[0], source_connection)
        close_connection.assert_called_once_with(source_connection)

    @patch("scripts.export_table_to_parquet.configure_logging")
    def test_fetch_size_must_be_positive(self, _configure_logging):
        with self.assertRaises(SystemExit):
            export_cli._parse_args(["--table", "Messages", "--fetch-size", "0"])


if __name__ == "__main__":
    unittest.main()
