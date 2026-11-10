from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import load_parquet_to_snowflake as load_cli


class LoadParquetCliTests(unittest.TestCase):
    def _create_parquet(self, path: Path) -> None:
        schema = pa.schema(
            [pa.field("FORM_ID", pa.string())],
            metadata={
                b"run_id": b"run-123",
                b"source_table": b"User_Defined_Form_Builds",
                b"target_table": b"USER_DEFINED_FORM_BUILDS",
            },
        )
        pq.write_table(pa.Table.from_arrays([pa.array(["1"])], schema=schema), path)

    @patch("scripts.load_parquet_to_snowflake.configure_logging")
    @patch("scripts.load_parquet_to_snowflake.close_snowflake_connection")
    @patch("scripts.load_parquet_to_snowflake.connect_to_snowflake")
    @patch("scripts.load_parquet_to_snowflake.load_parquet_to_raw_table")
    @patch("scripts.load_parquet_to_snowflake.load_metadata_from_excel")
    def test_load_command_uses_parquet_run_id_and_mapped_table(
        self,
        load_metadata,
        load_file,
        connect,
        close,
        _configure_logging,
    ):
        workbook = Path(load_cli.DEFAULT_EXCEL_PATH)
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
        load_metadata.return_value = {
            "USER_DEFINED_FORM_BUILDS": mapping_rows,
        }
        snowflake_connection = Mock()
        connect.return_value = snowflake_connection
        load_file.return_value = Mock(
            target_table="USER_DEFINED_FORM_BUILDS",
            raw_table="USER_DEFINED_FORM_BUILDS_RAW",
            loaded_rows=1,
            file_path=Path("forms.parquet"),
            file_sha256="hash",
            skipped_as_duplicate=False,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            parquet_path = Path(temporary_directory) / "forms.parquet"
            self._create_parquet(parquet_path)
            result = load_cli.main(
                [
                    "--table",
                    "User_Defined_Form_Builds",
                    "--file",
                    str(parquet_path),
                    "--sheet",
                    "User_Defined_Form_Builds",
                    "--database",
                    "DW_TEST",
                    "--schema",
                    "RAW",
                ]
            )

        self.assertEqual(result, 0)
        load_metadata.assert_called_once_with(
            str(workbook),
            sheet_name="User_Defined_Form_Builds",
        )
        load_file.assert_called_once_with(
            snowflake_connection,
            "DW_TEST",
            "RAW",
            mapping_rows,
            parquet_path,
            "run-123",
        )
        close.assert_called_once_with(snowflake_connection)


if __name__ == "__main__":
    unittest.main()
