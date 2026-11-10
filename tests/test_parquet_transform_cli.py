from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import transform_parquet_raw as transform_cli


class TransformParquetCliTests(unittest.TestCase):
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

    @patch("scripts.transform_parquet_raw.configure_logging")
    @patch("scripts.transform_parquet_raw.close_snowflake_connection")
    @patch("scripts.transform_parquet_raw.connect_to_snowflake")
    @patch("scripts.transform_parquet_raw.transform_parquet_raw_to_target")
    @patch("scripts.transform_parquet_raw.load_metadata_from_excel")
    def test_transform_command_uses_parquet_run_id_and_mapped_table(
        self,
        load_metadata,
        transform,
        connect,
        close,
        _configure_logging,
    ):
        workbook = Path(transform_cli.DEFAULT_EXCEL_PATH)
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
        snowflake_connection = Mock()
        connect.return_value = snowflake_connection
        transform.return_value = Mock(
            target_table="USER_DEFINED_FORM_BUILDS",
            raw_table="USER_DEFINED_FORM_BUILDS_RAW",
            file_name="forms.parquet",
            source_rows=1,
            inserted_rows=1,
            rejected_rows=0,
            skipped_as_duplicate=False,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            parquet_path = Path(temporary_directory) / "forms.parquet"
            self._create_parquet(parquet_path)
            result = transform_cli.main(
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
        transform.assert_called_once_with(
            snowflake_connection,
            "DW_TEST",
            "RAW",
            mapping_rows,
            "run-123",
            "forms.parquet",
        )
        close.assert_called_once_with(snowflake_connection)


if __name__ == "__main__":
    unittest.main()
