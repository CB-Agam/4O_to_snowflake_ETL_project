from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts import run_parquet_etl


def _mapping(target_table: str) -> list[dict[str, str]]:
    return [
        {
            "source_table": target_table.title(),
            "source_column": "ID",
            "target_table": target_table.upper(),
            "target_column": "ID",
            "snowflake_datatype": "NUMBER(10,0)",
            "source_data_type": "INT32",
            "constraint": "PK",
        }
    ]


def _export_result(target_table: str) -> SimpleNamespace:
    return SimpleNamespace(
        run_id="run-123",
        target_table=target_table.upper(),
        row_count=10,
        file_path=Path(f"exports\\{target_table.upper()}\\run-123\\{target_table.lower()}.parquet"),
        file_size_bytes=1024,
        total_seconds=3.5,
    )


class RunParquetEtlTests(unittest.TestCase):
    def test_runs_export_load_transform_with_shared_run_id(self):
        with (
            patch("scripts.run_parquet_etl.configure_logging"),
            patch("scripts.run_parquet_etl.connect_to_snowflake") as connect_sf,
            patch("scripts.run_parquet_etl.close_snowflake_connection") as close_sf,
            patch("scripts.run_parquet_etl.connect_to_4d") as connect_4d,
            patch("scripts.run_parquet_etl.close_4d_connection") as close_4d,
            patch("scripts.run_parquet_etl.SnowflakeAuditLogger") as audit_class,
            patch("scripts.run_parquet_etl._load_selected_mappings") as load_mappings,
            patch("scripts.run_parquet_etl.export_table_to_parquet") as export,
            patch("scripts.run_parquet_etl.load_parquet_to_raw_table") as load_raw,
            patch("scripts.run_parquet_etl.transform_parquet_raw_to_target") as transform,
        ):
            snowflake_connection = Mock()
            source_connection = Mock()
            audit = Mock()
            audit.run_id = "run-123"
            audit.record.return_value = 1
            connect_sf.return_value = snowflake_connection
            connect_4d.return_value = source_connection
            audit_class.return_value = audit
            mapping_rows = _mapping("CUSTOMERS")
            load_mappings.return_value = [
                ("Customers", "Customers", mapping_rows),
            ]
            export.return_value = _export_result("CUSTOMERS")
            load_raw.return_value = SimpleNamespace(
                loaded_rows=10,
                skipped_as_duplicate=False,
            )
            transform.return_value = SimpleNamespace(
                inserted_rows=9,
                rejected_rows=1,
                skipped_as_duplicate=False,
            )

            result = run_parquet_etl.main(
                [
                    "--table",
                    "Customers",
                    "--database",
                    "DW_TEST",
                    "--schema",
                    "RAW",
                ]
            )

        self.assertEqual(result, 0)
        audit_class.assert_called_once_with(
            snowflake_connection,
            "DW_TEST",
            "RAW",
        )
        export.assert_called_once_with(
            source_connection,
            mapping_rows,
            run_parquet_etl.DEFAULT_EXPORT_DIR,
            "run-123",
            fetch_size=50_000,
        )
        load_raw.assert_called_once_with(
            snowflake_connection,
            "DW_TEST",
            "RAW",
            mapping_rows,
            _export_result("CUSTOMERS").file_path,
            "run-123",
        )
        transform.assert_called_once_with(
            snowflake_connection,
            "DW_TEST",
            "RAW",
            mapping_rows,
            "run-123",
            "customers.parquet",
        )
        self.assertEqual(audit.record.call_count, 5)
        close_4d.assert_called_once_with(source_connection)
        close_sf.assert_called_once_with(snowflake_connection)

    def test_continues_to_next_table_after_extraction_failure(self):
        with (
            patch("scripts.run_parquet_etl.configure_logging"),
            patch("scripts.run_parquet_etl.connect_to_snowflake") as connect_sf,
            patch("scripts.run_parquet_etl.close_snowflake_connection"),
            patch("scripts.run_parquet_etl.connect_to_4d") as connect_4d,
            patch("scripts.run_parquet_etl.close_4d_connection"),
            patch("scripts.run_parquet_etl.SnowflakeAuditLogger") as audit_class,
            patch("scripts.run_parquet_etl._load_selected_mappings") as load_mappings,
            patch("scripts.run_parquet_etl.export_table_to_parquet") as export,
            patch("scripts.run_parquet_etl.load_parquet_to_raw_table") as load_raw,
            patch("scripts.run_parquet_etl.transform_parquet_raw_to_target") as transform,
        ):
            snowflake_connection = Mock()
            source_connection = Mock()
            audit = Mock()
            audit.run_id = "run-123"
            audit.record.return_value = 1
            connect_sf.return_value = snowflake_connection
            connect_4d.return_value = source_connection
            audit_class.return_value = audit
            first_mapping = _mapping("FIRST_TABLE")
            second_mapping = _mapping("SECOND_TABLE")
            load_mappings.return_value = [
                ("First Table", "First Table", first_mapping),
                ("Second Table", "Second Table", second_mapping),
            ]
            export.side_effect = [
                RuntimeError("4D extraction failed"),
                _export_result("SECOND_TABLE"),
            ]
            load_raw.return_value = SimpleNamespace(
                loaded_rows=10,
                skipped_as_duplicate=False,
            )
            transform.return_value = SimpleNamespace(
                inserted_rows=10,
                rejected_rows=0,
                skipped_as_duplicate=False,
            )

            result = run_parquet_etl.main(
                [
                    "--tables",
                    "First Table",
                    "Second Table",
                    "--database",
                    "DW_TEST",
                    "--schema",
                    "RAW",
                ]
            )

        self.assertEqual(result, 1)
        self.assertEqual(export.call_count, 2)
        self.assertEqual(load_raw.call_count, 1)
        self.assertEqual(transform.call_count, 1)
        self.assertEqual(transform.call_args.args[3], second_mapping)
        pipeline_finish = audit.finish_step.call_args_list[-1]
        self.assertEqual(pipeline_finish.args[1], "PARTIAL")

    def test_rejects_nonpositive_fetch_size(self):
        with self.assertRaises(SystemExit):
            run_parquet_etl._parse_args(
                ["--table", "Customers", "--fetch-size", "0"]
            )


if __name__ == "__main__":
    unittest.main()
