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
        query_seconds=0.5,
        fetch_seconds=1.0,
        serialization_seconds=0.5,
        parquet_write_seconds=1.0,
        csv_write_seconds=1.0,
    )


class RunParquetEtlTests(unittest.TestCase):
    def test_csv_format_uses_csv_export_and_existing_load_transform_path(self):
        source_connection = Mock()
        snowflake_connection = Mock()
        audit = Mock()
        audit.run_id = "run-csv-123"
        audit.record.return_value = 1
        mapping_rows = _mapping("CUSTOMERS")
        extraction = SimpleNamespace(
            run_id="run-csv-123",
            row_count=2,
            file_path=Path("exports\\CUSTOMERS\\run-csv-123\\customers.csv"),
            file_size_bytes=128,
            total_seconds=2.5,
            query_seconds=0.5,
            fetch_seconds=1.0,
            serialization_seconds=0.5,
            csv_write_seconds=0.5,
        )
        raw_load = SimpleNamespace(
            loaded_rows=2,
            skipped_as_duplicate=False,
            raw_table='"DW_TEST"."RAW"."ETL_CSV_RAW_TEST"',
        )
        transformed = SimpleNamespace(
            inserted_rows=2,
            rejected_rows=0,
            skipped_as_duplicate=False,
        )

        with (
            patch(
                "scripts.run_parquet_etl.export_table_to_csv",
                return_value=extraction,
            ) as export_csv,
            patch("scripts.run_parquet_etl.export_table_to_parquet") as export_parquet,
            patch(
                "scripts.run_parquet_etl.load_parquet_to_raw_table",
                return_value=raw_load,
            ) as load_raw,
            patch(
                "scripts.run_parquet_etl.transform_parquet_raw_to_target",
                return_value=transformed,
            ) as transform,
            patch("scripts.run_parquet_etl.drop_temporary_raw_table"),
        ):
            result = run_parquet_etl._run_table(
                source_connection,
                snowflake_connection,
                audit,
                "DW_TEST",
                "RAW",
                Path("exports"),
                10_000,
                "Customers",
                "Customers",
                mapping_rows,
                output_format="csv",
                measure_memory=True,
            )

        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["output_format"], "csv")
        self.assertEqual(result["total_extracted_rows"], 2)
        self.assertGreater(result["peak_python_memory_bytes"], 0)
        export_csv.assert_called_once_with(
            source_connection,
            mapping_rows,
            Path("exports"),
            "run-csv-123",
            fetch_size=10_000,
        )
        export_parquet.assert_not_called()
        load_raw.assert_called_once_with(
            snowflake_connection,
            "DW_TEST",
            "RAW",
            mapping_rows,
            extraction.file_path,
            "run-csv-123",
            persistent_raw=False,
        )
        self.assertEqual(transform.call_args.args[5], "customers.csv")

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
            patch("scripts.run_parquet_etl.drop_temporary_raw_table") as drop_raw,
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
                raw_table='"DW_TEST"."RAW"."ETL_PARQUET_RAW_TEST"',
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
            persistent_raw=False,
        )
        transform.assert_called_once_with(
            snowflake_connection,
            "DW_TEST",
            "RAW",
            mapping_rows,
            "run-123",
            "customers.parquet",
            raw_table_reference='"DW_TEST"."RAW"."ETL_PARQUET_RAW_TEST"',
        )
        drop_raw.assert_called_once_with(
            snowflake_connection,
            '"DW_TEST"."RAW"."ETL_PARQUET_RAW_TEST"',
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
            patch("scripts.run_parquet_etl.drop_temporary_raw_table"),
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
            third_mapping = _mapping("THIRD_TABLE")
            load_mappings.return_value = [
                ("First Table", "First Table", first_mapping),
                ("Second Table", "Second Table", second_mapping),
                ("Third Table", "Third Table", third_mapping),
            ]
            export.side_effect = [
                RuntimeError("4D extraction failed"),
                _export_result("SECOND_TABLE"),
                _export_result("THIRD_TABLE"),
            ]
            load_raw.return_value = SimpleNamespace(
                loaded_rows=10,
                skipped_as_duplicate=False,
                raw_table='"DW_TEST"."RAW"."ETL_PARQUET_RAW_TEST"',
            )
            transform.side_effect = [
                RuntimeError(
                    "No rows were transformed: all source rows were rejected"
                ),
                SimpleNamespace(
                    inserted_rows=10,
                    rejected_rows=0,
                    skipped_as_duplicate=False,
                ),
            ]

            result = run_parquet_etl.main(
                [
                    "--tables",
                    "First Table",
                    "Second Table",
                    "Third Table",
                    "--database",
                    "DW_TEST",
                    "--schema",
                    "RAW",
                ]
            )

        self.assertEqual(result, 1)
        self.assertEqual(export.call_count, 3)
        self.assertEqual(load_raw.call_count, 2)
        self.assertEqual(transform.call_count, 2)
        self.assertEqual(transform.call_args_list[0].args[3], second_mapping)
        self.assertEqual(transform.call_args_list[1].args[3], third_mapping)
        pipeline_finish = audit.finish_step.call_args_list[-1]
        self.assertEqual(pipeline_finish.args[1], "PARTIAL")

    def test_rejects_nonpositive_fetch_size(self):
        with self.assertRaises(SystemExit):
            run_parquet_etl._parse_args(
                ["--table", "Customers", "--fetch-size", "0"]
            )

    def test_cli_selects_csv_format_and_optional_memory_measurement(self):
        args = run_parquet_etl._parse_args(
            ["--table", "Customers", "--format", "csv", "--measure-memory"]
        )

        self.assertEqual(args.format, "csv")
        self.assertTrue(args.measure_memory)


if __name__ == "__main__":
    unittest.main()
