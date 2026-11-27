import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts import extract_table_to_parquet as extract
from scripts import upload_parquet_exports as upload


class ParquetConnectionReuseTests(unittest.TestCase):
    def test_auto_upload_reuses_extraction_connection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export_dir = root / "exports"
            export_file = export_dir / "ACCOUNT_CONTACTS" / "batch" / "data.parquet"
            export_file.parent.mkdir(parents=True)
            export_file.write_bytes(b"test")
            archive_dir = root / "archive"
            connection = Mock()
            source = Mock()
            audit = Mock(run_id="batch")
            status = Mock()
            status.begin_extraction.return_value = True
            status.begin_upload.return_value = True
            status.eligible_batches.return_value = ["batch"]
            status.eligible_rows.return_value = [
                SimpleNamespace(
                    table_name="ACCOUNT_CONTACTS",
                    parquet_path=str(export_file.relative_to(export_dir)),
                    expected_rows=1,
                )
            ]
            result = SimpleNamespace(
                target_table="ACCOUNT_CONTACTS",
                row_count=1,
                worker_count=1,
                fetch_size=100,
                query_seconds=0.0,
                fetch_seconds=0.0,
                serialization_seconds=0.0,
                parquet_write_seconds=0.0,
                total_seconds=0.0,
                file_path=export_file,
                run_id="batch",
            )
            with (
                patch.object(extract, "configure_logging"),
                patch.object(extract, "connect_to_snowflake", return_value=connection) as connect,
                patch.object(extract, "close_snowflake_connection") as close,
                patch.object(extract, "connect_to_4d", return_value=source),
                patch.object(extract, "close_4d_connection"),
                patch.object(extract, "SnowflakeAuditLogger", return_value=audit),
                patch.object(extract, "TableStatusStore", return_value=status),
                patch.object(extract, "_discover_tables", return_value=["Account Contacts"]),
                patch.object(extract, "_source_mapping", return_value=[]),
                patch.object(extract, "export_table_to_parquet_parallel", return_value=result),
                patch.object(extract.signal, "signal"),
                patch.object(upload, "connect_to_snowflake") as upload_connect,
                patch.object(upload, "close_snowflake_connection") as upload_close,
                patch.object(upload, "TableStatusStore", return_value=status),
                patch.object(upload, "_parquet_info", return_value=("batch", "ACCOUNT_CONTACTS", ["ID"], 1)),
                patch.object(upload, "_upload_file", return_value=1) as upload_file,
            ):
                exit_code = extract.main([
                    "--table", "Account Contacts", "--export-dir", str(export_dir),
                    "--archive-dir", str(archive_dir), "--database", "DB",
                    "--schema", "SCHEMA",
                ])

            self.assertEqual(exit_code, 0)
            connect.assert_called_once_with()
            close.assert_called_once_with(connection)
            upload_connect.assert_not_called()
            upload_close.assert_not_called()
            self.assertIs(upload_file.call_args.args[0], connection)
            self.assertTrue((archive_dir / export_file.relative_to(export_dir)).is_file())
            status.mark_uploaded.assert_called_once_with("batch", "ACCOUNT_CONTACTS", 1)

    def test_standalone_uploader_owns_its_connection(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = Mock()
            status = Mock()
            status.eligible_batches.return_value = []
            with (
                patch.object(upload, "configure_logging"),
                patch.object(upload, "connect_to_snowflake", return_value=connection) as connect,
                patch.object(upload, "close_snowflake_connection") as close,
                patch.object(upload, "TableStatusStore", return_value=status),
            ):
                exit_code = upload.main([
                    "--export-dir", directory, "--database", "DB", "--schema", "SCHEMA",
                ])
            self.assertEqual(exit_code, 0)
            connect.assert_called_once_with()
            close.assert_called_once_with(connection)

    def test_borrowed_connection_is_not_closed_on_validation_error(self):
        connection = Mock()
        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(upload, "connect_to_snowflake") as connect,
                patch.object(upload, "close_snowflake_connection") as close,
            ):
                exit_code = upload.main([
                    "--export-dir", directory, "--archive-dir", directory,
                    "--database", "DB", "--schema", "SCHEMA",
                ], connection=connection)
        self.assertEqual(exit_code, 1)
        connect.assert_not_called()
        close.assert_not_called()

    def test_borrowed_connection_is_not_closed_after_upload_failure(self):
        connection = Mock()
        status = Mock()
        status.eligible_batches.return_value = ["batch"]
        status.eligible_rows.return_value = [
            SimpleNamespace(table_name="ACCOUNT_CONTACTS", parquet_path="data.parquet", expected_rows=1)
        ]
        status.begin_upload.return_value = True
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export_dir = root / "exports"
            export_dir.mkdir()
            (export_dir / "data.parquet").write_bytes(b"test")
            with (
                patch.object(upload, "connect_to_snowflake") as connect,
                patch.object(upload, "close_snowflake_connection") as close,
                patch.object(upload, "TableStatusStore", return_value=status),
                patch.object(upload, "_parquet_info", return_value=("batch", "ACCOUNT_CONTACTS", ["ID"], 1)),
                patch.object(upload, "_upload_file", side_effect=RuntimeError("upload failed")) as upload_file,
                patch.object(upload.time, "sleep"),
            ):
                exit_code = upload.main([
                    "--export-dir", str(export_dir), "--archive-dir", str(root / "archive"),
                    "--database", "DB", "--schema", "SCHEMA",
                ], connection=connection)
            self.assertTrue((export_dir / "data.parquet").is_file())
        self.assertEqual(exit_code, 1)
        self.assertEqual(upload_file.call_count, 3)
        self.assertEqual(status.mark_upload_failed.call_count, 3)
        connect.assert_not_called()
        close.assert_not_called()


if __name__ == "__main__":
    unittest.main()
