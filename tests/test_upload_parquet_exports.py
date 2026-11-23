from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import upload_parquet_exports as uploader


def _write_file(path: Path, target: str, values: list[str | None]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [pa.field("RAW_VALUE", pa.string()), pa.field("STATUS", pa.string())],
        metadata={b"target_table": target.encode("utf-8")},
    )
    pq.write_table(
        pa.Table.from_arrays([pa.array(values), pa.array(values)], schema=schema),
        path,
    )


class UploadParquetExportsTests(unittest.TestCase):
    def test_reads_all_columns_and_preserves_invalid_text(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "data.parquet"
            _write_file(path, "MESSAGES", ["bad-date", None])
            target, columns, rows = uploader._parquet_info(path)

        self.assertEqual(target, "MESSAGES")
        self.assertEqual(columns, ["RAW_VALUE", "STATUS"])
        self.assertEqual(rows, 2)

    @patch("scripts.upload_parquet_exports.release_table_run_lock")
    @patch("scripts.upload_parquet_exports.acquire_table_run_lock", return_value={"owner": "lock"})
    def test_stages_and_verifies_before_replacing_target_table(self, _acquire, _release):
        cursor = Mock()
        cursor.fetchall.side_effect = [[("UPLOADED",)], [(2, 0)]]

        def execute(sql, *_args):
            if sql.startswith("PUT "):
                cursor.description = [("status",)]
            elif sql.startswith("COPY INTO "):
                cursor.description = [("rows_loaded",), ("errors_seen",)]
            return cursor

        cursor.execute.side_effect = execute
        connection = Mock()
        connection.cursor.return_value = cursor
        result = uploader._upload_file(
            connection,
            "DB",
            "RAW",
            Path("messages.parquet"),
            "MESSAGES",
            ["RAW_VALUE", "STATUS"],
            2,
        )

        self.assertEqual(result, 2)
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertTrue(any('"RAW_VALUE" VARCHAR, "STATUS" VARCHAR' in sql for sql in statements))
        copy_index = next(i for i, sql in enumerate(statements) if sql.startswith("COPY INTO "))
        self.assertIn("NULL_IF=()", statements[copy_index])
        replace_index = next(
            i for i, sql in enumerate(statements)
            if sql.startswith('CREATE OR REPLACE TABLE "DB"."RAW"."MESSAGES" ')
        )
        self.assertLess(copy_index, replace_index)
        self.assertIn("COPY GRANTS", statements[replace_index])

    @patch("scripts.upload_parquet_exports.release_table_run_lock")
    @patch("scripts.upload_parquet_exports.acquire_table_run_lock", return_value={"owner": "lock"})
    def test_failed_copy_does_not_replace_target_table(self, _acquire, _release):
        cursor = Mock()
        cursor.fetchall.side_effect = [[("UPLOADED",)], [(1, 1)]]

        def execute(sql, *_args):
            if sql.startswith("PUT "):
                cursor.description = [("status",)]
            elif sql.startswith("COPY INTO "):
                cursor.description = [("rows_loaded",), ("errors_seen",)]
            return cursor

        cursor.execute.side_effect = execute
        connection = Mock()
        connection.cursor.return_value = cursor
        with self.assertRaisesRegex(RuntimeError, "loaded 1 of 2"):
            uploader._upload_file(
                connection, "DB", "RAW", Path("data.parquet"), "MESSAGES", ["VALUE"], 2
            )
        self.assertFalse(any(
            call.args[0].startswith("CREATE OR REPLACE TABLE")
            for call in cursor.execute.call_args_list
        ))

    @patch("scripts.upload_parquet_exports.configure_logging")
    @patch("scripts.upload_parquet_exports.close_snowflake_connection")
    @patch("scripts.upload_parquet_exports.connect_to_snowflake")
    @patch("scripts.upload_parquet_exports._upload_file", return_value=1)
    def test_uploads_oldest_first_and_archives_after_success(
        self, upload, _connect, _close, _logging
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            exports = root / "exports"
            archive = root / "uploaded"
            older = exports / "MESSAGES" / "old" / "data.parquet"
            newer = exports / "MESSAGES" / "new" / "data.parquet"
            _write_file(older, "MESSAGES", ["old"])
            _write_file(newer, "MESSAGES", ["new"])
            os.utime(older, (100, 100))
            os.utime(newer, (200, 200))
            result = uploader.main([
                "--export-dir", str(exports),
                "--archive-dir", str(archive),
                "--database", "DB",
                "--schema", "RAW",
            ])
            self.assertEqual(result, 0)
            self.assertEqual([call.args[3] for call in upload.call_args_list], [older, newer])
            self.assertFalse(older.exists())
            self.assertFalse(newer.exists())
            self.assertTrue((archive / "MESSAGES" / "old" / "data.parquet").is_file())
            self.assertTrue((archive / "MESSAGES" / "new" / "data.parquet").is_file())

    @patch("scripts.upload_parquet_exports.configure_logging")
    @patch("scripts.upload_parquet_exports.close_snowflake_connection")
    @patch("scripts.upload_parquet_exports.connect_to_snowflake")
    @patch("scripts.upload_parquet_exports._upload_file", side_effect=RuntimeError("COPY failed"))
    def test_failed_upload_keeps_files_and_blocks_later_snapshot(
        self, upload, _connect, _close, _logging
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            exports = root / "exports"
            older = exports / "MESSAGES" / "old" / "data.parquet"
            newer = exports / "MESSAGES" / "new" / "data.parquet"
            _write_file(older, "MESSAGES", ["old"])
            _write_file(newer, "MESSAGES", ["new"])
            os.utime(older, (100, 100))
            os.utime(newer, (200, 200))
            result = uploader.main([
                "--export-dir", str(exports),
                "--archive-dir", str(root / "uploaded"),
                "--database", "DB",
                "--schema", "RAW",
            ])
            self.assertEqual(result, 1)
            self.assertTrue(older.exists())
            self.assertTrue(newer.exists())
            self.assertFalse((root / "uploaded").exists())
            upload.assert_called_once()

    def test_archive_does_not_overwrite_existing_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.parquet"
            destination = root / "uploaded" / "source.parquet"
            source.write_bytes(b"new")
            destination.parent.mkdir()
            destination.write_bytes(b"old")
            with self.assertRaises(FileExistsError):
                uploader._archive_file(source, destination)
            self.assertEqual(source.read_bytes(), b"new")
            self.assertEqual(destination.read_bytes(), b"old")

    @patch("scripts.upload_parquet_exports.configure_logging")
    @patch("scripts.upload_parquet_exports.connect_to_snowflake")
    def test_missing_table_metadata_stops_before_upload(self, connect, _logging):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "exports" / "data.parquet"
            path.parent.mkdir()
            pq.write_table(pa.table({"VALUE": ["data"]}), path)
            result = uploader.main([
                "--export-dir", str(path.parent),
                "--archive-dir", str(root / "uploaded"),
                "--database", "DB",
                "--schema", "RAW",
            ])
            self.assertEqual(result, 1)
            self.assertTrue(path.exists())
            connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
