import csv
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

from scripts.export_table_load_summary import (
    _build_timing_query,
    _format_datetime,
    _qualified_name,
    export_table_load_summary,
)


class PipelineTimingReportTests(unittest.TestCase):
    def test_timing_query_excludes_pipeline_log_and_groups_per_run(self):
        query = _build_timing_query('"DW_AGAM_DEV"."UW_RAW_STG"."ETL_PIPELINE_LOG"')

        self.assertIn("UPPER(TRIM(TABLE_NAME)) <> 'ETL_PIPELINE_LOG'", query)
        self.assertIn("GROUP BY RUN_ID, TABLE_NAME", query)
        self.assertIn("MIN(START_TIME)", query)
        self.assertIn("MAX(END_TIME)", query)

    def test_qualified_name_escapes_identifier_quotes(self):
        self.assertEqual(
            _qualified_name('DW"AGAM', "UW_RAW_STG", 'TABLE"ONE'),
            '"DW""AGAM"."UW_RAW_STG"."TABLE""ONE"',
        )

    def test_datetime_format_uses_24_hour_time_and_marks_utc(self):
        self.assertEqual(
            _format_datetime(datetime(2026, 10, 21, 14, 30, 39, 185000)),
            "2026-10-21 14:30:39.185 UTC",
        )
        self.assertEqual(_format_datetime(None), "")

    def test_export_writes_each_run_with_current_table_row_count(self):
        start_time = datetime(2026, 10, 20, 8, 0)
        end_time = datetime(2026, 10, 20, 8, 1, 30)
        cursor = Mock()
        cursor.fetchall.return_value = [
            ("run-1", "ACCOUNTS", start_time, end_time, 90.0),
            ("run-2", "ACCOUNTS", start_time, end_time, 90.0),
        ]
        cursor.fetchone.side_effect = [(12,), (12,)]
        cursor.execute.return_value = cursor
        connection = Mock()
        connection.cursor.return_value = cursor

        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "summary.csv"
            count = export_table_load_summary(
                connection,
                "DW_AGAM_DEV",
                "UW_RAW_STG",
                output,
            )

            with output.open(newline="", encoding="utf-8") as csv_file:
                rows = list(csv.DictReader(csv_file))

        self.assertEqual(count, 2)
        self.assertEqual([row["RUN_ID"] for row in rows], ["run-1", "run-2"])
        self.assertEqual([row["NUMBER_OF_ROWS"] for row in rows], ["12", "12"])
        self.assertEqual(rows[0]["START_TIME"], "2026-10-20 08:00:00.000 UTC")
        self.assertEqual(rows[0]["END_TIME"], "2026-10-20 08:01:30.000 UTC")
        self.assertEqual(rows[0]["TOTAL_TIME_SECONDS"], "90.0")
        self.assertIn(
            'SELECT COUNT(*) FROM "DW_AGAM_DEV"."UW_RAW_STG"."ACCOUNTS"',
            cursor.execute.call_args_list[1].args[0],
        )
        self.assertEqual(cursor.close.call_count, 1)


if __name__ == "__main__":
    unittest.main()
