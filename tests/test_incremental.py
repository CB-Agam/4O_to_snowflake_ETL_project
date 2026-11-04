from __future__ import annotations

import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

from etl import incremental
from scripts.run_initial_etl import _parse_args


def _claim_mapping() -> list[dict[str, str]]:
    return [
        {
            "source_table": "Claims",
            "source_column": "Claim ID",
            "source_data_type": "INTEGER",
            "target_table": "CLAIMS",
            "target_column": "CLAIM_ID",
            "snowflake_datatype": "NUMBER",
            "constraint": "PK",
        },
        {
            "source_table": "Claims",
            "source_column": "Modified On",
            "source_data_type": "TIMESTAMP",
            "target_table": "CLAIMS",
            "target_column": "MODIFIED_ON",
            "snowflake_datatype": "TIMESTAMP_NTZ",
            "constraint": "",
        },
        {
            "source_table": "Claims",
            "source_column": "Amount",
            "source_data_type": "NUMERIC",
            "target_table": "CLAIMS",
            "target_column": "AMOUNT",
            "snowflake_datatype": "NUMBER(18, 2)",
            "constraint": "",
        },
    ]


def _claim_mapping_with_created_on() -> list[dict[str, str]]:
    rows = _claim_mapping()
    rows.insert(
        1,
        {
            "source_table": "Claims",
            "source_column": "Created On",
            "source_data_type": "TIMESTAMP",
            "target_table": "CLAIMS",
            "target_column": "CREATED_ON",
            "snowflake_datatype": "TIMESTAMP_NTZ",
            "constraint": "",
        },
    )
    return rows


def _timestamp_metadata_cursor():
    cursor = Mock()
    cursor.columns.return_value = [
        SimpleNamespace(column_name="Claim ID", type_name="INTEGER"),
        SimpleNamespace(column_name="Created On", type_name="TIMESTAMP"),
        SimpleNamespace(column_name="Modified On", type_name="TIMESTAMP"),
        SimpleNamespace(column_name="Amount", type_name="NUMERIC"),
    ]
    return cursor


class IncrementalStrategyTests(unittest.TestCase):
    def test_temporal_source_projection_avoids_odbc_datetime_conversion(self):
        query = incremental._source_projection_query(
            "Claims",
            ["Claim ID", "Modified On"],
            _claim_mapping(),
        )

        self.assertIn("SELECT [Claim ID], CAST([Modified On] AS VARCHAR(100))", query)
        self.assertIn("AS [Modified On] FROM [Claims]", query)

    def test_out_of_range_timestamp_text_is_left_for_row_level_load_handling(self):
        self.assertIsNone(
            incremental._normalize_optional_timestamp("12015-01-01 00:00:00", "Modified On")
        )
        self.assertEqual(
            incremental._normalize_optional_timestamp(
                "2025-01-02 03:04:05",
                "Modified On",
            ),
            datetime(2025, 1, 2, 3, 4, 5),
        )

    def test_failed_timestamp_row_keeps_previous_state_for_next_run(self):
        old_modified = datetime(2024, 1, 1)
        old_created = datetime(2023, 1, 1)
        old_state = {
            "2": ("old-hash", old_modified, old_created),
        }
        new_state = [
            ("CLAIMS", "1", "hash-1", datetime(2024, 1, 2), old_created),
            ("CLAIMS", "2", "new-hash", datetime(2024, 1, 3), old_created),
        ]
        failed_rows = pd.DataFrame(
            {"CLAIM_ID": [2], "MODIFIED_ON": [datetime(2024, 1, 3)]}
        )

        pending = incremental._keep_failed_rows_pending(
            new_state,
            old_state,
            failed_rows,
            "CLAIMS",
            {
                "strategy": "KEY_TIMESTAMP",
                "key_target_column": "CLAIM_ID",
            },
        )

        self.assertEqual(
            pending,
            [
                ("CLAIMS", "1", "hash-1", datetime(2024, 1, 2), old_created),
                ("CLAIMS", "2", "old-hash", old_modified, old_created),
            ],
        )

    def test_day_zero_row_state_contains_complete_initial_dataset(self):
        rows = _claim_mapping()
        data = pd.DataFrame(
            {
                "CLAIM_ID": [1, 2],
                "MODIFIED_ON": [
                    datetime(2025, 1, 1),
                    datetime(2025, 1, 2),
                ],
                "AMOUNT": [10, 20],
            }
        )

        state = incremental._rows_as_state(
            data,
            "CLAIMS",
            ["CLAIM_ID", "MODIFIED_ON", "AMOUNT"],
            incremental.select_change_strategy(rows),
        )

        self.assertEqual(len(state), 2)
        self.assertEqual([item[1] for item in state], ["1", "2"])
        self.assertEqual(state[0][3], datetime(2025, 1, 1))

    def test_day_zero_state_allows_null_modification_timestamp(self):
        data = pd.DataFrame(
            {
                "CLAIM_ID": [1],
                "MODIFIED_ON": [None],
                "AMOUNT": [10],
            }
        )

        state = incremental._rows_as_state(
            data,
            "CLAIMS",
            ["CLAIM_ID", "MODIFIED_ON", "AMOUNT"],
            incremental.select_change_strategy(_claim_mapping()),
        )

        self.assertEqual(state[0][1], "1")
        self.assertIsNone(state[0][3])

    def test_timestamp_and_confirmed_single_key_select_optimized_strategy(self):
        strategy = incremental.select_change_strategy(_claim_mapping())

        self.assertEqual(strategy["strategy"], "KEY_TIMESTAMP")
        self.assertEqual(strategy["key_source_column"], "Claim ID")
        self.assertIsNone(strategy["created_source_column"])
        self.assertEqual(strategy["modified_source_column"], "Modified On")

    def test_created_and_modified_timestamps_are_both_selected(self):
        strategy = incremental.select_change_strategy(_claim_mapping_with_created_on())

        self.assertEqual(strategy["strategy"], "KEY_TIMESTAMP")
        self.assertEqual(strategy["created_source_column"], "Created On")
        self.assertEqual(strategy["modified_source_column"], "Modified On")

    def test_created_timestamp_state_migration_is_only_needed_for_known_values(self):
        created_at = datetime(2025, 1, 1)
        old_state = {"1": ("row-hash", datetime(2025, 1, 2))}

        self.assertTrue(
            incremental._needs_created_timestamp_refresh(
                old_state,
                [("CLAIMS", "1", "row-hash", datetime(2025, 1, 2), created_at)],
            )
        )
        self.assertFalse(
            incremental._needs_created_timestamp_refresh(
                old_state,
                [("CLAIMS", "1", "row-hash", datetime(2025, 1, 2), None)],
            )
        )
        self.assertFalse(
            incremental._needs_created_timestamp_refresh(
                {
                    "1": (
                        "row-hash",
                        datetime(2025, 1, 2),
                        created_at,
                    )
                },
                [("CLAIMS", "1", "row-hash", datetime(2025, 1, 2), created_at)],
            )
        )

    def test_created_timestamp_without_modified_uses_key_hash_full_scan(self):
        rows = [
            row for row in _claim_mapping_with_created_on()
            if row["source_column"] != "Modified On"
        ]

        self.assertEqual(incremental.select_change_strategy(rows)["strategy"], "KEY_HASH")

    def test_unkeyed_alerts_use_full_row_hash_fallback(self):
        rows = _claim_mapping()
        rows[0]["constraint"] = "No unique values"
        self.assertEqual(incremental.select_change_strategy(rows)["strategy"], "ROW_HASH")

    def test_timestamp_without_unique_key_uses_full_row_hash_fallback(self):
        rows = _claim_mapping()
        for row in rows:
            row["constraint"] = ""
        self.assertEqual(incremental.select_change_strategy(rows)["strategy"], "ROW_HASH")

    @patch("etl.incremental.pd.read_sql")
    def test_row_hash_fallback_preserves_duplicate_record_multiplicity(self, read_sql):
        duplicate = ("same", 5)
        row_hash = incremental._row_hash(duplicate)
        read_sql.return_value = pd.DataFrame(
            [duplicate, duplicate],
            columns=["NAME", "VALUE"],
        )
        rows = [
            {"source_table": "Alerts", "source_column": "Name", "target_column": "NAME"},
            {"source_table": "Alerts", "source_column": "Value", "target_column": "VALUE"},
        ]

        changed, new_state, _, deleted_count = incremental._discover_hash_changes(
            Mock(),
            "ALERTS",
            rows,
            {f"{row_hash}:1": (row_hash, None)},
        )

        self.assertEqual(len(changed), 1)
        self.assertEqual([row[1] for row in new_state], [f"{row_hash}:1", f"{row_hash}:2"])
        self.assertEqual(deleted_count, 0)

    def test_multiple_key_columns_are_not_assumed_to_be_a_single_key(self):
        rows = _claim_mapping()
        rows[2]["constraint"] = "PK"
        strategy = incremental.select_change_strategy(rows)
        self.assertEqual(strategy["strategy"], "KEY_HASH")
        self.assertEqual(strategy["key_target_columns"], ["CLAIM_ID", "AMOUNT"])

    def test_primary_key_without_modified_timestamp_uses_key_hash_strategy(self):
        rows = [
            row for row in _claim_mapping()
            if row["source_column"] != "Modified On"
        ]

        strategy = incremental.select_change_strategy(rows)

        self.assertEqual(strategy["strategy"], "KEY_HASH")
        self.assertEqual(strategy["key_target_columns"], ["CLAIM_ID"])

    @patch("etl.incremental.pd.read_sql")
    def test_key_hash_fallback_distinguishes_new_updated_unchanged_and_deleted(
        self,
        read_sql,
    ):
        unchanged = (1, "same")
        changed = (2, "old")
        deleted = (3, "gone")
        rows = [
            {
                "source_table": "Claims",
                "source_column": "Claim ID",
                "target_column": "CLAIM_ID",
                "constraint": "PK",
            },
            {
                "source_table": "Claims",
                "source_column": "Amount",
                "target_column": "AMOUNT",
                "constraint": "",
            },
        ]
        read_sql.return_value = pd.DataFrame(
            [unchanged, (2, "new"), (4, "inserted")],
            columns=["CLAIM_ID", "AMOUNT"],
        )
        old_state = {
            incremental._row_hash((1,)): (incremental._row_hash(unchanged), None),
            incremental._row_hash((2,)): (incremental._row_hash(changed), None),
            incremental._row_hash((3,)): (incremental._row_hash(deleted), None),
        }

        changed_df, new_state, source_count, inserted, updated, deleted_count = (
            incremental._discover_key_hash_changes(
                Mock(),
                "CLAIMS",
                rows,
                old_state,
            )
        )

        self.assertEqual(changed_df.to_dict("records"), [
            {"CLAIM_ID": 2, "AMOUNT": "new"},
            {"CLAIM_ID": 4, "AMOUNT": "inserted"},
        ])
        self.assertEqual((source_count, inserted, updated, deleted_count), (3, 1, 1, 1))
        self.assertEqual(
            [row[1] for row in new_state],
            [incremental._row_hash((1,)), incremental._row_hash((2,)), incremental._row_hash((4,))],
        )

    def test_duplicate_declared_source_key_fails_validation(self):
        rows = _claim_mapping()
        data = pd.DataFrame(
            {
                "CLAIM_ID": [1, 1],
                "MODIFIED_ON": [datetime(2025, 1, 1)] * 2,
                "AMOUNT": [10, 11],
            }
        )

        with self.assertRaisesRegex(ValueError, "not unique"):
            incremental._rows_as_state(
                data,
                "CLAIMS",
                ["CLAIM_ID", "MODIFIED_ON", "AMOUNT"],
                incremental.select_change_strategy(rows),
            )

    @patch("etl.incremental.pd.read_sql")
    def test_row_hash_fallback_finds_new_and_deleted_unkeyed_rows(self, read_sql):
        unchanged = ("a", 1)
        deleted = ("b", 2)
        added = ("c", 3)
        old_state = {
            f"{incremental._row_hash(unchanged)}:1": (incremental._row_hash(unchanged), None),
            f"{incremental._row_hash(deleted)}:1": (incremental._row_hash(deleted), None),
        }
        read_sql.return_value = pd.DataFrame([unchanged, added], columns=["NAME", "VALUE"])
        rows = [
            {
                "source_table": "Alerts",
                "source_column": "Name",
                "target_column": "NAME",
            },
            {
                "source_table": "Alerts",
                "source_column": "Value",
                "target_column": "VALUE",
            },
        ]

        changed, new_state, source_count, deleted_count = incremental._discover_hash_changes(
            Mock(), "ALERTS", rows, old_state
        )

        self.assertEqual(source_count, 2)
        self.assertEqual(changed.to_dict("records"), [{"NAME": "c", "VALUE": 3}])
        self.assertEqual(deleted_count, 1)
        self.assertTrue(all(row[0] == "ALERTS" for row in new_state))

    @patch("etl.incremental.pd.read_sql")
    def test_row_hash_fallback_detects_no_change_without_duplicate_history_work(self, read_sql):
        row = ("a", 1)
        row_hash = incremental._row_hash(row)
        read_sql.return_value = pd.DataFrame([row], columns=["NAME", "VALUE"])
        rows = [
            {"source_table": "Alerts", "source_column": "Name", "target_column": "NAME"},
            {"source_table": "Alerts", "source_column": "Value", "target_column": "VALUE"},
        ]

        changed, new_state, source_count, deleted_count = incremental._discover_hash_changes(
            Mock(), "ALERTS", rows, {f"{row_hash}:1": (row_hash, None)}
        )

        self.assertTrue(changed.empty)
        self.assertEqual(len(new_state), 1)
        self.assertEqual(source_count, 1)
        self.assertEqual(deleted_count, 0)

    @patch("etl.incremental.pd.read_sql")
    def test_row_hash_snapshots_remain_independent_for_multiple_tables(self, read_sql):
        read_sql.side_effect = [
            pd.DataFrame({"VALUE": [1]}),
            pd.DataFrame({"VALUE": [2]}),
        ]
        first_mapping = [
            {"source_table": "First Source", "source_column": "Value", "target_column": "VALUE"}
        ]
        second_mapping = [
            {"source_table": "Second Source", "source_column": "Value", "target_column": "VALUE"}
        ]

        _, first_state, _, _ = incremental._discover_hash_changes(
            Mock(), "FIRST_TARGET", first_mapping, {}
        )
        _, second_state, _, _ = incremental._discover_hash_changes(
            Mock(), "SECOND_TARGET", second_mapping, {}
        )

        self.assertEqual(first_state[0][0], "FIRST_TARGET")
        self.assertEqual(second_state[0][0], "SECOND_TARGET")
        self.assertNotEqual(first_state[0][2], second_state[0][2])

    @patch("etl.incremental.pd.read_sql")
    def test_key_timestamp_detects_new_updated_deleted_and_timestamp_tie_rows(self, read_sql):
        timestamp = datetime(2025, 1, 2, 12)
        old_timestamp = datetime(2025, 1, 1, 12)
        unchanged_hash = incremental._row_hash((1, timestamp, "old"))
        old_state = {
            "1": (unchanged_hash, timestamp),
            "2": (incremental._row_hash((2, old_timestamp, "gone")), old_timestamp),
        }
        read_sql.side_effect = [
            pd.DataFrame(
                {
                    "Claim ID": [1, 3],
                    "Modified On": [timestamp, timestamp],
                }
            ),
            pd.DataFrame(
                {
                    "CLAIM_ID": [1, 3],
                    "MODIFIED_ON": [timestamp, timestamp],
                    "AMOUNT": ["changed", "new"],
                }
            ),
        ]
        fourd = Mock()
        fourd.cursor.return_value = _timestamp_metadata_cursor()

        changed, new_state, scan_count, inserted, updated, deleted = (
            incremental._discover_timestamp_changes(
                fourd,
                "CLAIMS",
                _claim_mapping(),
                old_state,
                timestamp,
                0,
            )
        )

        self.assertEqual(changed["CLAIM_ID"].tolist(), [1, 3])
        self.assertEqual((scan_count, inserted, updated, deleted), (2, 1, 1, 1))
        self.assertEqual(len(new_state), 2)
        self.assertEqual(read_sql.call_count, 2)

    @patch("etl.incremental.pd.read_sql")
    def test_key_timestamp_no_change_with_watermark_tie_does_not_advance_watermark(self, read_sql):
        timestamp = datetime(2024, 1, 1)
        old_hash = incremental._row_hash((1, timestamp, 10))
        read_sql.side_effect = [
            pd.DataFrame({"Claim ID": [1], "Modified On": [timestamp]}),
            pd.DataFrame(
                {
                    "CLAIM_ID": [1],
                    "MODIFIED_ON": [timestamp],
                    "AMOUNT": [10],
                }
            ),
        ]
        fourd = Mock()
        fourd.cursor.return_value = _timestamp_metadata_cursor()

        changed, state, scan_count, inserted, updated, deleted = (
            incremental._discover_timestamp_changes(
                fourd,
                "CLAIMS",
                _claim_mapping(),
                {"1": (old_hash, timestamp)},
                timestamp,
                0,
            )
        )

        self.assertTrue(changed.empty)
        self.assertEqual((scan_count, inserted, updated, deleted), (1, 0, 0, 0))
        self.assertEqual(state[0][3], timestamp)
        self.assertEqual(read_sql.call_count, 2)

    @patch("etl.incremental.pd.read_sql")
    def test_null_timestamp_rows_are_rehashed_and_changes_are_detected(self, read_sql):
        old_data = (1, None, "old")
        old_hash = incremental._row_hash(old_data)
        read_sql.side_effect = [
            pd.DataFrame({"Claim ID": [1], "Modified On": [None]}),
            pd.DataFrame(
                {
                    "CLAIM_ID": [1],
                    "MODIFIED_ON": [None],
                    "AMOUNT": ["changed"],
                }
            ),
        ]
        fourd = Mock()
        fourd.cursor.return_value = _timestamp_metadata_cursor()

        changed, state, _, inserted, updated, deleted = (
            incremental._discover_timestamp_changes(
                fourd,
                "CLAIMS",
                _claim_mapping(),
                {"1": (old_hash, None)},
                None,
                86400,
            )
        )

        self.assertEqual(changed["CLAIM_ID"].tolist(), [1])
        self.assertEqual((inserted, updated, deleted), (0, 1, 0))
        self.assertIsNone(state[0][3])
        self.assertEqual(read_sql.call_count, 2)

    @patch("etl.incremental.pd.read_sql")
    def test_unchanged_null_timestamp_row_is_rechecked_but_not_changed(self, read_sql):
        record = (1, None, 10)
        row_hash = incremental._row_hash(record)
        read_sql.side_effect = [
            pd.DataFrame({"Claim ID": [1], "Modified On": [None]}),
            pd.DataFrame(
                {
                    "CLAIM_ID": [1],
                    "MODIFIED_ON": [None],
                    "AMOUNT": [10],
                }
            ),
        ]
        fourd = Mock()
        fourd.cursor.return_value = _timestamp_metadata_cursor()

        changed, state, _, inserted, updated, deleted = (
            incremental._discover_timestamp_changes(
                fourd,
                "CLAIMS",
                _claim_mapping(),
                {"1": (row_hash, None)},
                None,
                86400,
            )
        )

        self.assertTrue(changed.empty)
        self.assertEqual((inserted, updated, deleted), (0, 0, 0))
        self.assertEqual(state[0][2:], (row_hash, None, None))
        self.assertEqual(read_sql.call_count, 2)

    @patch("etl.incremental.pd.read_sql")
    def test_created_change_and_both_null_timestamps_are_hash_compared(self, read_sql):
        old_created = datetime(2025, 1, 1)
        new_created = datetime(2025, 1, 2)
        old_modified = datetime(2025, 1, 3)
        rows = _claim_mapping_with_created_on()
        old_row_hash = incremental._row_hash((1, old_created, old_modified, 10))
        null_row_hash = incremental._row_hash((2, None, None, 20))
        read_sql.side_effect = [
            pd.DataFrame(
                {
                    "Claim ID": [1, 2],
                    "Created On": [new_created, None],
                    "Modified On": [old_modified, None],
                }
            ),
            pd.DataFrame(
                {
                    "CLAIM_ID": [1, 2],
                    "CREATED_ON": [new_created, None],
                    "MODIFIED_ON": [old_modified, None],
                    "AMOUNT": [10, 20],
                }
            ),
        ]
        fourd = Mock()
        fourd.cursor.return_value = _timestamp_metadata_cursor()

        changed, state, _, inserted, updated, deleted = (
            incremental._discover_timestamp_changes(
                fourd,
                "CLAIMS",
                rows,
                {
                    "1": (old_row_hash, old_modified, old_created),
                    "2": (null_row_hash, None, None),
                },
                datetime(2025, 1, 3),
                0,
            )
        )

        self.assertEqual(changed["CLAIM_ID"].tolist(), [1])
        self.assertEqual((inserted, updated, deleted), (0, 1, 0))
        self.assertEqual(state[0][4], new_created)
        self.assertEqual(state[1][2], null_row_hash)
        self.assertEqual(read_sql.call_count, 2)

    def test_source_extraction_failure_is_not_treated_as_no_change(self):
        rows = [
            {"source_table": "Alerts", "source_column": "Name", "target_column": "NAME"},
        ]

        with patch(
            "etl.incremental.pd.read_sql",
            side_effect=RuntimeError("4D extraction failed"),
        ) as read_sql:
            with self.assertRaisesRegex(RuntimeError, "4D extraction failed"):
                incremental._discover_hash_changes(Mock(), "ALERTS", rows, {})
        read_sql.assert_called_once()

    def test_run_cli_exposes_incremental_and_overlap_options(self):
        args = _parse_args(
            ["--table", "CLAIMS", "--incremental", "--overlap-seconds", "3600"]
        )

        self.assertTrue(args.incremental)
        self.assertEqual(args.overlap_seconds, 3600)

    def test_lock_acquisition_fails_when_another_run_owns_the_table(self):
        cursor = Mock()
        cursor.execute.return_value = cursor
        cursor.fetchone.return_value = (1,)
        cursor.rowcount = 0
        connection = Mock()

        with self.assertRaisesRegex(RuntimeError, "Another incremental run"):
            incremental._acquire_lock(
                cursor,
                connection,
                "STATE",
                "CLAIMS",
                "KEY_TIMESTAMP",
                "batch-owner",
            )

        connection.rollback.assert_called_once()


class IncrementalTransactionTests(unittest.TestCase):
    def _patch_initial_dependencies(self, stack):
        paths = {
            "state": '"DW_TEST"."RAW_TEST"."ETL_INCREMENTAL_TABLE_STATE"',
            "row_state": '"DW_TEST"."RAW_TEST"."ETL_SOURCE_ROW_STATE"',
            "batch_log": '"DW_TEST"."RAW_TEST"."ETL_INCREMENTAL_BATCH_LOG"',
        }
        stack.enter_context(patch.object(incremental, "_ensure_history_table"))
        stack.enter_context(patch.object(incremental, "_ensure_helper_tables", return_value=paths))
        stack.enter_context(patch.object(incremental, "_acquire_lock"))
        stack.enter_context(patch.object(incremental, "_renew_lock"))
        stack.enter_context(patch.object(
            incremental,
            "_read_table_state",
            return_value={
                "strategy": "KEY_TIMESTAMP",
                "initialized": False,
                "watermark": None,
                "last_batch_id": None,
                "source_count": None,
            },
        ))
        stack.enter_context(patch.object(incremental, "_read_row_state", return_value={}))
        stack.enter_context(patch.object(incremental, "_start_batch"))
        stack.enter_context(patch.object(incremental, "_replace_row_state"))
        stack.enter_context(patch.object(incremental, "_finish_batch"))
        stack.enter_context(patch.object(incremental, "_release_lock"))
        stack.enter_context(patch.object(incremental, "_validate_timestamp_metadata"))
        stack.enter_context(patch.object(incremental, "load_dataframe_to_snowflake", return_value=2))
        return paths

    def test_initial_full_load_creates_empty_history_without_archiving(self):
        import contextlib

        cursor = Mock()
        cursor.execute.return_value = cursor
        cursor.fetchone.return_value = (0,)
        connection = Mock()
        initial = pd.DataFrame(
            {
                "CLAIM_ID": [1, 2],
                "MODIFIED_ON": [datetime(2025, 1, 1), datetime(2025, 1, 2)],
                "AMOUNT": [10, 20],
            }
        )
        mapped_rows = _claim_mapping()
        with contextlib.ExitStack() as stack:
            self._patch_initial_dependencies(stack)
            stage_loader = stack.enter_context(
                patch.object(incremental, "load_dataframe_to_snowflake")
            )
            stack.enter_context(patch("etl.incremental.pd.read_sql", return_value=initial))
            result = incremental.incremental_load_table(
                Mock(),
                connection,
                cursor,
                "DW_TEST",
                "RAW_TEST",
                "CLAIMS",
                mapped_rows,
                "run-1",
            )

        self.assertEqual(result["rows_loaded"], 2)
        self.assertEqual(result["status"], "SUCCESS")
        loaded_columns = stage_loader.call_args.args[2]
        loaded_frame = stage_loader.call_args.args[3]
        self.assertEqual(loaded_columns, ["CLAIM_ID", "MODIFIED_ON", "AMOUNT"])
        self.assertEqual(
            list(loaded_frame.columns),
            ["CLAIM_ID", "MODIFIED_ON", "AMOUNT"],
        )
        sql_calls = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertFalse(
            any("INSERT INTO" in sql and '"CLAIMS_H"' in sql for sql in sql_calls)
        )
        self.assertIn("CREATE TEMPORARY TABLE", "\n".join(sql_calls))
        self.assertFalse(
            any("ADD COLUMN IF NOT EXISTS" in sql for sql in sql_calls)
        )

    def test_initial_incremental_load_records_and_keeps_failed_row_pending(self):
        import contextlib

        cursor = Mock()
        cursor.execute.return_value = cursor
        cursor.fetchone.return_value = (0,)
        connection = Mock()
        initial = pd.DataFrame(
            {
                "CLAIM_ID": [1, 2],
                "MODIFIED_ON": [
                    datetime(2025, 1, 1),
                    "12015-01-02 00:00:00",
                ],
                "AMOUNT": [10, 20],
            }
        )
        exception_logger = Mock()
        with contextlib.ExitStack() as stack:
            self._patch_initial_dependencies(stack)
            read_sql = stack.enter_context(
                patch("etl.incremental.pd.read_sql", return_value=initial)
            )
            stage_load = stack.enter_context(
                patch.object(
                    incremental,
                    "load_dataframe_to_snowflake_with_exceptions",
                    return_value={
                        "rows_loaded": 1,
                        "rows_failed": 1,
                        "failed_indexes": [1],
                    },
                )
            )
            replace_state = stack.enter_context(
                patch.object(incremental, "_replace_row_state")
            )
            finish = stack.enter_context(patch.object(incremental, "_finish_batch"))

            result = incremental.incremental_load_table(
                Mock(),
                connection,
                cursor,
                "DW_TEST",
                "RAW_TEST",
                "CLAIMS",
                _claim_mapping(),
                "run-with-row-error",
                exception_logger=exception_logger,
            )

        self.assertEqual(result["status"], "SUCCESS_WITH_ERRORS")
        self.assertEqual(result["rows_processed"], 2)
        self.assertEqual(result["rows_loaded"], 1)
        self.assertEqual(result["rows_failed"], 1)
        self.assertEqual(result["exception_count"], 1)
        self.assertEqual(finish.call_args.args[3], "SUCCESS_WITH_ERRORS")
        self.assertEqual(finish.call_args.args[4]["failed"], 1)
        self.assertEqual(finish.call_args.args[4]["exception_count"], 1)
        self.assertEqual(
            [row[1] for row in replace_state.call_args.args[3]],
            ["1"],
        )
        self.assertEqual(
            stage_load.call_args.kwargs["target_table_name"],
            "CLAIMS",
        )
        self.assertIn("CAST([Modified On] AS VARCHAR(100))", read_sql.call_args.args[0])

    def test_no_change_batch_keeps_current_history_and_watermark_unchanged(self):
        import contextlib

        cursor = Mock()
        cursor.execute.return_value = cursor
        cursor.fetchone.return_value = (1,)
        connection = Mock()
        paths = {
            "state": "STATE",
            "row_state": "ROW_STATE",
            "batch_log": "BATCH_LOG",
        }
        timestamp = datetime(2024, 1, 1)
        old_hash = incremental._row_hash((1, timestamp, 10))
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(incremental, "_ensure_history_table"))
            stack.enter_context(patch.object(incremental, "_ensure_helper_tables", return_value=paths))
            stack.enter_context(patch.object(incremental, "_acquire_lock"))
            stack.enter_context(patch.object(incremental, "_renew_lock"))
            stack.enter_context(patch.object(
                incremental,
                "_read_table_state",
                return_value={
                    "strategy": "KEY_TIMESTAMP",
                    "initialized": True,
                    "watermark": timestamp,
                    "last_batch_id": "old",
                    "source_count": 1,
                },
            ))
            stack.enter_context(patch.object(incremental, "_read_row_state", return_value={"1": (old_hash, timestamp)}))
            stack.enter_context(patch.object(incremental, "_start_batch"))
            finish = stack.enter_context(patch.object(incremental, "_finish_batch"))
            release = stack.enter_context(patch.object(incremental, "_release_lock"))
            replace_state = stack.enter_context(
                patch.object(incremental, "_replace_row_state")
            )
            stack.enter_context(patch.object(incremental, "_validate_timestamp_metadata"))
            stack.enter_context(patch.object(incremental, "_discover_timestamp_changes", return_value=(
                pd.DataFrame(
                    columns=["CLAIM_ID", "CREATED_ON", "MODIFIED_ON", "AMOUNT"]
                ),
                [("CLAIMS", "1", old_hash, timestamp, datetime(2023, 1, 1))],
                1,
                0,
                0,
                0,
            )))
            stage_loader = stack.enter_context(patch.object(incremental, "load_dataframe_to_snowflake"))

            result = incremental.incremental_load_table(
                Mock(),
                connection,
                cursor,
                "DW_TEST",
                "RAW_TEST",
                "CLAIMS",
                _claim_mapping_with_created_on(),
                "run-2",
            )

        self.assertEqual(result["status"], "NO_CHANGE")
        self.assertEqual(result["watermark"], timestamp)
        stage_loader.assert_not_called()
        finish.assert_called_once()
        self.assertEqual(finish.call_args.args[3], "SUCCESS")
        self.assertEqual(finish.call_args.kwargs["change_status"], "NO_CHANGE")
        replace_state.assert_called_once_with(
            cursor,
            "ROW_STATE",
            "CLAIMS",
            [("CLAIMS", "1", old_hash, timestamp, datetime(2023, 1, 1))],
        )
        release.assert_called_once()
        self.assertFalse(any("DELETE FROM" in call.args[0] for call in cursor.execute.call_args_list))

    def test_source_extraction_failure_rolls_back_and_records_failed_batch(self):
        import contextlib

        cursor = Mock()
        cursor.execute.return_value = cursor
        cursor.fetchone.return_value = (1,)
        connection = Mock()
        paths = {"state": "STATE", "row_state": "ROW_STATE", "batch_log": "BATCH_LOG"}
        timestamp = datetime(2024, 1, 1)
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(incremental, "_ensure_history_table"))
            stack.enter_context(patch.object(incremental, "_ensure_helper_tables", return_value=paths))
            stack.enter_context(patch.object(incremental, "_acquire_lock"))
            stack.enter_context(patch.object(incremental, "_renew_lock"))
            stack.enter_context(patch.object(
                incremental,
                "_read_table_state",
                return_value={
                    "strategy": "KEY_TIMESTAMP",
                    "initialized": True,
                    "watermark": timestamp,
                    "last_batch_id": "old",
                    "source_count": 1,
                },
            ))
            stack.enter_context(patch.object(incremental, "_read_row_state", return_value={"1": ("hash", timestamp)}))
            stack.enter_context(patch.object(incremental, "_start_batch"))
            finish = stack.enter_context(patch.object(incremental, "_finish_batch"))
            stack.enter_context(patch.object(incremental, "_release_lock"))
            stack.enter_context(patch.object(incremental, "_discover_timestamp_changes", side_effect=RuntimeError("source offline")))

            with self.assertRaisesRegex(RuntimeError, "source offline"):
                incremental.incremental_load_table(
                    Mock(), connection, cursor, "DW_TEST", "RAW_TEST", "CLAIMS", _claim_mapping(), "run-3"
                )

        connection.rollback.assert_called_once()
        self.assertEqual(finish.call_args.args[3], "FAILED")
        self.assertFalse(any("DELETE FROM" in call.args[0] for call in cursor.execute.call_args_list))

    def test_history_failure_rolls_back_before_current_table_replacement(self):
        import contextlib

        cursor = Mock()
        cursor.execute.return_value = cursor
        cursor.fetchone.return_value = (1,)
        cursor.rowcount = 1
        connection = Mock()
        paths = {"state": "STATE", "row_state": "ROW_STATE", "batch_log": "BATCH_LOG"}
        timestamp = datetime(2024, 1, 1)
        changed = pd.DataFrame(
            {"CLAIM_ID": [2], "MODIFIED_ON": [datetime(2025, 1, 1)], "AMOUNT": [20]}
        )

        def fail_history(sql, _params=None):
            if _params is not None:
                self.assertIsInstance(_params, tuple)
            if "INSERT INTO" in sql and "CLAIMS_H" in sql:
                raise RuntimeError("history insert failed")
            return cursor

        cursor.execute.side_effect = fail_history
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(incremental, "_ensure_history_table"))
            stack.enter_context(patch.object(incremental, "_ensure_helper_tables", return_value=paths))
            stack.enter_context(patch.object(incremental, "_acquire_lock"))
            stack.enter_context(patch.object(incremental, "_renew_lock"))
            stack.enter_context(patch.object(
                incremental,
                "_read_table_state",
                return_value={
                    "strategy": "KEY_TIMESTAMP",
                    "initialized": True,
                    "watermark": timestamp,
                    "last_batch_id": "old",
                    "source_count": 1,
                },
            ))
            stack.enter_context(patch.object(incremental, "_read_row_state", return_value={"1": ("oldhash", timestamp)}))
            stack.enter_context(patch.object(incremental, "_start_batch"))
            finish = stack.enter_context(patch.object(incremental, "_finish_batch"))
            stack.enter_context(patch.object(incremental, "_release_lock"))
            stack.enter_context(patch.object(incremental, "_discover_timestamp_changes", return_value=(
                changed, [("CLAIMS", "2", "newhash", datetime(2025, 1, 1))], 2, 1, 0, 0
            )))
            stack.enter_context(patch.object(incremental, "_count_rows", return_value=3))
            stack.enter_context(patch.object(incremental, "load_dataframe_to_snowflake", return_value=1))

            with self.assertRaisesRegex(RuntimeError, "history insert failed"):
                incremental.incremental_load_table(
                    Mock(), connection, cursor, "DW_TEST", "RAW_TEST", "CLAIMS", _claim_mapping(), "run-4"
                )

        connection.rollback.assert_called_once()
        self.assertEqual(finish.call_args.args[3], "FAILED")
        self.assertFalse(
            any(
                "DELETE FROM" in call.args[0]
                and "ETL_INCREMENTAL" not in call.args[0]
                for call in cursor.execute.call_args_list
            )
        )

    def test_failed_attempt_can_retry_from_unchanged_state_without_committing_half_batch(self):
        import contextlib

        cursor = Mock()
        cursor.execute.return_value = cursor
        cursor.fetchone.return_value = (1,)
        cursor.rowcount = 1
        connection = Mock()
        paths = {"state": "STATE", "row_state": "ROW_STATE", "batch_log": "BATCH_LOG"}
        timestamp = datetime(2024, 1, 1)
        changed = pd.DataFrame(
            {"CLAIM_ID": [2], "MODIFIED_ON": [datetime(2025, 1, 1)], "AMOUNT": [20]}
        )
        delete_attempts = 0

        def fail_first_replace(sql, _params=None):
            nonlocal delete_attempts
            if _params is not None:
                self.assertIsInstance(_params, tuple)
            if "DELETE FROM" in sql and "STATE" not in sql:
                delete_attempts += 1
                if delete_attempts == 1:
                    raise RuntimeError("current replacement failed")
            return cursor

        cursor.execute.side_effect = fail_first_replace
        dependencies = contextlib.ExitStack()
        with dependencies as stack:
            stack.enter_context(patch.object(incremental, "_ensure_history_table"))
            stack.enter_context(patch.object(incremental, "_ensure_helper_tables", return_value=paths))
            stack.enter_context(patch.object(incremental, "_acquire_lock"))
            stack.enter_context(patch.object(incremental, "_renew_lock"))
            stack.enter_context(patch.object(
                incremental,
                "_read_table_state",
                return_value={
                    "strategy": "KEY_TIMESTAMP",
                    "initialized": True,
                    "watermark": timestamp,
                    "last_batch_id": "old",
                    "source_count": 1,
                },
            ))
            stack.enter_context(patch.object(incremental, "_read_row_state", return_value={"1": ("oldhash", timestamp)}))
            stack.enter_context(patch.object(incremental, "_start_batch"))
            stack.enter_context(patch.object(incremental, "_finish_batch"))
            stack.enter_context(patch.object(incremental, "_release_lock"))
            stack.enter_context(patch.object(incremental, "_discover_timestamp_changes", return_value=(
                changed, [("CLAIMS", "2", "newhash", datetime(2025, 1, 1))], 2, 1, 0, 0
            )))
            stack.enter_context(patch.object(incremental, "_count_rows", return_value=3))
            stack.enter_context(patch.object(incremental, "load_dataframe_to_snowflake", return_value=1))

            with self.assertRaisesRegex(RuntimeError, "current replacement failed"):
                incremental.incremental_load_table(
                    Mock(), connection, cursor, "DW_TEST", "RAW_TEST", "CLAIMS", _claim_mapping(), "run-5"
                )
            cursor.execute.side_effect = None
            cursor.execute.return_value = cursor
            result = incremental.incremental_load_table(
                Mock(), connection, cursor, "DW_TEST", "RAW_TEST", "CLAIMS", _claim_mapping(), "run-6"
            )

        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(delete_attempts, 1)
        self.assertGreaterEqual(connection.rollback.call_count, 1)
        sql_calls = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertEqual(sum(sql.strip() == "COMMIT" for sql in sql_calls), 1)
        history_index = max(
            index for index, sql in enumerate(sql_calls)
            if "INSERT INTO" in sql and "CLAIMS_H" in sql
        )
        current_delete_index = max(
            index for index, sql in enumerate(sql_calls)
            if "DELETE FROM" in sql and "CLAIMS\"" in sql
        )
        current_insert_index = max(
            index for index, sql in enumerate(sql_calls)
            if "INSERT INTO" in sql and "CLAIMS\"" in sql
        )
        commit_index = max(
            index for index, sql in enumerate(sql_calls) if sql.strip() == "COMMIT"
        )
        self.assertLess(history_index, current_delete_index)
        self.assertLess(current_delete_index, current_insert_index)
        self.assertLess(current_insert_index, commit_index)


if __name__ == "__main__":
    unittest.main()
