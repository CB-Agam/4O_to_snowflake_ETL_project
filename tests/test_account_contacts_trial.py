from __future__ import annotations

import unittest
from datetime import datetime

import pandas as pd

from scripts.run_account_contacts_trial import (
    _prepare_trial_objects,
    _validated_frame,
    prepare_trial_batches,
)


class RecordingCursor:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, statement: str):
        self.statements.append(statement)
        return self


class AccountContactsTrialTests(unittest.TestCase):
    def _sample(self) -> pd.DataFrame:
        rows = [
            {
                "CONTACT_ID": str(contact_id),
                "MODIFIED_ON": f"2026-05-{min(day, 14):02d} 00:00:00.000",
                "FULL_NAME": f"Sample Contact {contact_id}",
            }
            for contact_id, day in enumerate(range(1, 24), start=1)
        ]
        rows.append(
            {
                "CONTACT_ID": "64",
                "MODIFIED_ON": "2026-05-15 11:14:00.000",
                "FULL_NAME": "Latest Sample Contact",
            }
        )
        return pd.DataFrame(rows)

    def test_day_zero_gets_23_older_rows_and_day_one_gets_latest_row(self):
        day_zero, day_one = prepare_trial_batches(
            self._sample(),
            today=datetime(2026, 10, 16, 17, 0),
        )

        self.assertEqual(len(day_zero), 23)
        self.assertEqual(len(day_one), 1)
        self.assertNotIn("64", day_zero["CONTACT_ID"].tolist())
        self.assertEqual(day_one.iloc[0]["CONTACT_ID"], "64")
        self.assertEqual(day_one.iloc[0]["MODIFIED_ON"], "2026-10-16 17:00:00.000000")

    def test_fixture_with_multiple_latest_rows_fails_closed(self):
        sample = self._sample()
        sample.loc[0, "MODIFIED_ON"] = "2026-05-15 11:14:00.000"

        with self.assertRaisesRegex(ValueError, "exactly one row"):
            prepare_trial_batches(sample)

    def test_fixture_with_duplicate_contact_id_fails_closed(self):
        sample = self._sample()
        sample.loc[0, "CONTACT_ID"] = "64"

        with self.assertRaisesRegex(ValueError, "not unique"):
            prepare_trial_batches(sample)

    def test_fixture_must_have_expected_row_count_and_split_columns(self):
        with self.assertRaisesRegex(ValueError, "Expected exactly 24"):
            prepare_trial_batches(self._sample().iloc[:-1])
        with self.assertRaisesRegex(ValueError, "missing required columns"):
            prepare_trial_batches(
                pd.DataFrame({"CONTACT_ID": [str(index) for index in range(24)]})
            )

    def test_trial_objects_use_mapping_schema_without_live_target_table(self):
        cursor = RecordingCursor()
        mapping_rows = [
            {
                "target_column": "CONTACT_ID",
                "snowflake_datatype": "NUMBER(10,0)",
                "constraint": "PK",
            },
            {
                "target_column": "CREATED_TIMESTAMP",
                "snowflake_datatype": "TIMESTAMP_NTZ",
                "constraint": "",
            },
        ]

        objects = _prepare_trial_objects(
            cursor,
            "DEV_DB",
            "DEV_SCHEMA",
            mapping_rows,
        )

        self.assertIn("ACCOUNT_CONTACTS_TRIAL", objects["trial"])
        create_trial_sql = cursor.statements[0]
        self.assertIn('"DEV_DB"."DEV_SCHEMA"."ACCOUNT_CONTACTS_TRIAL"', create_trial_sql)
        self.assertIn("CONTACT_ID NUMBER(10,0) PRIMARY KEY", create_trial_sql)
        self.assertIn("CREATED_TIMESTAMP TIMESTAMP_NTZ", create_trial_sql)
        self.assertNotIn("LIKE", create_trial_sql)
        self.assertFalse(any("ACCOUNT_CONTACTS\"" in sql for sql in cursor.statements))

    def test_trial_object_setup_requires_target_column_mappings(self):
        with self.assertRaisesRegex(ValueError, "No target column mappings"):
            _prepare_trial_objects(
                RecordingCursor(),
                "DEV_DB",
                "DEV_SCHEMA",
                [{"target_column": ""}],
            )

    def test_blank_typed_values_become_null_but_blank_text_stays_empty(self):
        frame = pd.DataFrame(
            {
                "CREATED_ON": [""],
                "CONTACT_ID": ["5"],
                "FULL_NAME": [""],
            }
        )
        mapping_rows = [
            {"target_column": "CREATED_ON", "snowflake_datatype": "TIMESTAMP_NTZ"},
            {"target_column": "CONTACT_ID", "snowflake_datatype": "NUMBER(10,0)"},
            {"target_column": "FULL_NAME", "snowflake_datatype": "VARCHAR(250)"},
        ]

        columns, prepared = _validated_frame(
            frame,
            ["CREATED_ON", "CONTACT_ID", "FULL_NAME"],
            mapping_rows,
        )

        self.assertEqual(columns, ["CREATED_ON", "CONTACT_ID", "FULL_NAME"])
        self.assertIsNone(prepared.loc[0, "CREATED_ON"])
        self.assertEqual(prepared.loc[0, "CONTACT_ID"], "5")
        self.assertEqual(prepared.loc[0, "FULL_NAME"], "")


if __name__ == "__main__":
    unittest.main()
