from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from etl.parquet_transform import (
    _conversion_for_column,
    _normalize_type,
    transform_parquet_raw_to_target,
)


MAPPING = [
    {
        "source_table": "User_Defined_Form_Builds",
        "source_column": "Created On",
        "source_data_type": "TIMESTAMP",
        "target_table": "USER_DEFINED_FORM_BUILDS",
        "target_column": "CREATED_ON",
        "snowflake_datatype": "TIMESTAMP_NTZ",
        "constraint": "",
    },
    {
        "source_table": "User_Defined_Form_Builds",
        "source_column": "Form ID",
        "source_data_type": "INT32",
        "target_table": "USER_DEFINED_FORM_BUILDS",
        "target_column": "FORM_ID",
        "snowflake_datatype": "NUMBER(10,0)",
        "constraint": "PK",
    },
    {
        "source_table": "",
        "source_column": "",
        "source_data_type": "",
        "target_table": "USER_DEFINED_FORM_BUILDS",
        "target_column": "CREATED_TIMESTAMP",
        "snowflake_datatype": "TIMESTAMP_NTZ",
        "constraint": "",
    },
]


class ParquetTransformTests(unittest.TestCase):
    def test_normalizes_review_comments_and_widens_clob_varchar(self):
        self.assertEqual(
            _normalize_type(
                "NUMBER(10,0) /*review: 4D time/duration value*/",
                "INT32",
            ),
            "NUMBER(10,0)",
        )
        self.assertEqual(_normalize_type("VARCHAR(250)", "CLOB"), "VARCHAR")

    def test_timestamp_conversion_tries_day_first_and_iso_formats(self):
        conversion = _conversion_for_column("CREATED_ON", "TIMESTAMP_NTZ")

        self.assertIn("TRY_TO_TIMESTAMP_NTZ", conversion.expression)
        self.assertIn("DD/MM/YYYY HH24:MI:SS.FF9", conversion.expression)
        self.assertIn('YYYY-MM-DD"T"HH24:MI:SS', conversion.expression)
        self.assertIn("IS NULL", conversion.invalid_condition)
        self.assertEqual(conversion.error_type, "INVALID_TIMESTAMP")

    def test_numeric_overflow_and_variant_parse_have_safe_try_conversions(self):
        number = _conversion_for_column("AMOUNT", "NUMBER(18,6)")
        variant = _conversion_for_column("OBJECT_DATA", "VARIANT")

        self.assertIn("TRY_TO_DECIMAL", number.expression)
        self.assertIn("TRY_PARSE_JSON", variant.expression)
        self.assertEqual(number.error_type, "INVALID_NUMERIC_VALUE")
        self.assertEqual(variant.error_type, "INVALID_VARIANT")

    def test_rejects_unrecognized_mapping_type(self):
        with self.assertRaisesRegex(ValueError, "Unsupported Snowflake datatype"):
            _conversion_for_column("VALUE", "GEOGRAPHY")

    @patch("etl.parquet_transform.release_table_run_lock")
    @patch("etl.parquet_transform.acquire_table_run_lock", return_value={"owner": "lock"})
    def test_transforms_valid_rows_and_quarantines_invalid_rows_in_one_transaction(
        self, _acquire_lock, _release_lock
    ):
        cursor = Mock()
        cursor.execute.return_value = cursor
        cursor.rowcount = 1
        cursor.fetchone.side_effect = [
            ("SUCCESS", 2, None, None, None),
            (2,),
            (1,),
            (0,),
        ]
        connection = Mock()
        connection.cursor.return_value = cursor

        result = transform_parquet_raw_to_target(
            connection,
            "DW_TEST",
            "RAW",
            MAPPING,
            "run-123",
            "forms.parquet",
        )

        self.assertEqual(result.source_rows, 2)
        self.assertEqual(result.inserted_rows, 1)
        self.assertEqual(result.rejected_rows, 1)
        self.assertFalse(result.skipped_as_duplicate)
        sql = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertTrue(any("CREATE TEMPORARY TABLE" in statement for statement in sql))
        validation_call = next(
            call
            for call in cursor.execute.call_args_list
            if call.args[0].startswith("CREATE TEMPORARY TABLE")
            and "ETL_PARQUET_VALIDATE_" in call.args[0]
        )
        self.assertIn("%s", validation_call.args[0])
        self.assertEqual(validation_call.args[1], ("run-123", "forms.parquet"))
        self.assertTrue(
            any(
                statement.startswith(
                    'SELECT COUNT(*) FROM "DW_TEST"."RAW"."USER_DEFINED_FORM_BUILDS_RAW" r WHERE r."ETL_RUN_ID"'
                )
                for statement in sql
            )
        )
        self.assertTrue(any("TRY_TO_TIMESTAMP_NTZ" in statement for statement in sql))
        self.assertTrue(any("ARRAY_CONSTRUCT_COMPACT" in statement for statement in sql))
        self.assertTrue(any("ETL_DATA_QUALITY_ERRORS" in statement for statement in sql))
        quarantine_insert = next(
            statement
            for statement in sql
            if statement.strip().startswith(
                'INSERT INTO "DW_TEST"."RAW"."ETL_DATA_QUALITY_ERRORS"'
            )
        )
        self.assertIn(
            "'Invalid value for CREATED_ON (TIMESTAMP_NTZ): ' || COALESCE",
            quarantine_insert,
        )
        self.assertNotIn("' ('TIMESTAMP_NTZ')", quarantine_insert)
        self.assertTrue(
            any(
                statement.startswith(
                    'INSERT INTO "DW_TEST"."RAW"."ETL_PARQUET_PUBLISH_'
                )
                for statement in sql
            )
        )
        self.assertTrue(
            any(
                statement.startswith("ALTER TABLE")
                and "RENAME TO" in statement
                and '"USER_DEFINED_FORM_BUILDS"' in statement
                for statement in sql
            )
        )
        self.assertTrue(any("TRANSFORM_STATUS = 'SUCCESS'" in statement for statement in sql))
        self.assertEqual(connection.commit.call_count, 2)
        connection.rollback.assert_not_called()
        self.assertTrue(any(statement.startswith("DROP TABLE IF EXISTS") for statement in sql))

    @patch("etl.parquet_transform.release_table_run_lock")
    @patch("etl.parquet_transform.acquire_table_run_lock", return_value={"owner": "lock"})
    def test_skips_file_already_transformed_successfully(
        self, _acquire_lock, release_lock
    ):
        cursor = Mock()
        cursor.execute.return_value = cursor
        cursor.fetchone.return_value = ("SUCCESS", 2, "SUCCESS", 1, 1)
        connection = Mock()
        connection.cursor.return_value = cursor

        result = transform_parquet_raw_to_target(
            connection,
            "DW_TEST",
            "RAW",
            MAPPING,
            "run-123",
            "forms.parquet",
        )

        self.assertTrue(result.skipped_as_duplicate)
        self.assertEqual(result.inserted_rows, 1)
        sql = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertFalse(any("INSERT INTO" in statement for statement in sql))
        self.assertEqual(len(sql), 5)
        release_lock.assert_called_once()

    @patch("etl.parquet_transform.release_table_run_lock")
    @patch("etl.parquet_transform.acquire_table_run_lock", return_value={"owner": "lock"})
    def test_replaces_existing_target_and_preserves_old_snapshot(
        self, _acquire_lock, _release_lock
    ):
        cursor = Mock()
        cursor.execute.return_value = cursor
        cursor.rowcount = 1
        cursor.fetchone.side_effect = [
            ("SUCCESS", 2, None, None, None),
            (2,),
            (1,),
            (1,),
        ]
        connection = Mock()
        connection.cursor.return_value = cursor

        result = transform_parquet_raw_to_target(
            connection,
            "DW_TEST",
            "RAW",
            MAPPING,
            "run-123",
            "forms.parquet",
        )

        self.assertEqual(result.inserted_rows, 1)
        sql = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertTrue(
            any(
                "SWAP WITH" in statement
                and '"USER_DEFINED_FORM_BUILDS"' in statement
                for statement in sql
            )
        )
        self.assertTrue(any("_HIST_" in statement for statement in sql))


if __name__ == "__main__":
    unittest.main()
