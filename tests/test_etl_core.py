import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from config.logging_config import configure_logging
from etl.audit import SnowflakeAuditLogger
from etl.ddl import build_create_table_sql
from etl.metadata import table_metadata_from_dataframe
from config.settings import Settings
from connections.fourd_connection import connect_to_4d
from etl.load import build_source_query
from scripts import run_initial_etl
from scripts.run_initial_etl import _parse_args


class EtlCoreTests(unittest.TestCase):
    def test_snowflake_audit_logger_creates_table_and_records_events(self):
        connection = Mock()
        cursor = Mock()
        connection.cursor.return_value = cursor
        audit = SnowflakeAuditLogger(connection, "DW_TEST", "UW_RAW_STG")

        audit.ensure_table()
        audit.record("INSERT_TARGET_DATA", "SUCCESS", "REPORTS", "Loaded rows")

        create_sql = cursor.execute.call_args_list[0].args[0]
        insert_sql, insert_values = cursor.execute.call_args_list[1].args
        self.assertIn('"DW_TEST"."UW_RAW_STG"."ETL_PIPELINE_LOG"', create_sql)
        self.assertIn("AUTOINCREMENT", create_sql)
        self.assertIn("ERROR_MESSAGE VARCHAR", create_sql)
        self.assertIn("TIME_STAMP TIMESTAMP_NTZ", create_sql)
        self.assertIn("(RUN_ID, TABLE_NAME, STEP, STATUS, MESSAGE, ERROR_MESSAGE)", insert_sql)
        self.assertEqual(insert_values[1:], ("REPORTS", "INSERT_TARGET_DATA", "SUCCESS", "Loaded rows", None))
        self.assertTrue(cursor.close.call_count == 2)

    def test_snowflake_audit_logger_records_failure_traceback(self):
        connection = Mock()
        cursor = Mock()
        connection.cursor.return_value = cursor
        audit = SnowflakeAuditLogger(connection, "DW_TEST", "UW_RAW_STG")

        with self.assertRaisesRegex(RuntimeError, "test step failure"):
            audit.run_step(
                "EXTRACT_SOURCE_DATA",
                lambda: (_ for _ in ()).throw(RuntimeError("test step failure")),
                "REPORTS",
            )

        failure_values = cursor.execute.call_args_list[-1].args[1]
        self.assertEqual(failure_values[2:4], ("EXTRACT_SOURCE_DATA", "FAILURE"))
        self.assertIn("test step failure", failure_values[4])
        self.assertIn("RuntimeError", failure_values[5])
        self.assertIn("Traceback (most recent call last)", failure_values[5])

    def test_snowflake_audit_logger_records_step_start_and_success(self):
        connection = Mock()
        cursor = Mock()
        connection.cursor.return_value = cursor
        audit = SnowflakeAuditLogger(connection, "DW_TEST", "UW_RAW_STG")

        result = audit.run_step("LOAD_MAPPING", lambda: "loaded", "REPORTS")

        self.assertEqual(result, "loaded")
        started_values = cursor.execute.call_args_list[0].args[1]
        success_values = cursor.execute.call_args_list[1].args[1]
        self.assertEqual(started_values[2:4], ("LOAD_MAPPING", "STARTED"))
        self.assertEqual(success_values[2:4], ("LOAD_MAPPING", "SUCCESS"))

    def test_pipeline_logging_writes_full_exception_to_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "etl_pipeline.log"
            root_logger = logging.getLogger()
            try:
                configure_logging(log_path)
                try:
                    raise RuntimeError("test pipeline failure detail")
                except RuntimeError:
                    logging.getLogger("test_pipeline").exception("Pipeline failed")

                log_contents = log_path.read_text(encoding="utf-8")
                self.assertIn("test pipeline failure detail", log_contents)
                self.assertIn("RuntimeError", log_contents)
                self.assertIn("Traceback (most recent call last)", log_contents)
            finally:
                for handler in list(root_logger.handlers):
                    if getattr(handler, "_etl_pipeline_logging_handler", False):
                        root_logger.removeHandler(handler)
                        handler.close()

    def test_cli_accepts_unquoted_multiword_table_name(self):
        args = _parse_args(["--table", "Accounts", "Aging", "Item"])

        self.assertEqual(" ".join(args.table), "Accounts Aging Item")

    @patch("scripts.run_initial_etl.run_for_table", return_value={"tables": []})
    @patch("scripts.run_initial_etl.pd.ExcelFile")
    @patch(
        "scripts.run_initial_etl._parse_args",
        return_value=_parse_args(["--table", "ACCOUNT_CONTACTS"]),
    )
    def test_cli_resolves_selected_table_to_matching_mapping_sheet(
        self, parse_args, excel_file, run_for_table
    ):
        excel_file.return_value.sheet_names = ["Snowflake_table_names", "Account Contacts"]

        run_initial_etl.main()

        run_for_table.assert_called_once_with(
            str(run_initial_etl.DEFAULT_EXCEL_PATH),
            "Account Contacts",
            "ACCOUNT_CONTACTS",
        )

    @patch("connections.fourd_connection.pyodbc.connect")
    @patch("connections.fourd_connection.settings")
    def test_connect_to_4d_does_not_execute_probe_query(self, settings, connect):
        settings.missing_4d_settings.return_value = []
        settings.fourd_connection_string.return_value = "DRIVER=test;"
        settings.fourd_server = "server"
        settings.fourd_port = "19813"
        settings.fourd_query_timeout_seconds = 1800
        connection = Mock()
        connect.return_value = connection

        result = connect_to_4d()

        self.assertIs(result, connection)
        connect.assert_called_once_with("DRIVER=test;", timeout=30)
        self.assertEqual(connection.timeout, 1800)
        connection.cursor.assert_not_called()

    def test_4d_connection_string_does_not_require_database(self):
        settings = Settings()
        settings.fourd_server = "server"
        settings.fourd_port = "19813"
        settings.fourd_username = "username"
        settings.fourd_password = "test_password"
        settings.fourd_database = ""

        connection_string = settings.fourd_connection_string()

        self.assertEqual(settings.missing_4d_settings(), [])
        self.assertIn("UID=username;", connection_string)
        self.assertIn("PWD=test_password;", connection_string)
        self.assertNotIn("DATABASE=", connection_string)

    def test_table_metadata_from_dataframe_builds_grouped_rows(self):
        df = pd.DataFrame([
            {
                "Source table": "Acc_Cheque_Payments",
                "Source column": "Cheque_ID_NO",
                "Target table": "ACC_CHEQUE_PAYMENTS",
                "Target column": "CHEQUE_ID_NO",
                "Constraints": "PK",
                "Snowflake Datatype": "NUMBER(18,0)",
            },
            {
                "Source table": "Acc_Cheque_Payments",
                "Source column": "Amount",
                "Target table": "ACC_CHEQUE_PAYMENTS",
                "Target column": "AMOUNT",
                "Constraints": "",
                "Snowflake Datatype": "NUMBER(18,2)",
            },
        ])

        metadata = table_metadata_from_dataframe(df)

        self.assertIn("ACC_CHEQUE_PAYMENTS", metadata)
        self.assertEqual(len(metadata["ACC_CHEQUE_PAYMENTS"]), 2)
        self.assertEqual(metadata["ACC_CHEQUE_PAYMENTS"][0]["target_column"], "CHEQUE_ID_NO")

    def test_table_metadata_skips_columns_marked_not_in_snowflake(self):
        df = pd.DataFrame([
            {
                "Source table": "Reports",
                "Source column": "Report Name",
                "Target table": "REPORTS",
                "Target column": "REPORT_NAME",
                "Snowflake Datatype": "VARCHAR(250)",
            },
            {
                "Source table": "Reports",
                "Source column": "QuickReport_",
                "Target table": "REPORTS",
                "Target column": "not in snowflake",
                "Snowflake Datatype": "BINARY",
            },
        ])

        metadata = table_metadata_from_dataframe(df)

        self.assertEqual(
            [row["target_column"] for row in metadata["REPORTS"]],
            ["REPORT_NAME"],
        )

    def test_build_source_query_quotes_identifiers_and_skips_blank_columns(self):
        query = build_source_query(
            "Acc_Cheque_Payments",
            ["Cheque_ID_No", "Cheque Date", "", "Name ] With Bracket"],
        )

        self.assertEqual(
            query,
            "SELECT [Cheque_ID_No], [Cheque Date], [Name ]] With Bracket] "
            "FROM [Acc_Cheque_Payments]",
        )

    def test_build_create_table_sql_includes_primary_key(self):
        rows = [
            {
                "source_table": "Acc_Cheque_Payments",
                "source_column": "Cheque_ID_NO",
                "target_table": "ACC_CHEQUE_PAYMENTS",
                "target_column": "CHEQUE_ID_NO",
                "snowflake_datatype": "NUMBER(18,0)",
                "constraint": "PK",
            },
            {
                "source_table": "Acc_Cheque_Payments",
                "source_column": "Amount",
                "target_table": "ACC_CHEQUE_PAYMENTS",
                "target_column": "AMOUNT",
                "snowflake_datatype": "NUMBER(18,2)",
                "constraint": "",
            },
        ]

        ddl = build_create_table_sql("ACC_CHEQUE_PAYMENTS", rows, schema_name="UW_RAW_STG")

        self.assertIn("CREATE TABLE IF NOT EXISTS UW_RAW_STG.ACC_CHEQUE_PAYMENTS", ddl)
        self.assertIn("CHEQUE_ID_NO NUMBER(18,0) PRIMARY KEY", ddl)
        self.assertIn("AMOUNT NUMBER(18,2)", ddl)


if __name__ == "__main__":
    unittest.main()
