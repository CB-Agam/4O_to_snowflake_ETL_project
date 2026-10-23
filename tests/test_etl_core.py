import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from config.logging_config import configure_logging
from etl.audit import SnowflakeAuditLogger
from etl.ddl import (
    build_create_table_sql,
    build_drop_status_column_sql,
    build_timestamp_column_sql,
    build_widen_clob_columns_sql,
)
from etl.metadata import table_metadata_from_dataframe
from config.settings import Settings
from connections.fourd_connection import connect_to_4d
from etl.load import build_insert_sql, build_source_query, load_dataframe_to_snowflake
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
        insert_call = cursor.execute.call_args_list[5]
        insert_sql, insert_values = insert_call.args
        self.assertIn('"DW_TEST"."UW_RAW_STG"."ETL_PIPELINE_LOG"', create_sql)
        self.assertNotIn("S_NO", create_sql)
        self.assertIn("RUN_STEP_NO NUMBER", create_sql)
        self.assertIn("ERROR_MESSAGE VARCHAR", create_sql)
        self.assertIn("START_TIME TIMESTAMP_NTZ", create_sql)
        self.assertIn("END_TIME TIMESTAMP_NTZ", create_sql)
        self.assertIn("TIME_STAMP TIMESTAMP_NTZ", create_sql)
        self.assertIn("DROP COLUMN IF EXISTS S_NO", cursor.execute.call_args_list[2].args[0])
        self.assertIn(
            "(RUN_ID, RUN_STEP_NO, TABLE_NAME, STEP, STATUS, MESSAGE,",
            insert_sql,
        )
        self.assertEqual(
            insert_values[:7],
            (audit.run_id, 1, "REPORTS", "INSERT_TARGET_DATA", "SUCCESS", "Loaded rows", None),
        )
        self.assertEqual(insert_values[8], "SUCCESS")
        self.assertEqual(cursor.close.call_count, 2)

    def test_each_audit_run_has_new_run_id_and_step_numbers_restart(self):
        connection = Mock()
        connection.cursor.return_value = Mock()
        first_run = SnowflakeAuditLogger(connection, "DW_TEST", "UW_RAW_STG")
        second_run = SnowflakeAuditLogger(connection, "DW_TEST", "UW_RAW_STG")

        first_run.record("STEP_ONE", "SUCCESS")
        first_run.record("STEP_TWO", "SUCCESS")
        second_run.record("STEP_ONE", "SUCCESS")

        self.assertNotEqual(first_run.run_id, second_run.run_id)
        values = [
            call.args[1]
            for call in connection.cursor.return_value.execute.call_args_list
        ]
        self.assertEqual([entry[1] for entry in values], [1, 2, 1])
        self.assertEqual(
            [entry[0] for entry in values],
            [first_run.run_id, first_run.run_id, second_run.run_id],
        )

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
        self.assertEqual(failure_values[:2], ("FAILED", "EXTRACT_SOURCE_DATA failed: test step failure"))
        self.assertIn("RuntimeError", failure_values[2])
        self.assertIn("Traceback (most recent call last)", failure_values[2])
        self.assertEqual(failure_values[3:], (audit.run_id, 1))

    def test_snowflake_audit_logger_records_step_start_and_success(self):
        connection = Mock()
        cursor = Mock()
        connection.cursor.return_value = cursor
        audit = SnowflakeAuditLogger(connection, "DW_TEST", "UW_RAW_STG")

        result = audit.run_step("LOAD_MAPPING", lambda: "loaded", "REPORTS")

        self.assertEqual(result, "loaded")
        started_values = cursor.execute.call_args_list[0].args[1]
        success_values = cursor.execute.call_args_list[1].args[1]
        self.assertEqual(started_values[3:5], ("LOAD_MAPPING", "RUNNING"))
        self.assertEqual(success_values[:2], ("SUCCESS", "LOAD_MAPPING completed successfully"))
        self.assertEqual(success_values[3:], (audit.run_id, 1))

    def test_snowflake_audit_logger_marks_interrupted_steps(self):
        connection = Mock()
        cursor = Mock()
        connection.cursor.return_value = cursor
        audit = SnowflakeAuditLogger(connection, "DW_TEST", "UW_RAW_STG")

        with self.assertRaises(KeyboardInterrupt):
            audit.run_step(
                "EXTRACT_SOURCE_DATA",
                lambda: (_ for _ in ()).throw(KeyboardInterrupt()),
            )

        interrupted_values = cursor.execute.call_args_list[-1].args[1]
        self.assertEqual(interrupted_values[:2], ("INTERRUPTED", "EXTRACT_SOURCE_DATA interrupted"))
        self.assertEqual(interrupted_values[3:], (audit.run_id, 1))

    @patch("scripts.run_initial_etl.load_dataframe_to_snowflake", return_value=1)
    @patch("scripts.run_initial_etl.pd.read_sql", return_value=pd.DataFrame({"SOURCE_COLUMN": [1]}))
    @patch("scripts.run_initial_etl.table_exists_in_snowflake", return_value=True)
    @patch("scripts.run_initial_etl.connect_to_4d")
    @patch(
        "scripts.run_initial_etl.load_metadata_from_excel",
        return_value={
            "REPORTS": [{
                "source_table": "Reports",
                "source_column": "Source Column",
                "target_column": "SOURCE_COLUMN",
            }]
        },
    )
    @patch("scripts.run_initial_etl.SnowflakeAuditLogger")
    @patch("scripts.run_initial_etl.connect_to_snowflake")
    def test_run_for_table_records_pipeline_start_and_final_success(
        self,
        connect_snowflake,
        audit_logger,
        load_metadata,
        connect_4d,
        table_exists,
        read_sql,
        load_dataframe,
    ):
        snowflake_connection = Mock()
        snowflake_cursor = Mock()
        snowflake_connection.cursor.return_value = snowflake_cursor
        connect_snowflake.return_value = snowflake_connection
        fourd_connection = Mock()
        connect_4d.return_value = fourd_connection
        audit = Mock()
        audit.run_id = "test-run-id"
        audit.record.return_value = 1
        audit.run_step.side_effect = lambda step, action, table_name=None: action()
        audit_logger.return_value = audit

        with (
            patch("scripts.run_initial_etl.acquire_table_run_lock", return_value={}),
            patch("scripts.run_initial_etl.renew_table_run_lock"),
            patch("scripts.run_initial_etl.release_table_run_lock"),
        ):
            result = run_initial_etl.run_for_table("mapping.xlsx", "Reports", "REPORTS")

        self.assertEqual(result["tables"][0]["rows_loaded"], 1)
        records = [call.args for call in audit.record.call_args_list]
        pipeline_records = [record for record in records if record[0] == "ETL_PIPELINE"]
        self.assertEqual([record[1] for record in pipeline_records], ["RUNNING"])
        self.assertIn("test-run-id", pipeline_records[0][3])
        audit.finish_step.assert_called_once()
        finish_values = audit.finish_step.call_args.args
        self.assertEqual(finish_values[:2], (1, "SUCCESS"))
        self.assertIn("Loaded 1 rows into REPORTS", finish_values[2])
        fourd_connection.close.assert_called_once()
        snowflake_cursor.close.assert_called_once()
        snowflake_connection.close.assert_called_once()

    @patch(
        "scripts.run_initial_etl.load_metadata_from_excel",
        side_effect=RuntimeError("mapping failed"),
    )
    @patch("scripts.run_initial_etl.SnowflakeAuditLogger")
    @patch("scripts.run_initial_etl.connect_to_snowflake")
    def test_run_for_table_marks_pipeline_failed(
        self,
        connect_snowflake,
        audit_logger,
        load_metadata,
    ):
        snowflake_connection = Mock()
        connect_snowflake.return_value = snowflake_connection
        audit = Mock()
        audit.run_id = "test-run-id"
        audit.record.return_value = 1
        audit.run_step.side_effect = lambda _step, action, _table_name=None: action()
        audit_logger.return_value = audit

        with self.assertRaisesRegex(RuntimeError, "mapping failed"):
            run_initial_etl.run_for_table("mapping.xlsx", "Reports", "REPORTS")

        self.assertEqual(audit.finish_step.call_args.args[:2], (1, "FAILED"))
        snowflake_connection.close.assert_called_once()

    @patch(
        "scripts.run_initial_etl.load_metadata_from_excel",
        side_effect=KeyboardInterrupt(),
    )
    @patch("scripts.run_initial_etl.SnowflakeAuditLogger")
    @patch("scripts.run_initial_etl.connect_to_snowflake")
    def test_run_for_table_marks_pipeline_interrupted(
        self,
        connect_snowflake,
        audit_logger,
        load_metadata,
    ):
        snowflake_connection = Mock()
        connect_snowflake.return_value = snowflake_connection
        audit = Mock()
        audit.run_id = "test-run-id"
        audit.record.return_value = 1
        audit.run_step.side_effect = lambda _step, action, _table_name=None: action()
        audit_logger.return_value = audit

        with self.assertRaises(KeyboardInterrupt):
            run_initial_etl.run_for_table("mapping.xlsx", "Reports", "REPORTS")

        self.assertEqual(audit.finish_step.call_args.args[:2], (1, "INTERRUPTED"))
        snowflake_connection.close.assert_called_once()

    @patch("scripts.run_initial_etl.load_dataframe_to_snowflake", return_value=1)
    @patch("scripts.run_initial_etl.pd.read_sql", return_value=pd.DataFrame({"SOURCE_COLUMN": [1]}))
    @patch("scripts.run_initial_etl.table_exists_in_snowflake", return_value=True)
    @patch("scripts.run_initial_etl.connect_to_4d")
    @patch("scripts.run_initial_etl.load_metadata_from_excel")
    @patch("scripts.run_initial_etl.SnowflakeAuditLogger")
    @patch("scripts.run_initial_etl.connect_to_snowflake")
    def test_run_for_table_processes_multiple_tables_in_one_run(
        self,
        connect_snowflake,
        audit_logger,
        load_metadata,
        connect_4d,
        table_exists,
        read_sql,
        load_dataframe,
    ):
        targets_by_sheet = {
            "Accounts Aging Item": "ACCOUNTS_AGING_ITEM",
            "Reports": "REPORTS",
            "Account Contacts": "ACCOUNT_CONTACTS",
        }
        load_metadata.side_effect = lambda _path, sheet_name: {
            targets_by_sheet[sheet_name]: [{
                "source_table": sheet_name,
                "source_column": "Source Column",
                "target_column": "SOURCE_COLUMN",
            }]
        }
        snowflake_connection = Mock()
        snowflake_cursor = Mock()
        snowflake_connection.cursor.return_value = snowflake_cursor
        connect_snowflake.return_value = snowflake_connection
        fourd_connection = Mock()
        connect_4d.return_value = fourd_connection
        audit = Mock()
        audit.run_id = "test-run-id"
        audit.run_step.side_effect = lambda _step, action, _table_name=None: action()
        audit_logger.return_value = audit

        with (
            patch("scripts.run_initial_etl.acquire_table_run_lock", return_value={}),
            patch("scripts.run_initial_etl.renew_table_run_lock"),
            patch("scripts.run_initial_etl.release_table_run_lock"),
        ):
            result = run_initial_etl.run_for_table(
                "mapping.xlsx",
                {
                    "ACCOUNTS_AGING_ITEM": "Accounts Aging Item",
                    "REPORTS": "Reports",
                    "ACCOUNT_CONTACTS": "Account Contacts",
                },
                ["ACCOUNTS_AGING_ITEM", "Reports", "Account_Contacts"],
            )

        self.assertEqual(
            [item["table"] for item in result["tables"]],
            ["ACCOUNTS_AGING_ITEM", "REPORTS", "ACCOUNT_CONTACTS"],
        )
        self.assertEqual(load_metadata.call_count, 3)
        self.assertEqual(read_sql.call_count, 3)
        self.assertEqual(load_dataframe.call_count, 3)
        status_drops = [
            call.args[0]
            for call in snowflake_cursor.execute.call_args_list
            if "DROP COLUMN IF EXISTS STATUS" in call.args[0]
        ]
        self.assertEqual(len(status_drops), 3)
        fourd_connection.close.assert_called_once()
        snowflake_cursor.close.assert_called_once()
        snowflake_connection.close.assert_called_once()

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

    def test_cli_accepts_multiple_target_tables(self):
        args = _parse_args([
            "--tables",
            "ACCOUNTS_AGING_ITEM",
            "Reports",
            "Account_Contacts",
        ])

        self.assertEqual(
            args.tables,
            ["ACCOUNTS_AGING_ITEM", "Reports", "Account_Contacts"],
        )

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

    @patch("scripts.run_initial_etl.run_for_table", return_value={"tables": []})
    @patch("scripts.run_initial_etl.pd.ExcelFile")
    @patch(
        "scripts.run_initial_etl._parse_args",
        return_value=_parse_args([
            "--tables",
            "ACCOUNTS_AGING_ITEM",
            "Reports",
            "Account_Contacts",
        ]),
    )
    def test_cli_resolves_multiple_tables_to_mapping_sheets(
        self, parse_args, excel_file, run_for_table
    ):
        excel_file.return_value.sheet_names = [
            "Snowflake_table_names",
            "Accounts Aging Item",
            "Reports",
            "Account Contacts",
        ]

        run_initial_etl.main()

        run_for_table.assert_called_once_with(
            str(run_initial_etl.DEFAULT_EXCEL_PATH),
            {
                "ACCOUNTS_AGING_ITEM": "Accounts Aging Item",
                "REPORTS": "Reports",
                "ACCOUNT_CONTACTS": "Account Contacts",
            },
            ["ACCOUNTS_AGING_ITEM", "Reports", "Account_Contacts"],
        )

    def test_drop_status_column_sql_targets_selected_table(self):
        self.assertEqual(
            build_drop_status_column_sql("REPORTS", schema_name="UW_RAW_STG"),
            "ALTER TABLE UW_RAW_STG.REPORTS DROP COLUMN IF EXISTS STATUS",
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
                "4D DataType": "INT32",
                "Target table": "ACC_CHEQUE_PAYMENTS",
                "Target column": "CHEQUE_ID_NO",
                "Constraints": "PK",
                "Snowflake Datatype": "NUMBER(18,0)",
            },
            {
                "Source table": "Acc_Cheque_Payments",
                "Source column": "Amount",
                "4D DataType": "REAL",
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
        self.assertEqual(metadata["ACC_CHEQUE_PAYMENTS"][0]["source_data_type"], "INT32")

    def test_table_metadata_accepts_existing_workbook_target_table_header_typo(self):
        df = pd.DataFrame([
            {
                "Source table": "Vehicle Table",
                "Source column": "Make",
                "Traget table": "VEHICLE_TABLE",
                "Target Column": "MAKE",
                "4D Data Type": "CLOB",
                "Snowflake Data Type": "VARCHAR(250)",
            },
        ])

        metadata = table_metadata_from_dataframe(df)

        self.assertEqual(metadata["VEHICLE_TABLE"][0]["source_table"], "Vehicle Table")
        self.assertEqual(metadata["VEHICLE_TABLE"][0]["target_column"], "MAKE")

    def test_clob_columns_use_unbounded_varchar_for_create_and_existing_tables(self):
        rows = [
            {
                "source_data_type": "CLOB",
                "target_column": "DESCRIPTION",
                "snowflake_datatype": "VARCHAR(250)",
                "constraint": "",
            },
            {
                "source_data_type": "CLOB",
                "target_column": "REPORT_NAME",
                "snowflake_datatype": "VARCHAR",
                "constraint": "",
            },
            {
                "source_data_type": "UUID",
                "target_column": "REPLICATOR_KEY",
                "snowflake_datatype": "VARCHAR(36)",
                "constraint": "",
            },
        ]

        create_sql = build_create_table_sql("REPORTS", rows, schema_name="UW_RAW_STG")
        alter_sql = build_widen_clob_columns_sql("REPORTS", rows, schema_name="UW_RAW_STG")

        self.assertIn("DESCRIPTION VARCHAR", create_sql)
        self.assertNotIn("DESCRIPTION VARCHAR(250)", create_sql)
        self.assertEqual(
            alter_sql,
            [
                "ALTER TABLE UW_RAW_STG.REPORTS "
                "ALTER COLUMN DESCRIPTION SET DATA TYPE VARCHAR"
            ],
        )

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

    def test_metadata_omits_status_but_keeps_managed_timestamps(self):
        df = pd.DataFrame([
            {
                "Source table": "Reports",
                "Source column": "Report Name",
                "Target table": "REPORTS",
                "Target column": "REPORT_NAME",
                "Snowflake Datatype": "VARCHAR(250)",
            },
            {
                "Source table": "",
                "Source column": "",
                "Target table": "REPORTS",
                "Target column": "Status",
                "Snowflake Datatype": "VARCHAR(20)",
            },
            {
                "Source table": "",
                "Source column": "",
                "Target table": "REPORTS",
                "Target column": "CREATED_TIMESTAMP",
                "Snowflake Datatype": "TIMESTAMP_NTZ",
            },
            {
                "Source table": "",
                "Source column": "",
                "Target table": "REPORTS",
                "Target column": "UPDATED_TIMESTAMP",
                "Snowflake Datatype": "TIMESTAMP_NTZ",
            },
        ])

        rows = table_metadata_from_dataframe(df)["REPORTS"]

        self.assertEqual(
            [row["target_column"] for row in rows],
            ["REPORT_NAME", "CREATED_TIMESTAMP", "UPDATED_TIMESTAMP"],
        )

    def test_timestamp_columns_are_defaulted_and_existing_nulls_are_backfilled(self):
        rows = [
            {
                "target_column": "CREATED_TIMESTAMP",
                "snowflake_datatype": "TIMESTAMP_NTZ",
                "constraint": "",
            },
            {
                "target_column": "UPDATED_TIMESTAMP",
                "snowflake_datatype": "TIMESTAMP_NTZ",
                "constraint": "",
            },
        ]

        create_sql = build_create_table_sql("REPORTS", rows, schema_name="UW_RAW_STG")
        repair_sql = build_timestamp_column_sql("REPORTS", rows, schema_name="UW_RAW_STG")

        self.assertIn(
            "CREATED_TIMESTAMP TIMESTAMP_NTZ "
            "DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ NOT NULL",
            create_sql,
        )
        self.assertIn(
            "UPDATED_TIMESTAMP TIMESTAMP_NTZ "
            "DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ NOT NULL",
            create_sql,
        )
        self.assertEqual(len(repair_sql), 2)
        self.assertIn(
            "UPDATE UW_RAW_STG.REPORTS SET CREATED_TIMESTAMP = "
            "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ WHERE CREATED_TIMESTAMP IS NULL",
            repair_sql,
        )
        self.assertTrue(all("ALTER COLUMN" not in statement for statement in repair_sql))

    def test_create_table_uses_timestamp_type_for_managed_columns_regardless_of_mapping_type(self):
        rows = [
            {
                "target_column": "CREATED_TIMESTAMP",
                "snowflake_datatype": "VARCHAR",
                "constraint": "",
            },
            {
                "target_column": "UPDATED_TIMESTAMP",
                "snowflake_datatype": "VARCHAR",
                "constraint": "",
            },
        ]

        create_sql = build_create_table_sql("VEHICLE_TABLE", rows, schema_name="UW_RAW_STG")

        self.assertIn(
            "CREATED_TIMESTAMP TIMESTAMP_NTZ "
            "DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ NOT NULL",
            create_sql,
        )
        self.assertIn(
            "UPDATED_TIMESTAMP TIMESTAMP_NTZ "
            "DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ NOT NULL",
            create_sql,
        )

    def test_insert_sets_both_managed_timestamps_in_snowflake(self):
        sql = build_insert_sql("REPORTS", ["REPORT_NAME"], schema_name="UW_RAW_STG")

        self.assertEqual(
            sql,
            "INSERT INTO \"UW_RAW_STG\".\"REPORTS\" "
            "(\"REPORT_NAME\", \"CREATED_TIMESTAMP\", \"UPDATED_TIMESTAMP\") "
            "VALUES (%s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, "
            "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)",
        )

    def test_reserved_words_in_metadata_are_quoted(self):
        sql = build_create_table_sql(
            "MESSAGES",
            [
                {"target_column": "WHERE", "snowflake_datatype": "VARCHAR", "constraint": ""},
                {"target_column": "CREATED_TIMESTAMP", "snowflake_datatype": "TIMESTAMP", "constraint": ""},
            ],
            schema_name="UW_RAW_STG",
        )

        self.assertIn('"WHERE" VARCHAR', sql)
        self.assertIn('"CREATED_TIMESTAMP" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ NOT NULL', sql)

    def test_load_dataframe_populates_timestamps_without_python_values(self):
        cursor = Mock()
        dataframe = pd.DataFrame(
            {
                "REPORT_NAME": ["Example"],
                "CREATED_TIMESTAMP": [None],
                "UPDATED_TIMESTAMP": [None],
            }
        )

        inserted = load_dataframe_to_snowflake(
            cursor,
            "REPORTS",
            ["REPORT_NAME", "CREATED_TIMESTAMP", "UPDATED_TIMESTAMP"],
            dataframe,
            schema_name="UW_RAW_STG",
        )

        self.assertEqual(inserted, 1)
        sql, rows = cursor.executemany.call_args.args
        self.assertIn("CREATED_TIMESTAMP, UPDATED_TIMESTAMP", sql)
        self.assertIn(
            "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, "
            "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ",
            sql,
        )
        self.assertEqual(rows, [("Example",)])

    def test_load_dataframe_splits_large_inserts_to_avoid_snowflake_expression_limit(self):
        cursor = Mock()
        columns = [f"COL_{idx}" for idx in range(30)]
        dataframe = pd.DataFrame(
            {
                column: [f"value-{idx}-{col}" for idx in range(7000)]
                for col, column in enumerate(columns)
            }
        )

        inserted = load_dataframe_to_snowflake(
            cursor,
            "ALERTS",
            columns,
            dataframe,
            schema_name="UW_RAW_STG",
        )

        self.assertEqual(inserted, 7000)
        self.assertGreater(cursor.executemany.call_count, 1)

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
