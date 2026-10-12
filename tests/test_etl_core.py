import unittest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import simplejson as json
import pandas as pd

from etl.ddl import build_create_table_sql
from etl.metadata import table_metadata_from_dataframe
from config.settings import Settings
from connections.fourd_connection import connect_to_4d
from etl.load import build_source_query
from etl.sftp_snowpipe import (
    create_snowpipe_objects,
    find_table_json_files,
    map_json_export_to_target,
    snowpipe_object_names,
    stage_and_submit_json_file,
    transform_json_file,
)


class EtlCoreTests(unittest.TestCase):
    @patch("connections.fourd_connection.pyodbc.connect")
    @patch("connections.fourd_connection.settings")
    def test_connect_to_4d_does_not_execute_probe_query(self, settings, connect):
        settings.missing_4d_settings.return_value = []
        settings.fourd_connection_string.return_value = "DRIVER=test;"
        settings.fourd_server = "server"
        settings.fourd_port = "19813"
        settings.fourd_query_timeout_seconds = 300
        connection = Mock()
        connect.return_value = connection

        result = connect_to_4d()

        self.assertIs(result, connection)
        connect.assert_called_once_with("DRIVER=test;", timeout=30)
        self.assertEqual(connection.timeout, 300)
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

    def test_snowflake_key_pair_settings_support_unattended_connections(self):
        settings = Settings()
        settings.snowflake_account = "account"
        settings.snowflake_user = "user"
        settings.snowflake_warehouse = "warehouse"
        settings.snowflake_database = "database"
        settings.snowflake_schema = "schema"
        settings.snowflake_role = "role"
        settings.snowflake_password = ""
        settings.snowflake_authenticator = "externalbrowser"
        settings.snowflake_pipe_private_key_file = "C:\\keys\\snowflake_key.p8"
        settings.snowflake_pipe_private_key_passphrase = "test_passphrase"

        connection_config = settings.snowflake_connection_config()

        self.assertEqual(settings.missing_snowflake_settings(), [])
        self.assertEqual(connection_config["private_key_file"], "C:\\keys\\snowflake_key.p8")
        self.assertEqual(connection_config["private_key_file_pwd"], "test_passphrase")
        self.assertNotIn("authenticator", connection_config)
        self.assertNotIn("password", connection_config)

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

    def test_build_source_query_quotes_identifiers_and_skips_blank_columns(self):
        query = build_source_query(
            "Acc_Cheque_Payments",
            ["Cheque_ID_No", "Cheque Date", "", "Name \"With\" Quotes"],
        )

        self.assertEqual(
            query,
            'SELECT "Cheque_ID_No", "Cheque Date", "Name ""With"" Quotes" '
            'FROM "Acc_Cheque_Payments"',
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

    def test_map_json_export_to_target_uses_mapping_and_generated_timestamps(self):
        rows = [
            {
                "source_table": "Source Table",
                "source_column": "Cheque Date",
                "target_column": "CHEQUE_DATE",
                "transformation": "",
            },
            {
                "source_table": "",
                "source_column": "",
                "target_column": "CREATED_TIMESTAMP",
                "transformation": "CURRENT_TIMESTAMP() when inserted",
            },
        ]

        result = map_json_export_to_target([{"cheque date": "2026-10-12"}], rows)

        self.assertEqual(result[0]["CHEQUE_DATE"], "2026-10-12")
        self.assertRegex(result[0]["CREATED_TIMESTAMP"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")

    def test_map_json_export_requires_array_of_records(self):
        with self.assertRaisesRegex(ValueError, "array of row objects"):
            map_json_export_to_target({"row": 1}, [])

    def test_transform_json_file_streams_array_and_preserves_decimal_precision(self):
        rows = [
            {
                "source_table": "Source Table",
                "source_column": "Amount",
                "target_column": "AMOUNT",
                "transformation": "",
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            source_path = Path(directory) / "source.json"
            target_path = Path(directory) / "target.json"
            source_path.write_text('[{"Amount":0.1234567890123456789}]', encoding="utf-8")

            count = transform_json_file(source_path, target_path, rows)
            output = json.loads(target_path.read_text(encoding="utf-8"), use_decimal=True)

        self.assertEqual(count, 1)
        self.assertEqual(str(output[0]["AMOUNT"]), "0.1234567890123456789")

    def test_stage_and_submit_uploads_file_and_queues_it(self):
        connection = Mock()
        cursor = connection.cursor.return_value
        cursor.description = [("status",), ("message",)]
        cursor.fetchall.return_value = [("UPLOADED", "")]
        ingest_manager = Mock()
        ingest_manager.ingest_files.return_value = {"responseCode": "SUCCESS"}
        objects = {"stage": '"DB"."SCHEMA"."TEST_STAGE"'}

        with tempfile.TemporaryDirectory() as directory:
            staged_path = Path(directory) / "content-hash.json"
            staged_path.write_text("[]", encoding="utf-8")
            filename = stage_and_submit_json_file(
                connection,
                objects,
                ingest_manager,
                staged_path,
                "content-hash",
            )

        self.assertEqual(filename, "content-hash.json")
        self.assertIn("PUT ", cursor.execute.call_args.args[0])
        queued_file = ingest_manager.ingest_files.call_args.args[0][0]
        self.assertEqual(queued_file.path, "content-hash.json")
        cursor.close.assert_called_once()

    def test_find_table_json_files_matches_table_name_with_suffix(self):
        sftp = Mock()
        sftp.listdir_attr.return_value = [
            Mock(filename="Accounts_Aging_Item_20261012.json"),
            Mock(filename="Other_Table.json"),
            Mock(filename="Accounts_Aging_Item.partial"),
        ]

        files = find_table_json_files(sftp, "/exports", ["Accounts Aging Item"])

        self.assertEqual(
            files,
            [("Accounts_Aging_Item_20261012.json", "/exports/Accounts_Aging_Item_20261012.json")],
        )

    def test_create_snowpipe_objects_creates_table_specific_stage_and_pipe(self):
        cursor = Mock()
        objects = snowpipe_object_names("DW", "RAW", "ACCOUNTS_AGING_ITEM")

        create_snowpipe_objects(cursor, objects)

        self.assertEqual(cursor.execute.call_count, 2)
        self.assertIn("CREATE STAGE IF NOT EXISTS", cursor.execute.call_args_list[0].args[0])
        pipe_sql = cursor.execute.call_args_list[1].args[0]
        self.assertIn("STRIP_OUTER_ARRAY=TRUE", pipe_sql)
        self.assertIn("MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE", pipe_sql)


if __name__ == "__main__":
    unittest.main()
