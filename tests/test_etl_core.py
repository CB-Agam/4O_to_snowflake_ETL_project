import unittest

import pandas as pd

from etl.ddl import build_create_table_sql
from etl.metadata import table_metadata_from_dataframe
from config.settings import Settings


class EtlCoreTests(unittest.TestCase):
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
