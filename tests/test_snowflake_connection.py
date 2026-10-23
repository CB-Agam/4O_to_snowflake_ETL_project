import unittest
from unittest.mock import MagicMock, patch

from connections.snowflake_connection import connect_to_snowflake


class SnowflakeConnectionTests(unittest.TestCase):
    @patch("connections.snowflake_connection.snowflake.connector.connect")
    @patch("connections.snowflake_connection.settings")
    def test_connection_uses_utc_and_24_hour_timestamp_format(
        self, settings, snowflake_connect
    ):
        settings.missing_snowflake_settings.return_value = []
        settings.snowflake_connection_config.return_value = {"account": "test"}
        connection = MagicMock()
        snowflake_connect.return_value = connection

        result = connect_to_snowflake()

        self.assertIs(result, connection)
        snowflake_connect.assert_called_once_with(
            account="test",
            session_parameters={
                "TIMEZONE": "UTC",
                "TIMESTAMP_NTZ_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF3",
            },
        )
        connection.cursor.return_value.__enter__.return_value.execute.assert_called_once_with(
            "SELECT 1"
        )


if __name__ == "__main__":
    unittest.main()
