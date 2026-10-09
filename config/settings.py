import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


BASE_DIR = Path(__file__).resolve().parent.parent


class Settings:
    def __init__(self):
        self.fourd_server = os.getenv("FOURD_SERVER", "")
        self.fourd_port = os.getenv("FOURD_PORT", "")
        self.fourd_database = os.getenv("FOURD_DATABASE", "")
        self.fourd_username = os.getenv("FOURD_USERNAME", "")
        self.fourd_password = os.getenv("FOURD_PASSWORD", "")
        self.fourd_dsn = os.getenv("FOURD_DSN", "")
        self.fourd_driver = os.getenv("FOURD_DRIVER", "")

        self.snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT", "")
        self.snowflake_user = os.getenv("SNOWFLAKE_USER", "")
        self.snowflake_password = os.getenv("SNOWFLAKE_PASSWORD", "")
        self.snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "")
        self.snowflake_database = os.getenv("SNOWFLAKE_DATABASE", "")
        self.snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA", "")
        self.snowflake_role = os.getenv("SNOWFLAKE_ROLE", "")
        self.snowflake_authenticator = os.getenv("SNOWFLAKE_ROLE", "externalbrowser")

    def missing_4d_settings(self):
        required = {
            "FOURD_SERVER": self.fourd_server,
            "FOURD_PORT": self.fourd_port,
            "FOURD_USERNAME": self.fourd_username,
            "FOURD_PASSWORD": self.fourd_password,
        }
        missing = [name for name, value in required.items() if not value]
        return missing

    def missing_snowflake_settings(self):
        required = {
            "SNOWFLAKE_ACCOUNT": self.snowflake_account,
            "SNOWFLAKE_USER": self.snowflake_user,
            "SNOWFLAKE_WAREHOUSE": self.snowflake_warehouse,
            "SNOWFLAKE_DATABASE": self.snowflake_database,
            "SNOWFLAKE_SCHEMA": self.snowflake_schema,
            "SNOWFLAKE_ROLE": self.snowflake_role,
        }

        if self.snowflake_authenticator != "externalbrowser":
            required["SNOWFLAKE_PASSWORD"] = self.snowflake_password

        missing = [name for name, value in required.items() if not value]
        return missing

    def fourd_connection_string(self):
        if self.fourd_dsn:
            connection_parts = [
                f"DSN={self.fourd_dsn};",
                f"UID={self.fourd_username};",
                f"PWD={self.fourd_password};",
            ]
            if self.fourd_database:
                connection_parts.append(f"DATABASE={self.fourd_database};")
            connection_parts.extend(
                [
                    f"SERVER={self.fourd_server};",
                    f"PORT={self.fourd_port};",
                ]
            )
            return "".join(connection_parts)

        driver = self.fourd_driver or "{4D v18 ODBC Driver 64-bit}"
        connection_parts = [
            f"DRIVER={driver};",
            f"SERVER={self.fourd_server};",
            f"PORT={self.fourd_port};",
            f"UID={self.fourd_username};",
            f"PWD={self.fourd_password};",
        ]
        if self.fourd_database:
            connection_parts.append(f"DATABASE={self.fourd_database};")
        return "".join(connection_parts)

    def snowflake_connection_config(self):
        config = {
            "account": self.snowflake_account,
            "user": self.snowflake_user,
            "warehouse": self.snowflake_warehouse,
            "database": self.snowflake_database,
            "schema": self.snowflake_schema,
            "role": self.snowflake_role,
            "authenticator": self.snowflake_authenticator,
        }
        if self.snowflake_password:
            config["password"] = self.snowflake_password
        return config


settings = Settings()
