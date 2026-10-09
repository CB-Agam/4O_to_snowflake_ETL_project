import logging

import snowflake.connector

from config.settings import settings

logger = logging.getLogger(__name__)


def connect_to_snowflake():
    """Open a reusable Snowflake connection using account, user, password, role, warehouse, database, and schema."""
    missing = settings.missing_snowflake_settings()
    if missing:
        raise ValueError(f"Missing Snowflake environment variables: {', '.join(missing)}")

    try:
        connection = snowflake.connector.connect(
            **settings.snowflake_connection_config(),
            login_timeout=30,
            network_timeout=60,
        )

        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()

        logger.info("Snowflake connection successful")
        return connection
    except snowflake.connector.errors.DatabaseError as exc:
        logger.exception("Snowflake connection failed: %s", exc)
        raise
    except Exception:
        logger.exception("Unexpected error while connecting to Snowflake")
        raise


def close_snowflake_connection(connection):
    if connection is not None:
        try:
            connection.close()
            logger.info("Snowflake connection closed")
        except Exception:
            logger.exception("Error while closing Snowflake connection")
            raise
