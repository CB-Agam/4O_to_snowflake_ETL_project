import logging

import pyodbc

from config.settings import settings

logger = logging.getLogger(__name__)


def connect_to_4d():
    """Open a reusable connection to the 4D database using either DSN or direct driver settings."""
    missing = settings.missing_4d_settings()
    if missing:
        raise ValueError(f"Missing 4D environment variables: {', '.join(missing)}")

    conn_str = settings.fourd_connection_string()
    logger.info("Attempting connection to 4D server %s:%s", settings.fourd_server, settings.fourd_port)

    try:
        connection = pyodbc.connect(conn_str, timeout=30)
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        logger.info("4D connection successful")
        return connection
    except pyodbc.Error as exc:
        logger.exception("4D connection failed: %s", exc)
        raise
    except Exception:
        logger.exception("Unexpected error while connecting to 4D")
        raise


def close_4d_connection(connection):
    if connection is not None:
        try:
            connection.close()
            logger.info("4D connection closed")
        except Exception:
            logger.exception("Error while closing 4D connection")
            raise
