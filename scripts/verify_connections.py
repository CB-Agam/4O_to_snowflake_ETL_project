import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from connections.fourd_connection import connect_to_4d, close_4d_connection
from connections.snowflake_connection import connect_to_snowflake, close_snowflake_connection
from config.logging_config import configure_logging as configure_pipeline_logging


def configure_logging():
    configure_pipeline_logging()


def main():
    configure_logging()
    logger = logging.getLogger("verify_connections")

    fourd_conn = None
    snowflake_conn = None

    try:
        try:
            fourd_conn = connect_to_4d()
            logger.info("[OK] 4D connection test succeeded")
        except Exception:
            logger.exception("[FAIL] 4D connection test failed")

        try:
            snowflake_conn = connect_to_snowflake()
            logger.info("[OK] Snowflake connection test succeeded")
        except Exception:
            logger.exception("[FAIL] Snowflake connection test failed")

        if fourd_conn is None or snowflake_conn is None:
            logger.error("One or both connection checks failed. Review your environment variables and driver setup.")
            return 1

        logger.info("Connection verification completed successfully")
        return 0
    finally:
        if fourd_conn is not None:
            try:
                close_4d_connection(fourd_conn)
            except Exception:
                logger.exception("Failed to close 4D connection cleanly")

        if snowflake_conn is not None:
            try:
                close_snowflake_connection(snowflake_conn)
            except Exception:
                logger.exception("Failed to close Snowflake connection cleanly")


if __name__ == "__main__":
    sys.exit(main())
