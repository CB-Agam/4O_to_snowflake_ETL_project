import logging
import os
import sys

import snowflake.connector

from config.settings import settings

logger = logging.getLogger(__name__)


def _find_real_python_executable() -> str | None:
    """Return the real interpreter path when Windows is resolving to the App Execution Alias."""
    alias_marker = r"WindowsApps\PythonSoftwareFoundation"
    candidates: list[str] = []

    for value in (
        getattr(sys, "_base_executable", "") or "",
        getattr(sys, "executable", "") or "",
        os.environ.get("PYTHONEXECUTABLE", ""),
        os.environ.get("VIRTUAL_ENV", ""),
        sys.prefix,
        sys.base_prefix,
    ):
        if not value:
            continue
        if os.path.isdir(value):
            candidates.extend(
                [
                    os.path.join(value, "python.exe"),
                    os.path.join(value, "Scripts", "python.exe"),
                ]
            )
        else:
            candidates.append(value)

    seen: set[str] = set()
    for candidate in candidates:
        if not candidate or not os.path.exists(candidate):
            continue
        normalized = os.path.normcase(os.path.normpath(candidate))
        if normalized in seen:
            continue
        seen.add(normalized)
        if alias_marker not in candidate:
            return candidate
    return None


def _repair_windows_python_base_executable() -> None:
    """Snowflake's libc detection can choke on the Windows App Execution Alias path.
    If the active interpreter points to the alias, reset the active and base executable
    to the real Python executable before creating the Snowflake connection.
    """
    alias_marker = r"WindowsApps\PythonSoftwareFoundation"
    current_executable = getattr(sys, "executable", "") or ""
    base_executable = getattr(sys, "_base_executable", "") or ""
    if alias_marker not in current_executable and alias_marker not in base_executable:
        return

    real_python = _find_real_python_executable()
    if not real_python:
        return

    sys.executable = real_python
    sys._base_executable = real_python
    os.environ["PYTHONEXECUTABLE"] = real_python


_repair_windows_python_base_executable()


def connect_to_snowflake():
    """Open a reusable Snowflake connection using account, user, password, role, warehouse, database, and schema."""
    _repair_windows_python_base_executable()

    missing = settings.missing_snowflake_settings()
    if missing:
        raise ValueError(f"Missing Snowflake environment variables: {', '.join(missing)}")
    network_timeout = settings.snowflake_network_timeout_seconds
    if network_timeout < 0:
        raise ValueError("SNOWFLAKE_NETWORK_TIMEOUT_SECONDS cannot be negative")

    try:
        connection = snowflake.connector.connect(
            **settings.snowflake_connection_config(),
            login_timeout=30,
            network_timeout=network_timeout or None,
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
