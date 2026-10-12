from __future__ import annotations

import logging

import paramiko

from config.settings import settings

logger = logging.getLogger(__name__)


def connect_to_sftp() -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    missing = settings.missing_sftp_settings()
    if missing:
        raise ValueError(f"Missing SFTP settings: {', '.join(missing)}")

    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()
    if settings.sftp_known_hosts_file:
        ssh_client.load_host_keys(settings.sftp_known_hosts_file)
    ssh_client.set_missing_host_key_policy(paramiko.RejectPolicy())

    try:
        ssh_client.connect(
            hostname=settings.sftp_host,
            port=settings.sftp_port,
            username=settings.sftp_username,
            password=settings.sftp_password or None,
            key_filename=settings.sftp_private_key_file or None,
            passphrase=settings.sftp_private_key_passphrase or None,
            timeout=30,
            banner_timeout=30,
            auth_timeout=30,
            allow_agent=False,
            look_for_keys=False,
        )
        logger.info("SFTP connection established with %s", settings.sftp_host)
        return ssh_client, ssh_client.open_sftp()
    except Exception:
        ssh_client.close()
        logger.exception("SFTP connection failed")
        raise
