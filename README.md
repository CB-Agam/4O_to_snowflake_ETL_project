# 4D to Snowflake ETL

This project contains a direct 4D ODBC loader and a local SFTP-to-Snowpipe file loader.

## SFTP JSON to Snowpipe

The local Snowpipe workflow does not connect to 4D. The 4D administrator must export one JSON array of row objects per table to SFTP. The JSON object keys must use the source-column names from the Excel mapping workbook.

1. Copy `.env.example` to `.env` and configure the SFTP and Snowflake settings. SFTP must use a trusted host key from the machine's system known-hosts file or `SFTP_KNOWN_HOSTS_FILE`.
2. Generate an RSA key pair following [Snowflake key-pair authentication setup](https://docs.snowflake.com/en/user-guide/key-pair-auth). Register the public key on the Snowflake user. Set `SNOWFLAKE_PIPE_PRIVATE_KEY_FILE` and, if encrypted, `SNOWFLAKE_PIPE_PRIVATE_KEY_PASSPHRASE`. The same key is used for Snowflake connector uploads and Snowpipe REST, so scheduled runs do not need an interactive browser login or Snowflake password. Protect the private key as a secret. The Snowflake role needs privileges to create/use the target table, internal stage, and pipe.
3. Install the dependencies:

   ```powershell
   py -3.12 -m pip install -r requirements.txt
   ```

4. Ask the 4D export process to publish completed files with a `.json` extension, for example `Accounts_Aging_Item_20261012.json`. Upload under a temporary extension and rename to `.json` only after the export is complete.
5. Run the local loader:

   ```powershell
   py -3.12 scripts\run_sftp_snowpipe.py --table "Accounts Aging Item"
   ```

   Files may also have a timestamp or suffix after the table name. The loader maps source keys to target columns using the selected worksheet, uploads transformed JSON to a table-specific Snowflake internal stage, then submits the stage filename to Snowpipe's REST API. It reports files as **queued**, not loaded; Snowpipe loads asynchronously. The SFTP source files are not deleted or moved.

Run this command manually or schedule it with Windows Task Scheduler. The PC must be awake and connected to both SFTP and Snowflake while it runs. This first version processes one target table per run.

The loader streams the JSON array instead of keeping the whole export in memory. It names staged files from the source file content hash and uses `OVERWRITE=FALSE`; Snowpipe/Snowflake load history suppresses duplicate submissions of files that it still tracks. Keep finalized exports unchanged and publish changed contents as new files.

## Direct 4D ODBC loader

`scripts\run_initial_etl.py` continues to query 4D directly. Its optional `FOURD_QUERY_TIMEOUT_SECONDS` defaults to `1800` seconds.

## Important notes

- Never commit .env
- Never hard-code credentials in source files
- Use environment variables or a secrets manager in production
- Snowpipe ingests files; it cannot connect to or extract records from 4D.
- The separate `scripts\verify_connections.py` command tests direct 4D and Snowflake connections.
