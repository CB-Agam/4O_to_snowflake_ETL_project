# Incremental 4D-to-Snowflake loading

## Scope and operation

Incremental mode is an optional path in the existing runner. The original
full-load behavior remains the default. Incremental mode is table-generic: it
uses the selected workbook mappings and applies the same current/history and
state-management rules to each selected target table.

Run one table:

```powershell
py -3.12 scripts\run_initial_etl.py --table CLAIMS --incremental
```

Use a larger timestamp overlap when source timestamp precision or update timing
requires it:

```powershell
py -3.12 scripts\run_initial_etl.py --table CLAIMS --incremental --overlap-seconds 172800
```

Run multiple tables independently in one pipeline invocation:

```powershell
py -3.12 scripts\run_initial_etl.py --tables CLAIMS Vehicle_Table Alerts --incremental
```

The runner uses the configured Snowflake database and schema. Verify that those
settings point to a development schema before the first incremental run. The
legacy full load remains the default when `--incremental` is omitted.

## Timestamp change detection and internal fallback

The mapping workbook contains only source columns and their Snowflake target
columns. Incremental loading does not add derived hash columns to current or
history tables. It keeps row hashes only in its private source-state table to
compare candidate rows when a timestamp is NULL or a timestamp strategy is not
available.

| Case | Match and comparison | Result |
|---|---|---|
| Day 0 | Extract every source row | Load every mapped source column into current; leave `_H` empty |
| Key and `Modified...` timestamp available (optional `Created...`) | Scan the key plus mapped created/modified timestamps; fetch new keys, timestamp changes, overlap-window keys, and rows where either timestamp is NULL | Compare candidates with private row state; identify inserts and actual data changes |
| One or both timestamps are NULL | Fetch the row by its declared key and compare its full-row hash with private state | Same key and different row hash is changed; same key and same row hash is unchanged |
| New primary key | No matching key in private source state | Insert as new |
| Key available but no usable modified timestamp | Scan the full mapped dataset and compare keyed row hashes in private state | Detect new, updated, unchanged, and deleted rows |
| No primary key declared | Compare full-row hashes and duplicate occurrence counts in private state | Detect row changes/removals, but cannot reliably distinguish update from delete-plus-insert |

When both `CREATED...` and `Modified...` source timestamp columns are mapped,
the key scan checks both. The modified timestamp helps find updates, while the
created timestamp helps identify timestamp changes/newly created records. Rows
with NULL in either timestamp are fetched and checked against private row state;
rows with both timestamps NULL are therefore still checked by their key and
saved row hash. A
table with a key and only `CREATED...` (but no suitable `Modified...` timestamp)
uses the full key-hash scan so data changes are not missed. Row hashes include
all mapped source fields, including mapped timestamp fields, but are not
written as target columns.

After Day 0, each changed run appends the previous current table to `_H`, then
replaces current with only that run's changed rows. A no-change run leaves
current and history untouched. Existing initialized tables whose saved
strategy changes (for example, from `ROW_HASH` to `KEY_HASH`) must have their
incremental state explicitly reconciled before they can use the new strategy.
Internal row-state values are stored separately from current/history data.

## Synthetic `ACCOUNT_CONTACTS` Day 0 / Day 1 trial

To test the requested 24-row example without changing 4D or the live
`ACCOUNT_CONTACTS` target, save the supplied rows to
`ACCOUNT_CONTACTS_trial.csv` in the project root. Keep the header row and all 24
records, and save as UTF-8 CSV. The trial runner uses only this CSV; it does not
connect to 4D.

First point the Snowflake configuration to a development database/schema. The
runner builds the trial schema from the `Account Contacts` worksheet in the
mapping workbook, so the mapped `ACCOUNT_CONTACTS` table does not need to
already exist. It creates isolated `ACCOUNT_CONTACTS_TRIAL` and
`ACCOUNT_CONTACTS_TRIAL_H` tables in the configured schema.

Run Day 0 once:

```powershell
py -3.12 scripts\run_account_contacts_trial.py --phase day0
```

Expected result: 23 older records in `ACCOUNT_CONTACTS_TRIAL`, no rows in
`ACCOUNT_CONTACTS_TRIAL_H`.

Then run Day 1:

```powershell
py -3.12 scripts\run_account_contacts_trial.py --phase day1
```

The unique row with the maximum `MODIFIED_ON` in the CSV is treated as the
simulated new/updated record. Its `MODIFIED_ON` is rebased to the local time
when the trial runs. In a single DML transaction, Day 1 copies the existing
23 current rows into history, replaces the trial current table with that one
latest row, and records that the phase completed. Expected result:
`ACCOUNT_CONTACTS_TRIAL` has 1 row and `ACCOUNT_CONTACTS_TRIAL_H` has 23.
History rows retain the current table columns and include `HISTORY_BATCH_ID`
and `ARCHIVED_AT`. Blank fixture values in mapped numeric, boolean, date, or
time columns are loaded as SQL `NULL`; blank text values remain empty strings.

The trial runner refuses to repeat a completed phase or run Day 1 before Day 0.
To restart, use a fresh development schema or explicitly clear only the
`ACCOUNT_CONTACTS_TRIAL`, `ACCOUNT_CONTACTS_TRIAL_H`, and
`ACCOUNT_CONTACTS_TRIAL_CONTROL` trial objects. Never point this test at
production. To use a CSV elsewhere, pass `--fixture "C:\path\ACCOUNT_CONTACTS_trial.csv"`.

## Day 0: initial load

For a target such as `CLAIMS`:

1. The normal runner creates or prepares the mapped current table.
2. Incremental setup creates `CLAIMS_H` with the current-table schema and adds
   `HISTORY_BATCH_ID` and `ARCHIVED_AT` columns. The initial load does not copy
   its rows into history.
3. The source table is extracted in full. The loader checks mapped columns and,
   when using timestamp/key mode, verifies the source metadata and that the
   declared key is non-null and unique in the extracted dataset.
4. The extracted rows are loaded into a unique staging table. The mapped data
   and state snapshot are validated before the current table is changed.
5. A DML transaction replaces current data and persists the source-row state,
   watermark, and successful batch status together.

Expected result: `CLAIMS` contains the initial source dataset and `CLAIMS_H`
has no copied initial batch.

## Day 1 and later: changed batches

For mappings with exactly one declared key and one `Modified...` timestamp
column whose workbook type and live ODBC type are timestamp types, with an
optional mapped `Created...` timestamp:

1. Read the source key and mapped created/modified timestamp columns to identify
   new keys, keys whose timestamps changed, and keys that disappeared.
2. Fetch full mapped rows only for those candidates and rows in the overlap
   window. The default window is 24 hours; it includes records at the boundary.
   Rows with NULL created or modified timestamps are fetched and compared by
   a private row hash on every run.
3. Compare candidates with the last committed private per-key row-state
   snapshot, then validate the candidate batch.
4. In one DML transaction, append the previous current table to `_H`, delete
   current rows, insert the changed batch from staging, replace the per-table
   source snapshot, update the watermark, and mark the batch successful.

The current table intentionally represents the latest successful change batch
after the initial load; it is not a complete copy of the source after Day 1.
History is append-only and contains the previous current batch before each
replacement. A retry after a failed DML transaction rolls back both the history
append and current replacement.

If a primary key exists but a single suitable modified timestamp does not,
the loader scans the full mapped dataset and compares keyed row hashes in
private state to identify new and changed records. A composite primary key uses
this path too. If no primary key is declared, the loader instead uses full-row
hash-and-occurrence reconciliation; this detects changed rows and removals,
including duplicate identical rows, but cannot always distinguish an update
from a delete plus insert.

## No-change batches

When a source scan finds no row changes or deletions, the loader does not stage
or reload the current table, does not append history, and leaves the watermark
unchanged. `ETL_INCREMENTAL_BATCH_LOG` records the batch with `STATUS = SUCCESS`
and `CHANGE_STATUS = NO_CHANGE`.

## Snowflake objects and audit records

Incremental mode creates these helper tables in the configured Snowflake
database and schema:

- `ETL_INCREMENTAL_TABLE_STATE`: one state and expiring lock row per target
  table, including its strategy, initialization flag, last successful watermark,
  source count, and batch ID.
- `ETL_SOURCE_ROW_STATE`: committed source keys (or row-hash identities), row
  hashes, and modification timestamps used to compare future runs.
- `ETL_INCREMENTAL_BATCH_LOG`: one batch record with status, source scan and
  extraction counts, changes, inserts, updates, deletions, history row count,
  watermark boundaries, duration, and error details.

The current full-load and incremental modes both acquire the shared per-table
lock before target-table preparation and hold it through extraction and load.
The full-load mode creates only the lock/state table; the source-row snapshot
and batch log are created by incremental mode.

The existing `ETL_PIPELINE_LOG` continues to record the runner's step-level
audit lifecycle. Batch states use `RUNNING`, `SUCCESS`, `FAILED`, or
`INTERRUPTED`; `CHANGE_STATUS` distinguishes `CHANGED` from `NO_CHANGE`.
Passwords and connection tokens are not written to these records.

The loader creates the history and helper objects, and the unique staging
table, before starting the critical DML transaction. The transaction itself
contains DML only. This is deliberate: Snowflake DDL implicitly commits an
active transaction, and cannot be rolled back with the history/current data
changes.

Per-table concurrency uses a conditional state-row lock with a 24-hour expiry.
The lock is renewed after extraction and staging, and released after the run;
an abandoned lock becomes eligible after the expiry. If concurrent first-time
setup creates duplicate state rows,
incremental loading fails closed and requires the duplicate state rows to be
resolved rather than proceeding without a unique lock.

## Change detection and limitations

- A `Created...` timestamp alone is insufficient to detect updates. The
  optimized strategy requires a single mapped primary-key field and a
  modification timestamp; actual 4D metadata is checked at runtime.
- The key/timestamp strategy scans all source keys and timestamps on every run,
  but fetches complete rows only for candidates. It detects new keys and
  backdated changes when their key or modification timestamp differs from the
  saved snapshot. The overlap additionally catches content changes with equal
  timestamps inside the configured window.
- If a source update changes data without advancing its modification timestamp,
  and it falls outside the overlap window, timestamp mode cannot discover it.
  No timestamp watermark can prove such a change; configure the source to
  advance a reliable modification timestamp or use row-hash reconciliation.
- Timestamp/key mode detects hard deletes by comparing the full key projection.
  The batch log records the deletion count; the deleted source row is not added
  as a tombstone to the current table. Row-hash mode detects row removals as
  multiset differences, but without a unique source key it cannot always
  distinguish an update from a delete plus insert.
- A row-hash fallback transfers/scans all mapped source columns on each run.
  It trades more 4D read work for change detection without a trustworthy key and
  modification timestamp.
- Multi-column primary keys are not inferred by this implementation. Such
  tables use row-hash fallback rather than risking an incomplete key.
- A strategy change after the initial snapshot is rejected. Reconcile/reset
  the table's incremental state explicitly before changing its mapping.
- Null modification timestamps are accepted. A row with a null timestamp is
  fetched and compared by row hash on every run; this detects changes to those
  rows but adds source reads. Non-null timestamps still use key/timestamp
  detection and the configured overlap.
- A non-null modification timestamp can be stale or less precise than source
  updates. If content changes without advancing it and falls outside the
  overlap, timestamp mode cannot discover the change; use a verified source
  change mechanism or row-hash mode instead.
- The workbook's `PK` and timestamp labels are candidates only. Live 4D
  connectivity was unavailable during implementation, so each selected table
  still requires successful runtime metadata and data validation.

## Safe development verification

1. Set `SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA` to a dedicated development
   database/schema. Do not use production for these checks.
2. Use a dedicated, small 4D development table or a non-production 4D
   environment with known keys, timestamp ties, changed rows, and deletions.
3. Run Day 0 once. Verify current row count, empty `_H`, state snapshot, and a
   successful batch record.
4. Run again without source changes. Verify `CHANGE_STATUS = NO_CHANGE` and
   that current and history row counts/data did not change.
5. In the approved development source, add and modify rows, including equal
   modification timestamps. Run again and verify current contains the changed
   batch and history contains the previous current rows.
6. Test a deletion only in the development source and verify the logged delete
   count. Do not infer that the current table contains a deleted-row tombstone.
7. Inject or simulate a staging, history-insert, and current-replacement failure
   in unit tests. Confirm rollback, unchanged state/watermark, and safe retry.

Unit tests use mocked 4D and Snowflake connections and do not modify real
Snowflake or 4D data. No live integration test was run because the 4D ODBC
service refused the metadata connection during implementation.
