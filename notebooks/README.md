# Bronze `msg` daily ingest

`bronze_msg_daily.py` — Databricks notebook that lands the daily message export from
S3 into an append-only Delta bronze table.

```
s3://shira-digest-backups/analytics/dt=YYYY-MM-DD/messages.csv.gz.age
   │  age-decrypt (driver)
   ▼
/Volumes/<catalog>/<schema>/landing/msg/staged/dt=.../messages.csv.gz
   │  Auto Loader (all-string, rescue mode)
   ▼
<catalog>.<schema>.msg      insert-only MERGE, never deleted from
```

## Why it isn't "read today's file"

The source bucket has a 7-day retention. A notebook that only reads `dt=<today>` loses
that day **permanently** the moment retention deletes the file — one failed run, one
silent gap. Instead every run re-scans the last `lookback_days` (default 7) partitions.
The MERGE makes reprocessing free, so a missed run self-heals as long as it's noticed
inside the retention window. A missing `dt=<today>` raises rather than no-ops.

Bronze itself is never pruned: there is no `UPDATE` and deliberately no
`WHEN NOT MATCHED BY SOURCE THEN DELETE`. Rows that age out of S3 live on in bronze.
**Do not add that clause** — it is the one change that would break the requirement.

## Setup

1. **Unity Catalog external location** over `s3://shira-digest-backups/` with a storage
   credential the job's principal can `READ FILES` on. Verify with:
   ```sql
   LIST 's3://shira-digest-backups/analytics/';
   ```
2. **Secret** holding the age private key:
   ```bash
   databricks secrets create-scope digest-backups
   databricks secrets put-secret digest-backups age-identity   # AGE-SECRET-KEY-1...
   ```
   Set the `age_mode` widget to `identity` for an X25519 key, or `passphrase` if the
   export was encrypted with `age -p`. Both decrypt paths were verified locally against
   `pyrage` 1.3.0 with a real encrypt/decrypt round-trip.
3. Catalog, schema and staging volume are created by the notebook if absent.

## Widgets

| Widget | Default | Notes |
| --- | --- | --- |
| `catalog` / `schema` / `table` | `main` / `bronze` / `msg` | target table |
| `volume` | `landing` | staging volume for decrypted files |
| `s3_root` | `s3://shira-digest-backups/analytics` | no trailing slash needed |
| `secret_scope` / `secret_key` | `digest-backups` / `age-identity` | age key |
| `age_mode` | `identity` | `identity` (X25519) or `passphrase` |
| `lookback_days` | `7` | keep aligned with S3 retention |
| `merge_key` | `_row_hash` | see below |
| `fail_on_missing_today` | `true` | set `false` for a backfill run |
| `checkpoint_root` | *(blank)* | blank = staging volume; see below |

### `merge_key`

Defaults to `_row_hash` — `sha2` over all source columns — so "only append new lines"
means *rows whose content hasn't been seen before*. This works without knowing the
export's schema. **If `messages.csv` has a real primary key (e.g. `message_id`), point
`merge_key` at it**: content hashing will insert a second row if a message is ever
edited upstream, whereas a business key will not.

## Schedule

Import the notebook, then create the job:

```bash
databricks jobs create --json @notebooks/job.bronze_msg_daily.json
```

Adjust `schedule.quartz_cron_expression` so it fires *after* the upstream export lands.
The job uses `max_concurrent_runs: 1` — overlapping runs would contend on the Auto
Loader checkpoint.

## What is and isn't verified

The age decryption (both key modes), the Python syntax and the job JSON were checked
locally. **Nothing has been run against Databricks** — the UC external location, the
Volumes I/O, the Auto Loader stream and the MERGE are unexercised. Watch the first run
for two things in particular:

- **Checkpoint on a Volume.** Streaming checkpoints need rename semantics that
  object-storage-backed Volumes don't always provide. If the stream errors on the
  checkpoint, set the `checkpoint_root` widget to an external location such as
  `s3://shira-digest-backups/_checkpoints/bronze_msg/` (needs write access). That path
  also survives the Volume being recreated.
- **Staging rename.** The decrypt writes `*.partial` then renames. `os.replace` is tried
  first with a `dbutils.fs.mv` fallback for the same reason.

## Operating notes

- **`_rescued_data`** is non-null when the export gains or malforms a column. The run
  still succeeds (rescue mode); the verification cell prints a warning. Widen the table
  deliberately when this appears.
- **Re-running is always safe.** The MERGE is the sole idempotency guarantee — the Auto
  Loader checkpoint only speeds up file discovery. If the checkpoint is ever wiped, the
  worst case is re-reading staged files, not duplicate rows.
- **Driver-side decrypt** is capped at 512 MB per file (`MAX_DECRYPT_BYTES`). Raise it on
  a bigger driver if the export grows.
- **Backfill** beyond 7 days is only possible if the files still exist. Raise
  `lookback_days` and set `fail_on_missing_today=false`.
- **Table layout:** bronze is created with `CLUSTER BY (_row_hash)`. The merge predicate
  is a hash, so file-level min/max stats prune almost nothing — without clustering the
  daily MERGE degrades into a full-table scan as the table grows. If you switch
  `merge_key` to a business key, re-cluster on that column too.

