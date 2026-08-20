## Contents

- [When This Applies](#when-this-applies)
- [Why Spark Can't Read the File Directly](#why-spark-cant-read-the-file-directly)
- [Decrypt on the Driver, Not the Executors](#decrypt-on-the-driver-not-the-executors)
- [The Two CSV Options That Are Load-Bearing](#the-two-csv-options-that-are-load-bearing)
- [Full Snapshot vs. Incremental Export](#full-snapshot-vs-incremental-export)
- [Lookback Window, Not "Today Only"](#lookback-window-not-today-only)
- [Append-Only Bronze](#append-only-bronze)

---

# Age-Encrypted CSV → Bronze Delta Ingest Pattern

> **Part of:** [databricks](../SKILL.md)

Pattern for ingesting an age-encrypted, gzipped CSV export — a backup sidecar, a cross-system dump, anything produced outside Databricks and dropped into cloud storage — into a Databricks bronze Delta table. Verified end-to-end against a live workspace (real rows landed, zero duplicates on the merge key).

## When This Applies

Source files with a `.csv.gz.age` (or similar layered-encryption) extension, typically date-partitioned (`dt=YYYY-MM-DD/`) in cloud storage. Not applicable to plain Parquet/CSV/Delta sources — those read natively.

## Why Spark Can't Read the File Directly

Neither Auto Loader, `COPY INTO`, nor any Spark reader understands age encryption. The file must be decrypted before Spark ever touches it. There is no way around a driver-side (or UDF-side) decryption step.

## Decrypt on the Driver, Not the Executors

Use `pyrage` (Python bindings for `age`, prebuilt wheel, no Rust toolchain needed) inside plain Python — not a Spark UDF — so the private key is read once via `dbutils.secrets.get(...)` and never shipped to executors. # SECURITY: `dbutils.secrets.get` reads a workspace secret at runtime; the value must never be logged, printed, or written to a non-Volume path.

```python
from pyrage import x25519
identity = x25519.Identity.from_str(dbutils.secrets.get(scope="...", key="...").strip())
plaintext = pyrage.decrypt(encrypted_bytes, [identity])
```

Both `age` key modes exist in practice — `-r <recipient>` (X25519 identity) and `-p` (passphrase). The filename alone doesn't tell you which was used; make it a notebook widget rather than assuming.

Write the decrypted output to a UC Volume staging path, not `/tmp` — see [serverless-and-uc-gotchas.md](serverless-and-uc-gotchas.md#3-driver-local-filesystem-outside-workspace-and-volumes). Downstream Auto Loader reads gzip natively; there's no need to decompress before staging.

## The Two CSV Options That Are Load-Bearing

If the source was written by Postgres's own CSV writer (`COPY ... WITH (FORMAT csv, HEADER)`), two Spark reader options are correctness requirements, not performance tuning:

```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    ...
```

- **`multiLine=true`** — a value containing a real newline is written as a quoted field spanning multiple physical lines. Without this, Spark's line-based reader tears one logical record into several malformed rows.
- **`escape='"'`** — Postgres's CSV writer doubles embedded quotes RFC-4180 style (`"a ""quoted"" value"`). Spark's *default* escape character is backslash, which both mis-parses doubled quotes and — more damagingly — silently corrupts any value containing a literal backslash. Confirmed by direct test: a value written as `back\slash` parses to `backslash` under Spark's default escape setting. This is exactly the kind of corruption that produces a correct row count and wrong data, with nothing to flag it.

If the source instead came from `pg_dump`'s default *text* COPY format (not CSV), these options don't apply — that format uses different, backslash-based escaping (`\n`, `\t`, `\\`, `\N` for NULL) and needs a custom parser, not CSV reader options. Confirm which format actually produced the file before assuming either path.

## Full Snapshot vs. Incremental Export

Check whether each file is a full table snapshot or only new/changed rows — this changes the MERGE key entirely:

- **Full snapshot** (e.g. `\copy (SELECT * FROM table) TO STDOUT`, run nightly): the same row appears in every file within the lookback window. MERGE on the table's real primary key, and collapse each key to its newest snapshot *before* merging — a plain MERGE on a snapshot-only batch will raise on multiple source-row matches per key.
- **Incremental export**: MERGE on the primary key directly, no collapse step needed. A content hash (`sha2` over all columns) is a reasonable fallback key *only* when no real primary key exists — a hash key silently produces a new row every time an existing record is edited upstream, since the hash of the edited row no longer matches the old one.

## Lookback Window, Not "Today Only"

If the source has any retention policy (files deleted after N days) — even a policy the user only half-remembers — process the last N days of partitions on every run, not just today's. A single failed run against a "today only" job creates a permanent, silent gap the moment retention deletes the file. Reprocessing older partitions is nearly free once the MERGE is idempotent (which it must be regardless, to support job retries), so there's no real cost to the wider window — only upside.

## Append-Only Bronze

If the requirement is "bronze should keep data even after the source drops it" (common for backup/audit exports), the MERGE must never contain `WHEN NOT MATCHED BY SOURCE THEN DELETE`. Track the last-seen date instead (a forward-only `whenMatchedUpdate` on a watermark column), and let the row itself persist indefinitely. This single clause is the one line that would silently defeat that requirement — worth a comment in the code calling it out explicitly, since it's easy for a future edit to "clean up" the MERGE and add it back in.
