# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — `msg` daily ingest
# MAGIC
# MAGIC Loads the daily message export from S3 into an append-only Delta bronze table.
# MAGIC
# MAGIC **Source:** `s3://shira-digest-backups/analytics/dt=YYYY-MM-DD/messages.csv.gz.age`
# MAGIC (gzipped CSV, then age-encrypted — Spark cannot read `.age` directly, so the
# MAGIC notebook decrypts to a UC Volume first and reads from there.)
# MAGIC
# MAGIC ### Design notes
# MAGIC
# MAGIC * **Lookback, not "today only".** The source bucket has a 7-day retention, so a
# MAGIC   single failed run would lose that day *permanently* once the file is deleted.
# MAGIC   Every run re-scans the last `lookback_days` partitions; the MERGE makes
# MAGIC   reprocessing free and any missed run inside the window self-heals.
# MAGIC * **Bronze is never deleted from.** The MERGE has no `UPDATE` and deliberately no
# MAGIC   `WHEN NOT MATCHED BY SOURCE THEN DELETE` — rows aged out of S3 stay in bronze
# MAGIC   forever. Do not add that clause.
# MAGIC * **MERGE is the only idempotency guarantee.** The Auto Loader checkpoint is a
# MAGIC   performance optimisation for file discovery, not a correctness mechanism. If the
# MAGIC   checkpoint is wiped, re-running is still safe.
# MAGIC * **Bronze fidelity.** Every source column is read as `string` with no type
# MAGIC   inference. Malformed/extra columns land in `_rescued_data` rather than failing
# MAGIC   the run.

# COMMAND ----------

# MAGIC %pip install pyrage==1.3.0
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "1. Catalog")
dbutils.widgets.text("schema", "bronze", "2. Schema")
dbutils.widgets.text("table", "msg", "3. Table")
dbutils.widgets.text("volume", "landing", "4. Staging volume")
dbutils.widgets.text("s3_root", "s3://shira-digest-backups/analytics", "5. S3 root")
dbutils.widgets.text("secret_scope", "digest-backups", "6. Secret scope")
dbutils.widgets.text("secret_key", "age-identity", "7. Secret key (age identity)")
dbutils.widgets.dropdown("age_mode", "identity", ["identity", "passphrase"], "8. Age key mode")
dbutils.widgets.text("lookback_days", "7", "9. Lookback days")
dbutils.widgets.text("merge_key", "_row_hash", "10. Merge key column")
dbutils.widgets.dropdown("fail_on_missing_today", "true", ["true", "false"], "11. Fail if today missing")
# Streaming checkpoints on UC Volumes depend on rename semantics that object storage
# does not always provide. If the stream errors on the checkpoint, repoint this at an
# external location, e.g. s3://shira-digest-backups/_checkpoints/bronze_msg/.
dbutils.widgets.text("checkpoint_root", "", "12. Checkpoint root (blank = volume)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
TABLE = dbutils.widgets.get("table")
VOLUME = dbutils.widgets.get("volume")
S3_ROOT = dbutils.widgets.get("s3_root").rstrip("/")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
SECRET_KEY = dbutils.widgets.get("secret_key")
AGE_MODE = dbutils.widgets.get("age_mode")
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
MERGE_KEY = dbutils.widgets.get("merge_key")
FAIL_ON_MISSING_TODAY = dbutils.widgets.get("fail_on_missing_today") == "true"

FQN = f"`{CATALOG}`.`{SCHEMA}`.`{TABLE}`"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{TABLE}"
STAGING_ROOT = f"{VOLUME_ROOT}/staged"
SCHEMA_ROOT = f"{VOLUME_ROOT}/_schema"
CHECKPOINT_ROOT = dbutils.widgets.get("checkpoint_root").rstrip("/") or f"{VOLUME_ROOT}/_checkpoint"

print(f"target      : {FQN}")
print(f"source      : {S3_ROOT}/dt=*/")
print(f"staging     : {STAGING_ROOT}")
print(f"lookback    : {LOOKBACK_DAYS} days")
print(f"merge key   : {MERGE_KEY}")

# COMMAND ----------

# MAGIC %md ## 1. Prerequisites — catalog, schema, staging volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")

for path in (STAGING_ROOT, SCHEMA_ROOT, CHECKPOINT_ROOT):
    dbutils.fs.mkdirs(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decrypt the lookback window into staging
# MAGIC
# MAGIC Each `dt=` partition is copied from S3, age-decrypted on the driver, and written to
# MAGIC the staging volume as `messages.csv.gz`. Gzip is left intact — Spark decompresses it
# MAGIC natively, and a single daily file is small enough that non-splittability is a non-issue.

# COMMAND ----------

import datetime as _dt
import os
import shutil

import pyrage
from pyrage import passphrase, x25519

# SECURITY: the age private key decrypts the S3 backup export. It is read from the
# Databricks secret scope at runtime, never persisted to disk or logged. Redaction of
# secret values in notebook output is enforced by Databricks itself.
_AGE_SECRET = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)

if AGE_MODE == "identity":
    _IDENTITIES = [x25519.Identity.from_str(_AGE_SECRET.strip())]

    def age_decrypt(blob: bytes) -> bytes:
        return pyrage.decrypt(blob, _IDENTITIES)
else:

    def age_decrypt(blob: bytes) -> bytes:
        return passphrase.decrypt(blob, _AGE_SECRET)


def partition_dates(lookback: int):
    """The lookback window, most recent first."""
    today = _dt.date.today()
    return [today - _dt.timedelta(days=offset) for offset in range(lookback)]


def list_encrypted_files(prefix: str):
    try:
        return sorted(
            entry.path for entry in dbutils.fs.ls(prefix) if entry.path.endswith(".age")
        )
    except Exception as exc:  # partition absent (not yet exported, or aged out)
        if "FileNotFoundException" in str(exc) or "not found" in str(exc).lower():
            return []
        raise

# COMMAND ----------

MAX_DECRYPT_BYTES = 512 * 1024 * 1024  # driver-side decrypt; guard against surprise files

staged, skipped, missing = [], [], []

for day in partition_dates(LOOKBACK_DAYS):
    dt_str = day.isoformat()
    encrypted = list_encrypted_files(f"{S3_ROOT}/dt={dt_str}/")

    if not encrypted:
        missing.append(dt_str)
        continue

    for src in encrypted:
        name = os.path.basename(src).removesuffix(".age")  # messages.csv.gz.age -> messages.csv.gz
        dest_dir = f"{STAGING_ROOT}/dt={dt_str}"
        dest = f"{dest_dir}/{name}"

        if os.path.exists(dest):
            skipped.append(dest)
            continue

        local_encrypted = f"/tmp/{dt_str}__{os.path.basename(src)}"
        dbutils.fs.cp(src, f"file:{local_encrypted}")
        try:
            size = os.path.getsize(local_encrypted)
            if size > MAX_DECRYPT_BYTES:
                raise RuntimeError(
                    f"{src} is {size} bytes, over the {MAX_DECRYPT_BYTES} driver-decrypt "
                    "limit. Raise MAX_DECRYPT_BYTES on a larger driver, or split the export."
                )
            with open(local_encrypted, "rb") as fh:
                blob = fh.read()

            try:
                plaintext = age_decrypt(blob)
            except pyrage.DecryptError as exc:
                raise RuntimeError(
                    f"age decryption failed for {src}. Check that secret "
                    f"{SECRET_SCOPE}/{SECRET_KEY} holds the right key and that age_mode="
                    f"'{AGE_MODE}' matches how the file was encrypted."
                ) from exc

            # Write via a .partial name so a crash mid-write never leaves a truncated
            # file that Auto Loader would happily ingest. os.replace is atomic on POSIX
            # but Volumes are object-storage backed, so fall back to dbutils.fs.mv.
            os.makedirs(dest_dir, exist_ok=True)
            tmp_dest = f"{dest}.partial"
            with open(tmp_dest, "wb") as fh:
                fh.write(plaintext)
            try:
                os.replace(tmp_dest, dest)
            except OSError:
                dbutils.fs.mv(f"dbfs:{tmp_dest}", f"dbfs:{dest}")
            staged.append(dest)
        finally:
            if os.path.exists(local_encrypted):
                os.remove(local_encrypted)

print(f"newly staged : {len(staged)}")
for path in staged:
    print(f"  + {path}")
print(f"already staged: {len(skipped)}")
print(f"missing dt=   : {missing or 'none'}")

# COMMAND ----------

# A missing *today* means the upstream export did not land. Fail loudly — a silent no-op
# is how gaps accumulate unnoticed until the 7-day retention makes them unrecoverable.
today_str = _dt.date.today().isoformat()
if FAIL_ON_MISSING_TODAY and today_str in missing:
    raise RuntimeError(
        f"No .age export found at {S3_ROOT}/dt={today_str}/. The upstream backup did not "
        "land. Older partitions in the lookback window were still processed; re-run once "
        f"the export arrives (within {LOOKBACK_DAYS} days, before retention deletes it)."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Auto Loader → MERGE into bronze
# MAGIC
# MAGIC `schemaEvolutionMode="rescue"` keeps the stream from failing when the export gains a
# MAGIC column — new fields land in `_rescued_data` and the run still completes. Review that
# MAGIC column periodically and widen the table deliberately.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

reader = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_ROOT)
    .option("cloudFiles.inferColumnTypes", "false")   # bronze fidelity: everything a string
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .load(STAGING_ROOT)
)

METADATA_COLS = {"_ingest_ts", "_source_file", "_dt", "_row_hash", "_rescued_data"}


def with_metadata(df):
    source_cols = [c for c in df.columns if c not in METADATA_COLS]
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_dt", F.regexp_extract(F.col("_metadata.file_path"), r"dt=(\d{4}-\d{2}-\d{2})", 1))
        # Content identity: "only new lines" means a row whose source values we've not seen.
        # Point `merge_key` at a real business key (e.g. message_id) if the export has one.
        .withColumn(
            "_row_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in source_cols]), 256),
        )
    )

# COMMAND ----------

def upsert_batch(batch_df, batch_id):
    batch_df = with_metadata(batch_df)

    if MERGE_KEY not in batch_df.columns:
        raise ValueError(
            f"merge_key '{MERGE_KEY}' is not a column in the source. Available: "
            f"{sorted(batch_df.columns)}"
        )

    # Dedup within the batch: with an insert-only MERGE, two identical source rows would
    # both be inserted, since neither matches the target at plan time.
    batch_df = batch_df.dropDuplicates([MERGE_KEY]).persist()  # 3 actions below
    try:
        if not spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{TABLE}"):
            batch_df.limit(0).write.format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
            # The merge predicate is a hash, so min/max file stats prune almost nothing.
            # Cluster on it or the daily MERGE degrades into a full-table scan as bronze grows.
            spark.sql(f"ALTER TABLE {FQN} CLUSTER BY (`{MERGE_KEY}`)")

        target = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.{TABLE}")
        (
            target.alias("t")
            .merge(batch_df.alias("s"), f"t.`{MERGE_KEY}` = s.`{MERGE_KEY}`")
            .whenNotMatchedInsertAll()
            # No whenMatchedUpdate: bronze rows are immutable once landed.
            # No whenNotMatchedBySourceDelete: rows aged out of S3 must survive in bronze.
            .execute()
        )
        print(f"batch {batch_id}: merged {batch_df.count()} candidate rows")
    finally:
        batch_df.unpersist()


query = (
    reader.writeStream.foreachBatch(upsert_batch)
    .option("checkpointLocation", CHECKPOINT_ROOT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()  # availableNow still runs async; the job must block here
print("stream finished:", query.lastProgress)

# COMMAND ----------

# MAGIC %md ## 4. Post-load verification

# COMMAND ----------

# On a first-ever run where no partition existed, no batch ran and the table was never
# created. Say so plainly rather than failing with TABLE_OR_VIEW_NOT_FOUND.
if not spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{TABLE}"):
    dbutils.notebook.exit(
        f"No data loaded and {FQN} does not exist yet — no source partitions were found "
        f"in the last {LOOKBACK_DAYS} days under {S3_ROOT}."
    )

summary = spark.sql(f"""
    SELECT _dt,
           COUNT(*)                       AS rows,
           COUNT(DISTINCT `{MERGE_KEY}`)  AS distinct_keys,
           MAX(_ingest_ts)                AS last_ingest
    FROM {FQN}
    GROUP BY _dt
    ORDER BY _dt DESC
    LIMIT 14
""")
display(summary)

# COMMAND ----------

rescued = spark.sql(f"SELECT COUNT(*) AS n FROM {FQN} WHERE _rescued_data IS NOT NULL").first()["n"]
if rescued:
    print(f"WARNING: {rescued} rows carry _rescued_data — the export schema likely changed.")
    display(spark.sql(f"SELECT _dt, _rescued_data FROM {FQN} WHERE _rescued_data IS NOT NULL LIMIT 20"))
else:
    print("no rescued data — source schema matches the table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Staging retention
# MAGIC
# MAGIC Decrypted plaintext is kept only as long as the lookback window needs it. Bronze is
# MAGIC the durable copy; this only prunes the staging volume.

# COMMAND ----------

cutoff = _dt.date.today() - _dt.timedelta(days=LOOKBACK_DAYS)
for entry in dbutils.fs.ls(STAGING_ROOT):
    name = entry.name.rstrip("/")
    if not name.startswith("dt="):
        continue
    try:
        staged_date = _dt.date.fromisoformat(name[3:])
    except ValueError:
        continue
    if staged_date < cutoff:
        print(f"pruning staged partition {name}")
        dbutils.fs.rm(entry.path, recurse=True)
