## Contents

- [Why This Reference Exists](#why-this-reference-exists)
- [1. `file:` scheme on a Volumes path](#1-file-scheme-on-a-volumes-path)
- [2. `dbfs:` scheme on a Volumes path](#2-dbfs-scheme-on-a-volumes-path)
- [3. Driver-local filesystem outside `/Workspace` and `/Volumes`](#3-driver-local-filesystem-outside-workspace-and-volumes)
- [4. Read-only external location in Unity Catalog](#4-read-only-external-location-in-unity-catalog)
- [5. `.persist()` not supported on serverless](#5-persist-not-supported-on-serverless)
- [6. Ambiguous column after an explicit-condition join](#6-ambiguous-column-after-an-explicit-condition-join)
- [7. Serverless notebooks have no named cluster](#7-serverless-notebooks-have-no-named-cluster)
- [8. Catalog name isn't always `main`](#8-catalog-name-isnt-always-main)

---

# Serverless Compute & Unity Catalog Gotchas

> **Part of:** [databricks](../SKILL.md)

Every item below was confirmed against a real, live Databricks workspace during iterative debugging — not inferred from documentation. Databricks' own error messages are usually precise once you know what they mean; the fixes here are what they mean.

## Why This Reference Exists

A notebook that only imports cleanly and passes `py_compile` has told you nothing about whether it will actually run on Databricks. Unity Catalog and serverless compute each impose restrictions that look like generic permission or API errors, but have specific, narrow causes and one-line fixes. Recognize the error text below and skip straight to the fix instead of re-deriving it.

## 1. `file:` scheme on a Volumes path

**Error:**
```
com.databricks.backend.daemon.driver.LocalFilesystemAccessDeniedException
Cannot access non /Workspace local filesystem path: file:/Volumes/<catalog>/<schema>/<volume>/...
```

**Cause:** `dbutils.fs.cp(src, f"file:{local_path}")` where `local_path` is a `/Volumes/...` path. The `"file:"` scheme routes the call through a driver-local filesystem enforcement layer (`SharedUCWorkspaceLocalFileSystem`) that only allows `/Workspace` — even though the destination is a perfectly valid, writable Volume path.

**Fix:** Drop the scheme prefix entirely. `/Volumes/...` paths are natively understood by `dbutils.fs` — no `file:` needed, and no plain Python `open()`/`os` call needs it either, since Volumes are POSIX-mounted directly.

```python
# Wrong
dbutils.fs.cp(src, f"file:{local}")

# Right
dbutils.fs.cp(src, local)
```

## 2. `dbfs:` scheme on a Volumes path

**Error:** same `LocalFilesystemAccessDeniedException` shape, or a silent write to the wrong location.

**Cause:** `dbfs:` addresses the legacy DBFS root, not Unity Catalog Volumes. Using it on a `/Volumes/...` path is simply the wrong scheme.

**Fix:** Same as above — no prefix at all for a `/Volumes/...` path, on any `dbutils.fs.*` call (`cp`, `mv`, `rm`, `mkdirs`, `ls`).

## 3. Driver-local filesystem outside `/Workspace` and `/Volumes`

**Error:**
```
LocalFilesystemAccessDeniedException: Cannot access non /Workspace local filesystem path: file:/tmp/...
```

**Cause:** On Unity Catalog shared/standard-access compute (and on serverless), the driver's local disk is not freely writable the way it is on a classic cluster with no access-mode restriction. `/tmp` in particular is blocked.

**Fix:** Stage any scratch files (e.g. an encrypted download before decrypt) under a UC Volume path instead of `/tmp`. Create a dedicated subpath, e.g. `{volume_root}/_tmp`, and write there with plain Python `open()`/`os` calls (no scheme prefix — see #1).

## 4. Read-only external location in Unity Catalog

**Error:**
```
[UNAUTHORIZED_ACCESS] Unauthorized access:
PERMISSION_DENIED: User cannot write to a read-only external location <location-name>
SQLSTATE: 42501
```

**Cause:** Unity Catalog external locations carry their own read/write grant, layered *on top of* the underlying cloud IAM policy. A bucket whose IAM policy technically allows `PutObject` can still be registered read-only at the UC governance layer for a given principal. This is easy to miss if you're inferring writability from a sibling script's default path or from the bucket's IAM policy alone — neither actually proves the UC grant exists.

**Fix:** Don't assume a path is writable because IAM allows it, or because another notebook's *unexecuted* default happens to point there. Verify by execution, or point writes (streaming checkpoints especially) at a Volume you've already proven writable — e.g. one where `CREATE VOLUME IF NOT EXISTS` and a subsequent file write both succeeded in the same run.

## 5. `.persist()` not supported on serverless

**Error:**
```
pyspark.errors.exceptions.connect.AnalysisException:
[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute.
SQLSTATE: 0A000
```

**Cause:** Serverless compute (the default in many newer workspaces) does not support DataFrame/table caching via `.persist()`/`.cache()`, unlike classic all-purpose or job clusters.

**Fix:** Drop the `.persist()`/`.unpersist()` calls. If avoiding recomputation genuinely matters (e.g. a `foreachBatch` action list of 3+ actions over a large batch), that's a signal to move the workload to classic compute rather than work around the restriction — don't try to fake caching with a `write` + `read` round-trip inside a streaming batch.

## 6. Ambiguous column after an explicit-condition join

**Error:**
```
pyspark.errors.exceptions.connect.AnalysisException:
[AMBIGUOUS_REFERENCE] Reference `x` is ambiguous, could be: [`x`, `x`]. SQLSTATE: 42704
```

**Cause:** `df1.join(df2, df1["x"] == df2["x"])` — an explicit boolean join condition, as opposed to the string-column-list form (`df1.join(df2, "x")`) — does **not** deduplicate same-named columns from each side. The result carries two columns both named `x`; any later unqualified reference to `x` is ambiguous, and it isn't always caught until a downstream action forces evaluation, so the failure can appear several lines away from the actual join.

**Fix:** Alias the column on at least one side before joining, and drop the duplicate after:

```python
# Wrong — both sides end up with a column named _dt
other = df.groupBy("k").agg(F.max("_dt").alias("_dt"))
joined = df.join(other, (df["k"] == other["k"]) & (df["_dt"] == other["_dt"]))

# Right
other = df.groupBy("k").agg(F.max("_dt").alias("_newest_dt")).withColumnRenamed("k", "_k")
joined = df.join(other, (df["k"] == other["_k"]) & (df["_dt"] == other["_newest_dt"])) \
           .drop("_k", "_newest_dt")
```

## 7. Serverless notebooks have no named cluster

**Symptom:** A "cluster ID" pulled from an open notebook's URL resolves via `databricks clusters get <id>` to something like:
```json
{"cluster_source": "REPL", "kind": "SERVERLESS_REPL_VM", "custom_tags": {"NotebookId": "..."}}
```

**Cause:** The workspace runs Databricks serverless notebooks. What looks like a cluster ID in the URL is actually an ephemeral REPL VM tied to that one open notebook session — not a shareable all-purpose cluster, and not a valid `existing_cluster_id` target for the Jobs API.

**Fix:** Submit jobs with no cluster spec at all (no `existing_cluster_id`, no `new_cluster`) — Databricks assigns serverless job compute automatically. This is simpler than the classic-cluster flow, not a workaround.

## 8. Catalog name isn't always `main`

**Symptom:** `CREATE SCHEMA IF NOT EXISTS main.<schema>` fails: `Catalog 'main' was not found.`

**Cause:** `main` is a common default but not guaranteed — some workspaces never provision it, or use a workspace-scoped catalog (commonly literally named `workspace`) instead.

**Fix:** Make the catalog a notebook widget with no hardcoded assumption of `main`; have the user confirm via `SHOW CATALOGS` before the first run rather than guessing.
