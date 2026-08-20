---
name: databricks
description: "Use this skill when writing, debugging, or remotely testing Databricks notebooks and jobs -- Unity Catalog, serverless compute restrictions, UC Volumes I/O, Auto Loader, Delta MERGE, and the Databricks CLI/Jobs API. Covers real gotchas confirmed against a live workspace: file:/dbfs: scheme traps, read-only external locations, serverless caching restrictions, ambiguous-join errors, and Jobs API result retrieval. Common phrases: \"databricks notebook\", \"unity catalog\", \"auto loader\", \"databricks job failing\", \"serverless compute error\", \"push notebook to databricks\". Do NOT use for general PySpark DataFrame code outside Databricks (use python-data-engineering) or dbt model authoring (use dbt-transforms)."
model:
  preferred: sonnet
  acceptable: [sonnet, opus]
  minimum: sonnet
  allow_downgrade: false
  reasoning_demand: medium
version: 1.0.0
---

# Databricks Skill for Claude

Writing, debugging, and remotely testing Databricks notebooks and jobs — Unity Catalog, serverless compute, Auto Loader, Delta MERGE, and the Jobs API.

## When to Use This Skill

**Activate when:** writing a Databricks notebook, debugging a Databricks job failure, working with Unity Catalog (catalogs/schemas/Volumes/external locations), Auto Loader/`cloudFiles`, Delta MERGE, or setting up remote push-and-run testing against a live Databricks workspace via the CLI/Jobs API.

**Don't use for:** general PySpark code outside Databricks (use python-data-engineering), dbt model authoring even on a Databricks SQL warehouse (use dbt-transforms), pipeline scheduling/orchestration (use data-pipelines) — this skill covers what happens *inside* a Databricks job, not what triggers it.

## Scope Constraints

- Databricks-specific: Unity Catalog, Volumes, Auto Loader, Delta, serverless compute, the Databricks CLI/Jobs API.
- Does not provision cloud IAM/storage — assumes a Unity Catalog external location or Volume already exists; escalate storage/credential provisioning to the user.
- Credentials (PATs, service principal tokens) follow the convention in [references/remote-test-loop.md](references/remote-test-loop.md): a local file outside any git repo, `chmod 600`, sourced into a subshell, never typed into a chat transcript or committed. # SECURITY: this describes a handling convention, not a credential.
- Reference files are one level deep and loaded on demand — don't pre-load all three for a task that only touches one topic.

## Core Gotchas (confirmed against a live workspace)

None of these are documented clearly by Databricks or guessable from source alone — each cost a full push-run-fail cycle to discover in practice. Quick reference; full detail and fixes in [references/serverless-and-uc-gotchas.md](references/serverless-and-uc-gotchas.md).

| Symptom | Cause | Fix |
|---|---|---|
| `LocalFilesystemAccessDeniedException: Cannot access non /Workspace local filesystem path: file:/Volumes/...` | `dbutils.fs.cp(src, f"file:{path}")` — the `file:` scheme only allows `/Workspace`, even for a `/Volumes` destination | Drop the `file:` prefix; `/Volumes` paths are natively understood by `dbutils.fs` |
| Same exception with a `dbfs:` prefix | `dbfs:` addresses the legacy DBFS root, not Volumes | Drop the `dbfs:` prefix too |
| `UNAUTHORIZED_ACCESS: PERMISSION_DENIED ... read-only external location` | The S3/ADLS location is registered read-only *in Unity Catalog*, independent of the underlying cloud IAM policy | Don't assume IAM write access implies UC write access — checkpoint/write to a Volume you've already proven writable instead |
| `NOT_SUPPORTED_WITH_SERVERLESS` on `.persist()` / `.cache()` | Serverless compute doesn't support DataFrame/table caching | Drop `.persist()`; accept recomputation, or move to classic compute if caching is load-bearing |
| `AMBIGUOUS_REFERENCE: Reference X is ambiguous, could be: [X, X]` after a join | An explicit-condition join (`df1.join(df2, df1["x"] == df2["y"])`) keeps both sides' columns even when same-named | Alias one side's column before joining — never rely on the join to dedupe |
| Cluster ID resolves to `"kind": "SERVERLESS_REPL_VM"` tied to a `NotebookId` | The workspace runs serverless notebooks, not a named all-purpose cluster | Submit jobs with no `existing_cluster_id` / `new_cluster` at all — serverless job compute is assigned automatically |

## Remote Testing Loop

For iterating against a real workspace instead of guessing from docs: push the notebook via `databricks workspace import`, submit via `databricks jobs submit --no-wait`, poll `jobs get-run` (both `TERMINATED` *and* `INTERNAL_ERROR` are terminal states), pull the real traceback via `jobs get-run-output`. This closes the loop with actual execution — several of the gotchas above were only found this way, not by reading Databricks docs. Full script, the credential-file convention, PAT scoping, and CLI quirks (`--no-wait` is required to get `run_id` back on a run that fails, `print()`/`display()` output isn't captured by the API) in [references/remote-test-loop.md](references/remote-test-loop.md).

## Age-Encrypted S3 → Bronze Delta Pattern

For ingesting age-encrypted CSV/SQL exports (backup sidecars, cross-system dumps) into a Databricks bronze table: driver-side decryption before Spark ever sees the file, Volume staging, the two Auto Loader CSV options that are load-bearing rather than tuning (`multiLine`, `escape`), and full-snapshot-vs-incremental MERGE strategy. Detail in [references/age-encrypted-csv-ingest.md](references/age-encrypted-csv-ingest.md).

## Related Skills

- **data-pipelines** — scheduling/orchestrating the job this skill helps you write (e.g. Airflow triggering a Databricks job).
- **python-data-engineering** — PySpark DataFrame patterns outside the Databricks-specific concerns here.
- **dbt-transforms** — dbt models targeting a Databricks SQL warehouse.
