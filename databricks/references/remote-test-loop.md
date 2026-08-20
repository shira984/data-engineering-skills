## Contents

- [When to Use This Instead of Guessing](#when-to-use-this-instead-of-guessing)
- [Credential Handling](#credential-handling)
- [PAT Scope Selection](#pat-scope-selection)
- [The Loop: Push, Submit, Poll, Fetch](#the-loop-push-submit-poll-fetch)
- [CLI Quirks That Cost a Cycle Each](#cli-quirks-that-cost-a-cycle-each)
- [Cheap Probes Before the Real Notebook](#cheap-probes-before-the-real-notebook)
- [Cleanup](#cleanup)

---

# Remote Testing Loop (Databricks CLI + Jobs API)

> **Part of:** [databricks](../SKILL.md)

A notebook that passes local syntax checks has not been tested. Age/encryption logic, CSV parsing assumptions, and pure-Python helpers can be verified locally; Unity Catalog grants, serverless compute restrictions, and Auto Loader/Delta behavior cannot — they only exist inside a live workspace. This reference is the loop that closes that gap: push the real file, run it for real, read the real traceback back.

## When to Use This Instead of Guessing

Any time a notebook touches: `dbutils.fs`, Auto Loader, a streaming `foreachBatch`/MERGE, Unity Catalog Volumes, or anything with an access-mode-dependent restriction. Local `py_compile` and unit-testable pure-Python helpers (e.g. a decrypt function) are still worth doing first — they catch real bugs cheaply and narrow what's left to test remotely.

## Credential Handling

A Databricks PAT (or service-principal token) must never be typed into a chat transcript or committed to a repo. # SECURITY: the pattern below exists specifically to keep the token out of both.

1. The user generates the token themselves, in the Databricks UI, and creates a local credentials file **in a separate terminal, not through the agent**:
   ```bash
   mkdir -p ~/.databricks
   cat > ~/.databricks/<project>.env <<'EOF'
   DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
   DATABRICKS_TOKEN=<the PAT>
   DATABRICKS_CLUSTER_ID=<only if the workspace uses classic compute -- see gotcha #7>
   EOF
   chmod 600 ~/.databricks/<project>.env
   ```
2. The agent verifies structure without ever printing the token: file permissions (`ls -l`), line count (`wc -l`), and only the non-secret keys/values (host, cluster ID) via `grep '^DATABRICKS_HOST='` etc. — never `cat` the whole file, never `grep '^DATABRICKS_TOKEN='`.
3. Every CLI call sources the file into a subshell, scoped to that call only:
   ```bash
   bash -c '
   set -a
   source ~/.databricks/<project>.env
   set +a
   databricks <command>
   '
   ```
4. This file lives outside any git repository. If the project directory is itself a repo, keep the credentials file in `$HOME`, never inside the repo tree, even gitignored.

This is a throwaway, human-tied credential for interactive debugging — short lifetime (7–14 days), deleted when the debugging session ends. It is the wrong credential for production automation (see the Related Skills note in the main SKILL.md about handing off to data-pipelines/Airflow, which should use a service principal in its own secrets backend instead).

## PAT Scope Selection

If the workspace supports scoped tokens, select narrowly:
- **`jobs`** — required, submits runs and fetches output.
- **`workspace`** — required, imports/exports notebook files.
- **`clusters`** — optional, only useful for checking cluster state on classic-compute workspaces.
- Leave everything else unchecked. Never select "all APIs."

## The Loop: Push, Submit, Poll, Fetch

```bash
# 1. Push the current file
databricks workspace import \
  /Users/<you>/claude-test/<name> \
  --file /local/path/notebook.py \
  --language PYTHON --format SOURCE --overwrite

# 2. Submit without blocking -- see quirk below on why --no-wait matters
databricks jobs submit --no-wait --json "{
  \"run_name\": \"claude-test\",
  \"tasks\": [{
    \"task_key\": \"t\",
    \"notebook_task\": {
      \"notebook_path\": \"/Users/<you>/claude-test/<name>\",
      \"base_parameters\": {\"widget_key\": \"widget_value\"}
    }
  }]
}"
# -> {"run_id": 123456789}

# 3. Poll -- INTERNAL_ERROR is ALSO terminal, not just TERMINATED/SKIPPED
for i in $(seq 1 40); do
  STATE=$(databricks jobs get-run 123456789 | python3 -c \
    'import json,sys; print(json.load(sys.stdin)["state"]["life_cycle_state"])')
  case "$STATE" in TERMINATED|INTERNAL_ERROR|SKIPPED) break ;; esac
  sleep 10
done

# 4. Get the task-level run_id (not the parent run_id) from jobs get-run,
#    then fetch the real traceback
databricks jobs get-run-output <task_run_id>
# -> {"error": "...", "error_trace": "...", "notebook_output": {...}}
```

Long-running polls: a poll loop that runs past ~120 seconds may get moved to a background task by the harness — that is fine, wait for its completion notification rather than manually re-polling in the foreground.

## CLI Quirks That Cost a Cycle Each

- **`databricks jobs submit` without `--no-wait` blocks and, on failure, prints only an error message to stderr — no `run_id`.** Without the run_id you cannot call `get-run-output` at all. Always use `--no-wait` for a debug loop; poll separately.
- **`INTERNAL_ERROR` is a terminal `life_cycle_state`,** distinct from `TERMINATED`. A poll loop that only checks for `TERMINATED`/`SKIPPED` will spin until its own timeout on a job that has, in fact, already failed.
- **A failed run may show two task entries** (`attempt_number: 0` and `1`) if the platform auto-retried. Use the highest `attempt_number`'s `run_id` for `get-run-output`.
- **Plain `print()`/`display()` output is not captured by `get-run-output`.** Only `dbutils.notebook.exit(...)`'s return value (success path) or the `error`/`error_trace` fields (failure path) come back through the API. For a debug loop this is usually fine — failures are what you're chasing — but if you need programmatic visibility into a *successful* run's results, add an explicit `dbutils.notebook.exit(str(summary_dict))` at the end of the notebook, or run a small separate verification notebook that queries the target table directly and exits with the result.
- **`error_trace` contains ANSI color codes** (`\x1b[...m` or literal `[0;31m` sequences and `<span class='ansi-red-fg'>` HTML). Strip with a regex before printing, or the traceback is unreadable.
- **A `databricks aitools` subcommand exists** on recent CLI versions — it installs Databricks' own official Claude Code skills from `github.com/databricks/databricks-agent-skills`. Worth knowing it exists; not required for this manual push/submit/poll/fetch loop, since that talks to the Jobs/Workspace APIs directly.

## Cheap Probes Before the Real Notebook

Before running the actual target notebook, submit two throwaway one-liners through the same loop:
1. A trivial success case (`print("ok")`) — confirms auth, workspace path, and that jobs actually execute.
2. A trivial failure case (`raise ValueError("probe")`) — confirms the traceback-retrieval path actually surfaces a real Python exception, not just a platform-level error.

This costs under a minute and catches loop-plumbing bugs before they get confused with bugs in the real notebook.

## Cleanup

Delete throwaway probes and test notebooks/jobs from the workspace once the real notebook is confirmed working:
```bash
databricks workspace delete /Users/<you>/claude-test --recursive
databricks jobs delete <job_id>   # for any job created via a *named* create, not one-off submit-run
```
One-off `jobs submit` runs don't need explicit deletion — they aren't saved as jobs and don't appear in the Jobs UI (per the CLI's own `submit --help` text).
