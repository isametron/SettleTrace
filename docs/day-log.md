# SettleTrace — Day Log

## Day 1 — 2026-08-26: Repo setup + synthetic data generator

**Goal:** get the repo/env/Databricks connection working, and produce clean,
linked synthetic `orders` / `settlement_report` / `bank_feed` data at any scale.
No matching logic, no injected exceptions yet.

### Tooling installed

None of `uv`, Python, or the Databricks CLI were present on this machine, so:

- `uv` `0.12.5` via winget (`astral-sh.uv`)
- Databricks CLI `1.13.0` via winget (`Databricks.DatabricksCLI`)
- Python `3.12.14` via `uv python install 3.12` (uv-managed, not a system install)

Note: winget/uv add PATH entries via a registry update that only takes effect
in a new shell — this session's shell was already running, so tools were
invoked via their full install paths under `%LOCALAPPDATA%\Microsoft\WinGet\Packages\...`.
**A fresh terminal should pick up `uv`, `uvx`, and `databricks` on PATH normally** —
worth confirming next session.

### Repo scaffold

- `uv init` to create `pyproject.toml` / `.python-version` / `uv.lock`, then
  stripped the default installable-package scaffolding (`src/settletrace/`,
  `[project.scripts]`, `[build-system]`) since nothing imports this project as
  a library — set `[tool.uv] package = false` instead.
- Dependencies: `pyspark`, `faker` (runtime); `ruff` (dev group).
- `.gitignore` for `.venv/`, `__pycache__/`, `.databricks/`, etc.
- `uv sync` verified clean (resolves pyspark 4.2.0 locally — this is newer
  than most Databricks Runtime versions ship, but it's only used for local
  authoring/linting, not execution; the notebook itself runs on the cluster's
  own Spark).

### `notebooks/01_generate_synthetic_data.py`

Written as a real Databricks notebook (`# Databricks notebook source` +
`# COMMAND ----------` cells, `# MAGIC %md` docs), per the chosen format —
not a plain importable module.

Widgets: `num_orders` (default 50), `seed`, `settlement_batch_size` (25),
`catalog` (defaults to `hive_metastore`), `schema_name` (`settletrace`).

Generation logic:
- `orders`: laid out `settlement_batch_size` per day so date-grouping lines up
  with settlement batches; ~15% get a partial/full refund 1–3 days later.
- `settlement_report`: one line per order — `mdr_fee` at a fixed 2% rate,
  `gst_on_mdr` at 18%, refund netted out, `net_amount` computed; all orders in
  a batch share one `utr_number` / `settlement_batch_id`.
- `bank_feed`: one row per batch — `bank_credit_amount` = sum of that batch's
  `net_amount`, landing T+2 after the batch's order date.
- Inline validation cell (asserts, not a separate test suite, since this lives
  in a notebook): every settlement line maps to a real order 1:1, no
  duplicate/orphaned order_ids, refunds never exceed order amount, every
  bank credit sums exactly to its batch's net settlement amount.
- Catalog handling: tries `SHOW SCHEMAS IN <catalog>`, falls back to
  `hive_metastore` if that fails (Unity Catalog availability wasn't confirmed
  yet — see Open Items).
- Writes three Delta tables via `saveAsTable`, overwrite mode (idempotent
  re-runs at a new scale).

### Verification done today

- **Local dry-run** (`uv run python <scratchpad>/dryrun_notebook.py`): stubbed
  `dbutils`/`spark`/`display` with lightweight fakes (no local JDK, so a real
  local Spark session wasn't an option) and exec'd the notebook source
  directly. Ran at `num_orders=50` and `num_orders=500` — validation
  assertions passed both times, table row counts matched (`orders`=N,
  `settlement_report`=N, `bank_feed`=N/batch_size batches).
- `ruff check` clean on the notebook, after adding a `per-file-ignores` rule
  for `notebooks/*.py` (F821 for Databricks-injected `dbutils`/`spark`/`display`
  globals; BLE001 for the intentionally-broad catalog-fallback `except`).

**Not yet verified:** an actual run on a live Databricks cluster (Delta
writes, real Unity-Catalog-vs-hive_metastore behavior) — the dry-run only
proves the generation/validation arithmetic and linkage logic, not the
Spark/Delta write path itself.

### Open items / not done today

- Databricks CLI is installed but **not authenticated** — `databricks auth
  login --host <workspace-url>` needs an interactive browser login the user
  has to run themselves.
- GitHub repo not yet connected to the workspace as a Git folder (Repos).
- Catalog/schema naming unconfirmed — user wasn't sure if Unity Catalog is
  enabled on their workspace; notebook defaults to `hive_metastore` with a
  fallback check, to be confirmed once connected.
- No noise/exceptions injected into the synthetic data yet (missing payouts,
  fee-rate mismatches, timing lags, duplicate settlements) — planned for a
  later day, once the clean-data path is confirmed working end-to-end on
  Databricks.
- No matching/reconciliation engine yet — that's the actual core of the
  project and hasn't been started.

### Files touched

- `pyproject.toml`, `uv.lock`, `.python-version` (new)
- `.gitignore` (new)
- `notebooks/01_generate_synthetic_data.py` (new)
- `README.md` (expanded with setup/run instructions)
- `docs/day-log.md` (this file, new)
