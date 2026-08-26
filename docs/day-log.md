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

## Day 2 — 2026-08-27: Inject realistic messiness + freeze a demo batch

**Goal:** add the five exception categories from the plan on top of the clean
Day 1 data, spot-check that the ground-truth labels are actually correct, and
freeze a fixed 100–200 row batch as the reproducible demo fixture.

### Exception injection (`notebooks/01_generate_synthetic_data.py`)

Every order now gets exactly one label, assigned to disjoint seeded-random
subsets before settlement lines are generated:

- `timing_lag_refund` (4%) — order has a refund; `settlement_report` doesn't
  net it this batch (`refund_adjustment=0` even though `orders.refund_amount`
  is set). Forces a refund onto the order if the natural 15% refund roll
  didn't already give it one.
- `mdr_rate_mismatch` (2.5%) — MDR charged at `expected_mdr_rate + 0.001`
  (e.g. 2.1% instead of 2%) instead of the agreed rate.
- `duplicate_transaction` (1.5%) — a settlement line is appended as a literal
  copy of an existing one (same `transaction_id`), after the base
  `settlement_report` is built.
- `missing_payout` (1.5%) — the order's settlement line is skipped entirely.
- Everything else (~90.7% at the canonical N=150) — `clean_match`.

A new `ground_truth` table (order_id, exception_type, expected_reasoning,
related_transaction_id) is the answer key — one row per order, including the
clean ones. It's explicitly *not* something a reconciliation engine gets to
see; it exists to grade that engine's accuracy later. `expected_reasoning`
is a short human-readable explanation per row (e.g. "Refund of 823.06 issued
on 2026-01-06 was not netted in this settlement batch; expected in the next
cycle.") — the same style of reasoning SettleTrace's own agent should
eventually produce.

`bank_feed` generation now runs *after* exception injection, off the
(possibly messy) `settlement_report` — a duplicate or missing line changes
what the bank actually credits for that batch, same as production.

### Bug found and fixed: `uuid.uuid4()` ignores the seed

First implementation kept `uuid.uuid4()` for `order_id`/`transaction_id`.
Ran the export script twice and diffed checksums — `orders.csv`,
`settlement_report.csv`, and `ground_truth.csv` changed between runs despite
the fixed seed; only `bank_feed.csv` (which has no UUID columns) stayed
identical. Cause: `uuid.uuid4()` draws from `os.urandom()`, not Python's
`random` module, so `random.seed(SEED)` never touched it — silently breaking
the "reproducible, not regenerated randomly" requirement.

Fixed with a `new_id()` helper: `uuid.UUID(int=random.getrandbits(128),
version=4)`, which derives a UUID4 from the seeded `random` module instead.
Reran twice and confirmed identical checksums across all four CSVs before
moving on — this is the kind of bug that would have silently invalidated the
whole "fixed demo batch" premise if it had shipped.

### Validation — replacing "spot-check ~10 rows" with something stronger

Rather than only eyeballing rows, the validation cell programmatically checks
every ground_truth row against the raw data it claims to describe (e.g. a row
labeled `duplicate_transaction` must have exactly 2 settlement rows sharing a
`transaction_id`; `missing_payout` must have 0; `mdr_rate_mismatch` must have
a fee that doesn't match `orders.expected_mdr_rate`), plus a category-count
check and the existing bank_feed-sum check from Day 1. It then prints ~2
sample rows per category. Manually read through the printed samples and the
exported `ground_truth.csv` — labels and reasoning text matched the underlying
numbers in every case checked.

### Demo batch (`scripts/export_demo_batch.py`, `data/demo_batch/*.csv`)

Added a committed export script that runs the notebook's own generation code
locally (same `dbutils`/`spark`/`display` stubbing trick as the Day 1 dry-run
harness) and writes `orders.csv`, `settlement_report.csv`, `bank_feed.csv`,
`ground_truth.csv` to `data/demo_batch/`. This is the single source of truth
for generation logic — the export script doesn't reimplement it, just exec's
the notebook file. Canonical parameters: `num_orders=150`, `seed=42` (150 is
inside the requested 100–200 row range; label distribution at this size is
136 clean / 6 timing-lag / 4 MDR-mismatch / 2 duplicate / 2 missing-payout —
90.7% clean, matching the "90%+ clean" target).

Confirmed reproducibility directly: ran the export script twice back-to-back
and diffed `md5sum` of all four CSVs — identical both times (after the uuid
fix above).

### Not yet verified

Same caveat as Day 1: everything above was verified by exec'ing the notebook
logic locally with fake Spark/Databricks stubs (no local JDK available), not
by an actual run on a live Databricks cluster. The Delta-write path and
`SHOW SCHEMAS`/catalog-fallback logic still need a real run once Databricks
auth + the Git folder link are set up.

### Files touched

- `notebooks/01_generate_synthetic_data.py` (exception injection, `ground_truth`
  table, `new_id()` determinism fix, widened widgets, revised validation cell)
- `scripts/export_demo_batch.py` (new)
- `data/demo_batch/orders.csv`, `settlement_report.csv`, `bank_feed.csv`,
  `ground_truth.csv` (new — committed, frozen demo batch)
- `README.md` (Day 2 status, widget table, demo-batch section)
- `docs/day-log.md` (this section)
