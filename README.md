# SettleTrace

A settlement reconciliation agent that explains its exceptions instead of just flagging them.

It unpacks a lumped settlement (one bank credit netting hundreds of orders against MDR
fees, GST-on-MDR, and refunds) back to order-level, classifies each line as an exact
match, a fuzzy match, or an exception — and reasons about *why* on the exceptions
instead of just flagging them.

## Status

Day 1: repo + environment setup, and a synthetic data generator producing clean,
linked `orders` / `settlement_report` / `bank_feed` data at any scale.

Day 2: injected realistic messiness (timing-lag refunds, MDR rate mismatches,
duplicate transactions, missing payouts) on top of the clean batch, plus a
`ground_truth` answer-key table, and froze a reproducible 150-row demo batch
under `data/demo_batch/`.

Day 3: a 3-tier reconciliation engine (`notebooks/02_reconcile_settlements.py`)
that reads only `orders`/`settlement_report`/`bank_feed`, classifies every
order as `exact` / `fuzzy` / `no_match` plus a specific category, and writes
`reconciliation_result`. Scores 100% against `ground_truth` on the demo batch.

## Stack

- Databricks + PySpark for batch reconciliation
- `uv` for local Python env/dependency management
- Prophet, later, for a cash-forecasting angle

## Local setup

```
uv sync
```

This installs `pyspark` and `faker` locally, purely so notebook cell logic can be
authored and sanity-checked before running on the real Databricks cluster (the
local `pyspark` version does not need to match the cluster's Databricks Runtime
version — it's a dev convenience, not an execution target). A full local Spark
session additionally needs a JDK (Temurin 17 here) plus `JAVA_HOME`/`PYSPARK_PYTHON`
env vars pointed at it — see `scripts/test_reconcile_local.py` for the exact
setup and a runnable local test of the reconciliation notebook.

## Databricks setup

1. Install the Databricks CLI (done via winget: `Databricks.DatabricksCLI`).
2. Authenticate: `databricks auth login --host <your-workspace-url>` (opens a
   browser for OAuth login).
3. In the Databricks workspace, connect this GitHub repo
   (`isametron/SettleTrace`) as a **Git folder** (Repos), so pulling inside the
   workspace picks up notebook changes pushed from here.
4. Open `notebooks/01_generate_synthetic_data.py` in the workspace — it should
   render as a notebook with separate cells.

## Generating synthetic data

Run `notebooks/01_generate_synthetic_data.py` on a Databricks cluster. Widgets:

| Widget                | Default          | Meaning                                                          |
|-----------------------|------------------|-------------------------------------------------------------------|
| `num_orders`           | `150`            | Number of orders to generate                                      |
| `seed`                 | `42`             | RNG seed — same seed + num_orders always reproduces the same batch |
| `settlement_batch_size`| `25`             | Orders per lumped settlement batch                                 |
| `catalog`              | `workspace`      | Target catalog (falls back here if the requested one isn't usable) |
| `schema_name`          | `settletrace`    | Schema the tables are written under                                |
| `timing_lag_rate`      | `0.04`           | Fraction of orders with a refund not yet netted this batch         |
| `mdr_mismatch_rate`    | `0.025`          | Fraction of orders charged the wrong MDR rate                      |
| `duplicate_rate`       | `0.015`          | Fraction of orders with a duplicated settlement line                |
| `missing_payout_rate`  | `0.015`          | Fraction of orders with no settlement line at all                   |

It writes `orders`, `settlement_report`, `bank_feed`, and `ground_truth` as Delta
tables under `<catalog>.<schema_name>`, after an inline validation cell checks
every exception category against the raw data (not just its own label) and
prints a sample of each category for a manual spot-check.

`ground_truth` is the answer key — one row per order labeling it `clean_match`
or one of the four exception types, with a short explanation. It's what a real
reconciliation engine would need to reconstruct on its own; it exists here only
to measure that engine's accuracy later.

## Reconciling settlements

Run `notebooks/02_reconcile_settlements.py` on a Databricks cluster, after
`01_generate_synthetic_data.py` has populated the tables. Widgets:

| Widget         | Default       | Meaning                                              |
|----------------|---------------|-------------------------------------------------------|
| `catalog`      | `workspace`   | Catalog to read/write tables in                        |
| `schema_name`  | `settletrace` | Schema the tables live under                           |
| `tolerance`    | `0.01`        | Absolute tolerance for comparing recomputed vs. actual amounts |

It reads only `orders` / `settlement_report` / `bank_feed` — never
`ground_truth` — and recomputes what each order's settlement line *should*
look like, comparing that against what `settlement_report` actually shows to
classify every order:

- **exact** — `clean_match`: everything ties out.
- **fuzzy** — `mdr_rate_mismatch` or `timing_lag_refund`: linked correctly,
  one figure doesn't tie out in a recognizable way.
- **no_match** — `missing_payout` or `duplicate_transaction`: can't be
  linked to settlement at all, structurally.

It also cross-checks `bank_feed` against summed `settlement_report.net_amount`
per batch — a second, independent leg of reconciliation. Writes
`reconciliation_result` (order-level, with a `reasoning` string per row) and
prints match-rate / exception-count summaries. The last section of the
notebook reads `ground_truth` purely to grade the classification (100%
accuracy on the demo batch) — that's test-only, not part of the engine.

## Demo batch

`data/demo_batch/*.csv` is a frozen, checked-in export of the canonical run
(`num_orders=150`, `seed=42`) — the fixed dataset the demo/reconciliation work
should build against, so results don't change just because someone reran the
generator. Regenerate it with:

```
uv run python scripts/export_demo_batch.py
```

This runs the notebook's own generation logic locally (stubbing the
Databricks-injected `dbutils`/`spark`/`display`), so there's one source of
truth for the generation logic — it isn't duplicated between the notebook and
the export script.
