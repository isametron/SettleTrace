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

Day 4 (part 1): a prototype LLM reasoning layer (`scripts/reason_about_exceptions.py`)
that independently diagnoses cause + explanation + confidence per order,
without seeing the rule engine's own category. Tested manually on 5 sample
orders against a local model — see the widget/usage section below for the
result and what it means.

Day 5: the reasoning loop is automated across every flagged exception, and the
whole thing runs start to finish: data → match → exception reasoning → **audit
trail**. Every order gets an audit record carrying the engine's verdict and
evidence, the agent's independent diagnosis and recommendation, whether the two
agree, and an explicit `no autonomous action taken` marker.

It runs in two places from one codebase: `notebooks/03_reason_and_audit.py` on
Databricks (writing the `audit_log` Delta table, using a Foundation Model API
serving endpoint), and `scripts/run_pipeline.py` locally for fast iteration.
Both import the same prompt and audit schema, so they cannot drift apart.
See "Full pipeline" below.

## Stack

- Databricks + PySpark for batch reconciliation
- An LLM reasoning layer for exception explanation, running on Databricks
  Foundation Model APIs (`databricks-meta-llama-3-3-70b-instruct` by default),
  with an optional local-model backend for offline iteration
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

## LLM reasoning layer (prototype)

`scripts/reason_about_exceptions.py` feeds one order per category (four
exception types + one clean control) from the demo batch to a local model,
asking it to *independently* diagnose the cause — without being told the
rule engine's own category — and return `cause` / `explanation` /
`confidence` / `recommended_action` as structured JSON.

The prompt, response schema, and case-building all live in
`scripts/reasoning_agent.py`, shared with the automated pipeline — this script
is the small readable harness, not a second copy of the logic.

It targets an OpenAI-compatible local server (tested against Bionic AI
Studio running `qwen2.5-7b-instruct`, default `http://localhost:1234/v1`) —
edit `DEFAULT_BASE_URL` / `DEFAULT_MODEL` in `scripts/reasoning_agent.py` to
point at a different one. Run with:

```
uv run python scripts/reason_about_exceptions.py
```

**Result on the 5 sample orders: 3/5 matched `ground_truth`.** Correctly
diagnosed `mdr_rate_mismatch`, `missing_payout`, and `clean_match`. Missed
`timing_lag_refund` (called it `missing_payout`, despite its own explanation
noting a settlement line existed) and `duplicate_transaction` (called it
`clean_match` even after describing both identical settlement lines in its
own explanation). This is an honest result for a small local model, not a
bug — it validates the prompt design works end-to-end (grounded, schema-valid
reasoning) while showing accuracy on subtler structural cues (like "count the
lines") is a real limitation at this model size. A caveat worth knowing: this
local server did not actually enforce the JSON schema's `confidence` range —
it returned a 0–100 value once during testing despite the schema declaring
0–1 — so the response is normalized defensively in code rather than trusted
outright.

## Reasoning + audit on Databricks (`notebooks/03_reason_and_audit.py`)

The production path. Run it on a cluster after `02_reconcile_settlements.py` has
written `reconciliation_result`. Widgets:

| Widget           | Default                                  | Meaning                                                        |
|------------------|------------------------------------------|----------------------------------------------------------------|
| `catalog`        | `workspace`                              | Catalog to read/write in                                        |
| `schema_name`    | `settletrace`                            | Schema the tables live under                                    |
| `endpoint`       | `databricks-meta-llama-3-3-70b-instruct` | Foundation Model API serving endpoint to reason with            |
| `clean_controls` | `5`                                      | How many clean orders to also diagnose, as controls             |
| `max_flagged`    | `0`                                      | Cap on flagged orders (`0` = all); for cheap smoke tests        |
| `temperature`    | `0.0`                                    | Sampling temperature                                            |

It reads `reconciliation_result` plus the operational tables, sends every
flagged order (and the clean controls) to the endpoint for an independent
diagnosis, and writes the **`audit_log` Delta table** — one row per order. As in
notebook 02, `ground_truth` is touched only in the final, clearly-separated
grading cell.

The model is reached over the OpenAI-compatible protocol at
`<workspace-host>/serving-endpoints`, with auth resolved from the notebook's own
identity — no secrets in the notebook. The prompt, response schema, case-builder
and audit record schema are imported from `scripts/`, so the cluster and the
local runner cannot drift apart.

Verified on the live cluster (`databricks jobs submit`, SUCCESS), then checked
independently by querying the table rather than trusting the notebook's own
print output:

| Check | Result |
|-------|--------|
| `audit_log` rows | 150 |
| Review queue | 14 `needs_human_review`, 136 `auto_cleared` |
| `action_taken` distinct values | `none` (1 distinct value across all 150) |
| `autonomous_action_taken` sum | **0** |
| Engine vs. agent on flagged orders | 14 agree, 0 disagree |
| Agent vs. `ground_truth` (test-only) | **19/19 (100%)** |
| Provenance recorded | `databricks-meta-llama-3-3-70b-instruct`, prompt `e07e929411a5` |

Running a 70B model instead of the 7B local one fixed both Day 5 failure modes:
the `duplicate_transaction` structural miss (wrong on Day 4 *and* on Day 5's
local run) and the `mdr_rate_mismatch` materiality error.

## Local pipeline (data → match → reason → audit)

`scripts/run_pipeline.py` runs the same four stages on a laptop against the
frozen demo batch — useful for iterating without a cluster:

1. **Data** — reads `data/demo_batch/*.csv`.
2. **Match** — executes the real `notebooks/02_reconcile_settlements.py` against
   a local SparkSession (via `scripts/local_spark_harness.py`), so the pipeline
   and Databricks run the same notebook source, not two copies of it.
3. **Reason** — sends every order the engine flagged to the advisory LLM agent
   for an independent diagnosis, plus a deterministic sample of *clean* orders
   as controls, so the agent's false-positive rate is measured rather than
   assumed.
4. **Audit** — writes one record per order to the audit trail.

```
.venv\Scripts\python.exe scripts/run_pipeline.py
```

Needs a JDK (see Local setup). By default it uses the **same Databricks serving
endpoint** as the notebook, via your CLI auth — so a local run and a cluster run
are the same model and the same prompt. Useful flags:

- `--backend local` — use a local OpenAI-compatible model server instead
  (`--base-url`, `--model` to point it somewhere specific)
- `--no-llm` — run stages 1/2/4 with no model at all; the skipped diagnosis is
  logged as skipped rather than left blank
- `--clean-controls N`, `--limit N`, `--out DIR`

### Outputs (`data/audit_log/`)

| File | Contents |
|------|----------|
| `audit_log.jsonl` | One full record per order — engine verdict + evidence, agent diagnosis, adjudication, governance fields |
| `audit_log.csv`   | The same records flattened for spreadsheet review |
| `run_summary.json`| Run-level rollup: tiers, categories, agreement, review queue, model + prompt provenance |
| `grading.json`    | **Test-only** accuracy of both engine and agent vs. `ground_truth` |

`grading.json` is kept in a separate file, and `ground_truth` is kept out of the
audit schema entirely, for the same reason notebook 02 quarantines its accuracy
cell: the answer key exists to grade the system, and nothing that would run in
production may read it.

### Governance model

The audit trail encodes a deliberate split:

- The **deterministic engine is authoritative** — it decides the category, and
  its logic is inspectable line by line.
- The **LLM agent is advisory** — it explains and recommends, and never
  overrides. Given the measured accuracy gap below, treating it as authoritative
  would *lower* accuracy.
- **Disagreement escalates.** Where the two differ the order goes to human
  review rather than being resolved toward either one.
- **Nothing is actioned.** Every record carries `action_taken: "none"` and
  `autonomous_action_taken: false`. No ledger entry, payout, refund, or
  write-back to any source system happens anywhere in this pipeline.

Each record also carries the model name, a hash of the exact prompt used, and a
fingerprint of the exact evidence shown to the model, so any diagnosis can be
traced back to what produced it.

### Model comparison on the demo batch

Same prompt, same 19 orders (14 flagged + 5 clean controls), different model:

| Backend | Model | Agent vs. `ground_truth` |
|---------|-------|--------------------------|
| Databricks FM API | `databricks-meta-llama-3-3-70b-instruct` | **19/19 (100%)** |
| Local server      | `qwen2.5-7b-instruct`                    | 17/19 (89.5%) |
| Local server      | *Day 4, 5 hand-picked orders*            | 3/5 (60%) |

The engine scores 150/150 in every case. Clean controls: 5/5 on both backends —
neither model invented exceptions on clean orders.

The committed local artefact in `data/audit_log/` is the 7B run (157s; its first
call paid a ~69s cold model load — a warm run is ~75s).

### The two failure modes the 7B model had, and the 70B didn't

Both of the local model's errors were false negatives — calling a real exception
`clean_match` — on small-value orders, and both are instructive:

- On a `mdr_rate_mismatch` order it correctly *listed* the discrepancy
  (expected MDR 5.75 vs. actual 6.04) and then dismissed it as "minimal… could
  be due to rounding". That is a materiality error: ₹0.29 is small in absolute
  terms but a ~5% error in the fee rate, which the engine's fixed 0.01 tolerance
  catches without judgement.
- On a `duplicate_transaction` order it described the settlement line in the
  singular despite being shown two — the same structural counting blind spot
  seen on Day 4, still present at `temperature=0`.

Crucially, **both errors were contained by the architecture**: the engine had
both orders right, both were logged as `disagree`, and both were escalated to
human review. The agent's mistakes changed no outcome.

**Confidence is not a usable routing signal.** Mean confidence across the run
was 0.961, and the two wrong answers came in at 0.95 and 1.00 — at or above
average. That is why every flagged order goes to human review regardless of
confidence, and why the agent can never clear work, only add doubt: an
auto-clear threshold at ≥0.95 would have passed both misses through.

### Reproducibility caveat (measured, not assumed)

Running the identical 19 cases twice at `temperature=0`, with identical input
fingerprints, gave the same **cause** every time (0/19 changed) but different
**prose** on 11/19, and a different `confidence` on 2. This server's kernels are
nondeterministic below the sampler, so `temperature=0` stabilises the field that
drives routing without making a run reproducible. That is exactly why the audit
trail stores each explanation verbatim instead of regenerating it on demand —
re-running tomorrow produces different words for the same order.

### Known gap: the frozen batch and the cluster tables have drifted

Measured 2026-09-01: `data/demo_batch/` and the Delta tables in
`workspace.settletrace` were both generated at `num_orders=150, seed=42`, and are
structurally identical (same 136/6/4/2/2 category split) — but **4 of the 150
`order_id`s differ**, including one `duplicate_transaction` order. The differing
rows are the first four orders of the batch.

Local and cluster runs therefore grade against slightly different `ground_truth`
rows and are not order-for-order comparable, even though both currently score
the same. This is a determinism leak that survived the two fixes made on Day 2,
not something the Day 5 work introduced; it is un-root-caused and tracked as a
follow-up.
