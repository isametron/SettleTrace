# SettleTrace Day Log

## Day 1, 2026-08-26: Repo setup + synthetic data generator

**Goal:** get the repo/env/Databricks connection working, and produce clean,
linked synthetic `orders` / `settlement_report` / `bank_feed` data at any scale.
No matching logic, no injected exceptions yet.

### Tooling installed

None of `uv`, Python, or the Databricks CLI were present on this machine, so:

- `uv` `0.12.5` via winget (`astral-sh.uv`)
- Databricks CLI `1.13.0` via winget (`Databricks.DatabricksCLI`)
- Python `3.12.14` via `uv python install 3.12` (uv-managed, not a system install)

Note: winget/uv add PATH entries via a registry update that only takes effect
in a new shell, and this session's shell was already running, so tools were
invoked via their full install paths under `%LOCALAPPDATA%\Microsoft\WinGet\Packages\...`.
**A fresh terminal should pick up `uv`, `uvx`, and `databricks` on PATH normally**,
worth confirming next session.

### Repo scaffold

- `uv init` to create `pyproject.toml` / `.python-version` / `uv.lock`, then
  stripped the default installable-package scaffolding (`src/settletrace/`,
  `[project.scripts]`, `[build-system]`) since nothing imports this project as
  a library. Set `[tool.uv] package = false` instead.
- Dependencies: `pyspark`, `faker` (runtime); `ruff` (dev group).
- `.gitignore` for `.venv/`, `__pycache__/`, `.databricks/`, etc.
- `uv sync` verified clean (resolves pyspark 4.2.0 locally, which is newer
  than most Databricks Runtime versions ship, but it's only used for local
  authoring/linting, not execution; the notebook itself runs on the cluster's
  own Spark).

### `notebooks/01_generate_synthetic_data.py`

Written as a real Databricks notebook (`# Databricks notebook source` +
`# COMMAND ----------` cells, `# MAGIC %md` docs), per the chosen format,
not a plain importable module.

Widgets: `num_orders` (default 50), `seed`, `settlement_batch_size` (25),
`catalog` (defaults to `hive_metastore`), `schema_name` (`settletrace`).

Generation logic:
- `orders`: laid out `settlement_batch_size` per day so date-grouping lines up
  with settlement batches; ~15% get a partial/full refund 1-3 days later.
- `settlement_report`: one line per order, with `mdr_fee` at a fixed 2% rate,
  `gst_on_mdr` at 18%, refund netted out, `net_amount` computed; all orders in
  a batch share one `utr_number` / `settlement_batch_id`.
- `bank_feed`: one row per batch, where `bank_credit_amount` = sum of that batch's
  `net_amount`, landing T+2 after the batch's order date.
- Inline validation cell (asserts, not a separate test suite, since this lives
  in a notebook): every settlement line maps to a real order 1:1, no
  duplicate/orphaned order_ids, refunds never exceed order amount, every
  bank credit sums exactly to its batch's net settlement amount.
- Catalog handling: tries `SHOW SCHEMAS IN <catalog>`, falls back to
  `hive_metastore` if that fails (Unity Catalog availability wasn't confirmed
  yet; see Open Items).
- Writes three Delta tables via `saveAsTable`, overwrite mode (idempotent
  re-runs at a new scale).

### Verification done today

- **Local dry-run** (`uv run python <scratchpad>/dryrun_notebook.py`): stubbed
  `dbutils`/`spark`/`display` with lightweight fakes (no local JDK, so a real
  local Spark session wasn't an option) and exec'd the notebook source
  directly. Ran at `num_orders=50` and `num_orders=500`, and validation
  assertions passed both times, table row counts matched (`orders`=N,
  `settlement_report`=N, `bank_feed`=N/batch_size batches).
- `ruff check` clean on the notebook, after adding a `per-file-ignores` rule
  for `notebooks/*.py` (F821 for Databricks-injected `dbutils`/`spark`/`display`
  globals; BLE001 for the intentionally-broad catalog-fallback `except`).

**Not yet verified:** an actual run on a live Databricks cluster (Delta
writes, real Unity-Catalog-vs-hive_metastore behavior). The dry-run only
proves the generation/validation arithmetic and linkage logic, not the
Spark/Delta write path itself.

### Open items / not done today

- Databricks CLI is installed but **not authenticated**. `databricks auth
  login --host <workspace-url>` needs an interactive browser login the user
  has to run themselves.
- GitHub repo not yet connected to the workspace as a Git folder (Repos).
- Catalog/schema naming unconfirmed. The user wasn't sure if Unity Catalog is
  enabled on their workspace; notebook defaults to `hive_metastore` with a
  fallback check, to be confirmed once connected.
- No noise/exceptions injected into the synthetic data yet (missing payouts,
  fee-rate mismatches, timing lags, duplicate settlements), planned for a
  later day, once the clean-data path is confirmed working end-to-end on
  Databricks.
- No matching/reconciliation engine yet, which is the actual core of the
  project and hasn't been started.

### Files touched

- `pyproject.toml`, `uv.lock`, `.python-version` (new)
- `.gitignore` (new)
- `notebooks/01_generate_synthetic_data.py` (new)
- `README.md` (expanded with setup/run instructions)
- `docs/day-log.md` (this file, new)

## Day 2, 2026-08-27: Inject realistic messiness + freeze a demo batch

**Goal:** add the five exception categories from the plan on top of the clean
Day 1 data, spot-check that the ground-truth labels are actually correct, and
freeze a fixed 100-200 row batch as the reproducible demo fixture.

### Exception injection (`notebooks/01_generate_synthetic_data.py`)

Every order now gets exactly one label, assigned to disjoint seeded-random
subsets before settlement lines are generated:

- `timing_lag_refund` (4%): order has a refund; `settlement_report` doesn't
  net it this batch (`refund_adjustment=0` even though `orders.refund_amount`
  is set). Forces a refund onto the order if the natural 15% refund roll
  didn't already give it one.
- `mdr_rate_mismatch` (2.5%): MDR charged at `expected_mdr_rate + 0.001`
  (e.g. 2.1% instead of 2%) instead of the agreed rate.
- `duplicate_transaction` (1.5%): a settlement line is appended as a literal
  copy of an existing one (same `transaction_id`), after the base
  `settlement_report` is built.
- `missing_payout` (1.5%): the order's settlement line is skipped entirely.
- Everything else (~90.7% at the canonical N=150) is `clean_match`.

A new `ground_truth` table (order_id, exception_type, expected_reasoning,
related_transaction_id) is the answer key: one row per order, including the
clean ones. It's explicitly *not* something a reconciliation engine gets to
see; it exists to grade that engine's accuracy later. `expected_reasoning`
is a short human-readable explanation per row (e.g. "Refund of 823.06 issued
on 2026-01-06 was not netted in this settlement batch; expected in the next
cycle."), the same style of reasoning SettleTrace's own agent should
eventually produce.

`bank_feed` generation now runs *after* exception injection, off the
(possibly messy) `settlement_report`, so a duplicate or missing line changes
what the bank actually credits for that batch, same as production.

### Bug found and fixed: `uuid.uuid4()` ignores the seed

First implementation kept `uuid.uuid4()` for `order_id`/`transaction_id`.
Ran the export script twice and diffed checksums. `orders.csv`,
`settlement_report.csv`, and `ground_truth.csv` changed between runs despite
the fixed seed; only `bank_feed.csv` (which has no UUID columns) stayed
identical. Cause: `uuid.uuid4()` draws from `os.urandom()`, not Python's
`random` module, so `random.seed(SEED)` never touched it, silently breaking
the "reproducible, not regenerated randomly" requirement.

Fixed with a `new_id()` helper: `uuid.UUID(int=random.getrandbits(128),
version=4)`, which derives a UUID4 from the seeded `random` module instead.
Reran twice and confirmed identical checksums across all four CSVs before
moving on. This is the kind of bug that would have silently invalidated the
whole "fixed demo batch" premise if it had shipped.

### Validation: replacing "spot-check ~10 rows" with something stronger

Rather than only eyeballing rows, the validation cell programmatically checks
every ground_truth row against the raw data it claims to describe (e.g. a row
labeled `duplicate_transaction` must have exactly 2 settlement rows sharing a
`transaction_id`; `missing_payout` must have 0; `mdr_rate_mismatch` must have
a fee that doesn't match `orders.expected_mdr_rate`), plus a category-count
check and the existing bank_feed-sum check from Day 1. It then prints ~2
sample rows per category. Manually read through the printed samples and the
exported `ground_truth.csv`: labels and reasoning text matched the underlying
numbers in every case checked.

### Demo batch (`scripts/export_demo_batch.py`, `data/demo_batch/*.csv`)

Added a committed export script that runs the notebook's own generation code
locally (same `dbutils`/`spark`/`display` stubbing trick as the Day 1 dry-run
harness) and writes `orders.csv`, `settlement_report.csv`, `bank_feed.csv`,
`ground_truth.csv` to `data/demo_batch/`. This is the single source of truth
for generation logic. The export script doesn't reimplement it, just exec's
the notebook file. Canonical parameters: `num_orders=150`, `seed=42` (150 is
inside the requested 100-200 row range; label distribution at this size is
136 clean / 6 timing-lag / 4 MDR-mismatch / 2 duplicate / 2 missing-payout, so
90.7% clean, matching the "90%+ clean" target).

Confirmed reproducibility directly: ran the export script twice back-to-back
and diffed `md5sum` of all four CSVs: identical both times (after the uuid
fix above).

### Databricks auth done + catalog corrected

User ran `databricks auth login` themselves. Profile `siddhant verma` saved,
authenticated as `siddhantadiverma@gmail.com` against
`dbc-6e186b0c-cfb8.cloud.databricks.com`. Checked catalogs from the CLI:
this workspace is Unity-Catalog-only: `hive_metastore` **does not exist**
(`databricks catalogs get hive_metastore` errors), so the notebook's original
default/fallback catalog would have failed outright. The real usable catalog
here is `workspace` (a `MANAGED_CATALOG`, with `workspace.default` schema
auto-created). Updated the `catalog` widget default and the fallback target
in `notebooks/01_generate_synthetic_data.py` and `scripts/export_demo_batch.py`
from `hive_metastore` to `workspace`.

### Second determinism bug: `set()` iteration order

While re-verifying reproducibility after the catalog fix above, the exported
CSVs changed between runs *again* despite the seed and despite the earlier
`uuid.uuid4()` fix. Root-caused by testing with `PYTHONHASHSEED=0` fixed
across two runs, and output became identical, confirming the cause: the
exception-category order-id sets (`timing_lag_ids`, `mdr_mismatch_ids`,
`duplicate_ids`, `missing_payout_ids`) were built as `set()`s of UUID
strings. Python randomizes string hashing per process by default
(`PYTHONHASHSEED`), so iterating such a set has an order that isn't governed
by `SEED` at all. The timing-lag refund-forcing loop draws from the shared
`random` stream *while* iterating one of these sets, so a different
iteration order silently reassigned which random refund amount landed on
which order, changing `orders.csv`/`ground_truth.csv` (and, on a later test
with a larger diff, `settlement_report.csv` too) between runs with identical
seeds.

Fixed by keeping these four as plain list slices of the already-shuffled
`shuffled_order_ids` (dropping the `set()` wrapper) instead of converting to
sets. Nothing else in the notebook does membership testing against them, so
the change is safe. Reran the export script 3x with default (randomized)
`PYTHONHASHSEED` afterward and confirmed identical md5 checksums every time.

Lesson for later notebook work: any `set()` built from strings and then
*iterated* (not just membership-tested) is a reproducibility trap unless
`PYTHONHASHSEED` is pinned externally, so it is safer to just avoid iterating sets
built from strings when order can affect output.

### Not yet verified

Everything above was verified by exec'ing the notebook logic locally with
fake Spark/Databricks stubs (no local JDK available), not by an actual run on
a live Databricks cluster. The Delta-write path still needs a real run. The
next step is linking the GitHub repo to the workspace as a Git folder, then
running the notebook there.

### Files touched

- `notebooks/01_generate_synthetic_data.py` (exception injection, `ground_truth`
  table, `new_id()` determinism fix, widened widgets, revised validation cell)
- `scripts/export_demo_batch.py` (new)
- `data/demo_batch/orders.csv`, `settlement_report.csv`, `bank_feed.csv`,
  `ground_truth.csv` (new, committed, frozen demo batch)
- `README.md` (Day 2 status, widget table, demo-batch section)
- `docs/day-log.md` (this section)

## Day 3, 2026-08-30: Matching logic (3-tier reconciliation)

**Goal:** build the exact → fuzzy → no-match classifier as a Databricks
notebook, get match-rate/exception-count output working end to end, and
produce a classified output table against the fixed demo batch.

### Faker install missing on serverless compute

First real run of `01_generate_synthetic_data.py` on the live cluster failed
with `ModuleNotFoundError: faker`. This workspace's serverless compute
doesn't come with it pre-installed (unlike a classic cluster where it's easy
to bake into a cluster-scoped library). Fixed by adding a `%pip install -q
faker` cell followed by `dbutils.library.restartPython()` at the very top of
the notebook. Confirmed locally (stubbed `dbutils.library.restartPython()` in
the test harnesses) and the user confirmed it worked live afterward. Tables
(`orders`, `settlement_report`, `bank_feed`, `ground_truth`) verified present
under `workspace.settletrace` via `databricks tables list`. Day 2 is now
fully closed out on the real cluster, not just locally.

### `notebooks/02_reconcile_settlements.py`

Reads only `orders` / `settlement_report` / `bank_feed`, never
`ground_truth`, and classifies every order:

- Recomputes the expected settlement line per order from `orders` alone
  (expected MDR fee, GST-on-MDR, refund adjustment, net amount).
- Counts actual settlement lines per order (`settlement_row_count`) to catch
  structural problems before any value comparison: 0 → `missing_payout`,
  2+ → `duplicate_transaction`.
- For orders with exactly one settlement line, compares actual vs. expected
  MDR fee (→ `mdr_rate_mismatch` if off) then refund adjustment (→
  `timing_lag_refund` if off), else `clean_match`.
- Maps category → tier: `clean_match` → **exact**; `mdr_rate_mismatch` /
  `timing_lag_refund` → **fuzzy** (linked correctly, one figure doesn't tie
  out); `missing_payout` / `duplicate_transaction` → **no_match** (can't be
  linked at all, structurally).
- Separately cross-checks `bank_feed.bank_credit_amount` against summed
  `settlement_report.net_amount` per batch, the second, independent leg of
  "multi-source" reconciliation (always balances in this dataset, by
  construction, since the generator derives `bank_feed` from `settlement_report`
  directly, but the check exists for when that stops being true).
- Writes `reconciliation_result` (order-level, with a `reasoning` string).
- A clearly-separated final section reads `ground_truth`, the only place in
  the notebook it's touched, purely to grade the classification. Not part
  of the engine itself.

### Local test infrastructure: installed a JDK, hit a security block, worked around it

This notebook's logic is real Spark joins/aggregations (not generation logic
that happens to touch Spark at the end, like notebook 01), so faking `spark`
by hand wasn't a credible test. Installed Temurin 17 JDK via winget to get a
genuine local PySpark session.

Hit two environment issues along the way:
- **`uv run python` (and the venv's `.venv/Scripts/python.exe` directly) got
  blocked system-wide** by "An Application Control policy" immediately after
  the JDK install, likely a reputation/freshness check on a just-created
  executable. The underlying uv-managed interpreter
  (`%APPDATA%\uv\python\cpython-3.12.14-windows-x86_64-none\python.exe`)
  still ran fine. Worked around it by invoking that interpreter directly with
  `PYTHONPATH` pointed at `.venv/Lib/site-packages`, rather than trying to
  bypass or disable the policy itself.
- **PySpark workers defaulted to spawning `python3`**, which doesn't exist on
  Windows, fixed by setting `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to the
  real interpreter path above.
- **Persistent catalog writes (`CREATE DATABASE`, `saveAsTable`) failed** with
  the classic Windows `HADOOP_HOME`/`winutils.exe` error. Rather than chase
  down a winutils install, `scripts/test_reconcile_local.py` monkeypatches
  `spark.table()` to serve `data/demo_batch/*.csv` directly (by exact dotted
  name) and `DataFrameWriter.saveAsTable` to capture the result in-memory,
  sidesteps any Hadoop filesystem write, which has nothing to do with what's
  actually being tested (the join/classification logic itself).

### Result on the demo batch

100% accuracy against `ground_truth` (150/150), confusion matrix perfectly
diagonal. Match tiers: `exact=136` (90.7%), `fuzzy=10` (`mdr_rate_mismatch`=4,
`timing_lag_refund`=6), `no_match=4` (`missing_payout`=2,
`duplicate_transaction`=2), with an auto-resolved rate (exact+fuzzy) of 97.3%. Batch
check: 6/6 balanced.

### Not yet verified

Same caveat as before: verified via `scripts/test_reconcile_local.py` against
real local Spark and the frozen demo batch, not yet run on the live
Databricks cluster. Next step is pulling this into the workspace's Git folder
and running it there.

### Update: confirmed live on the Databricks cluster

Pulling into the Git folder hit a conflict: a `%pip install faker` cell had
been added directly in the notebook UI at some point and never committed.
Compared it against git (functionally identical to the already-pushed fix)
and, with the user's go-ahead, discarded the workspace-local edit
(`databricks repos update --dangerously-force-discard-all`) and pulled clean.

Ran `02_reconcile_settlements.py` as a one-time job via `databricks jobs
submit` (no cluster spec needed, as this workspace is serverless-only):
SUCCESS, ~79s. Rather than trust the notebook's own print output, queried the
live tables directly via the Statement Execution API
(`databricks api post /api/2.0/sql/statements` against the Serverless
Starter Warehouse. Note that `databricks api ...` needs `MSYS_NO_PATHCONV=1` in
Git Bash, or PowerShell, or the leading `/api/...` gets mangled into a
Windows path): tier/category breakdown matched exactly, and a live join
against `ground_truth` confirmed 150/150 (100%) independently, not just via
the notebook's self-reported numbers.

### Files touched

- `notebooks/01_generate_synthetic_data.py` (`%pip install -q faker` +
  `dbutils.library.restartPython()` cell)
- `notebooks/02_reconcile_settlements.py` (new)
- `scripts/test_reconcile_local.py` (new, committed local test harness)
- `README.md` (Day 3 status, reconciliation-notebook section, local-setup note)
- `docs/day-log.md` (this section)

## Day 4, 2026-08-30: Agent reasoning layer, part 1

**Goal:** design the LLM prompt for exception classification (row + context
in, cause + explanation + confidence out), get it working manually on 3-5
sample exceptions before automating the loop.

### Pivot: no Anthropic API credits: local model instead

Planned to use Claude (per the project's actual framing), invoked the
`claude-api` skill, and got as far as `client.messages.parse()` with a
`ExceptionDiagnosis` Pydantic model (cause/explanation/confidence/
recommended_action) before discovering the account had no API credits.
Installed the `ant` CLI (winget `Anthropic.Ant`) as a fallback path, but the
user then asked to pivot to a local model entirely ("Bionic AI Studio"). That was a
real scope decision, not just a stopgap, confirmed explicitly with the user
before writing any non-Anthropic code (per the claude-api skill's guardrail
against silently rewriting Claude-targeted code for another provider).

Recommended **Qwen2.5-7B-Instruct** given their hardware (RTX 5060 laptop,
~8GB VRAM; 24GB RAM; the Ryzen AI NPU doesn't factor in since local GGUF
runners use the GPU), and it was already loaded. Confirmed the server
(`http://localhost:1234/v1`) via direct HTTP calls before writing any code:
a plain chat completion worked, and a schema-constrained JSON request
revealed a real quirk: **the server does not enforce the schema's declared
`confidence` min/max range**, only structure; it returned `85` instead of
`0.85` in one test despite `"minimum": 0, "maximum": 1` in the schema. Added
a Pydantic `field_validator` to normalize any value >1 as a percentage
rather than trust the schema alone.

### Second Windows Defender block, this time on `_socket.pyd`

While testing, `uv run python` (previously working) started failing again,
not on `python.exe` this time, but importing the stdlib `socket` module
(`_socket.pyd` under the uv-managed CPython install) tripped the same
"Application Control policy" block seen on Day 3. Confirmed it was
file-specific, not a system-wide anti-network policy, by testing the
pre-existing Windows Store Python 3.13 install (`import socket` worked fine
there). Root-fixed rather than worked around per-script: rebuilt the
project's `.venv` against that Store Python interpreter (`uv venv --python
<store-python-path> --clear` + `uv sync --python <same>`). **Did not**
commit that interpreter path into `.python-version`. An early attempt via
`uv python pin --resolved` wrote the absolute, user-specific Windows path
into that committed file, which would break for anyone else (or a future
reinstall); reverted it via `git checkout -- .python-version` and used
`--python <path>` as an explicit per-command flag instead, keeping the venv
fix local-only.

### `scripts/reason_about_exceptions.py`

Feeds one order per category (four exception types + one clean control,
picked directly from `ground_truth.csv`) to the local model. System prompt
explains the settlement-reconciliation domain and the five real categories
plus an `other` escape hatch (so the model isn't forced into a wrong label
when nothing fits), instructs confidence as a 0.0-1.0 decimal explicitly (on
top of the schema, given the enforcement gap above), and asks for reasoning
grounded in the specific numbers given rather than a restatement of the
category name. Uses `response_format: json_schema` (verified against the
live server via raw HTTP before wiring it into the script) rather than the
`openai` SDK's newer `.parse()` sugar, since the exact request/response shape
was already hand-verified to work against this specific server.

Deliberately **does not** tell the model the rule engine's own category.
The point is an independent second opinion, not the model restating a label
it was handed.

### Result: 3/5 matched ground_truth

Ran for real against the live local server:

- `mdr_rate_mismatch`, `missing_payout`, `clean_match`: correctly diagnosed,
  well-grounded explanations citing the actual numbers.
- `timing_lag_refund`: **missed**: model called it `missing_payout` at 0.95
  confidence, despite its own explanation stating "only one settlement_report
  line exists for this order", an internal contradiction (it named the
  wrong category despite correctly describing the evidence).
- `duplicate_transaction`: **missed**: model called it `clean_match` at 1.00
  confidence, despite its own explanation noting "both settlement_report
  lines for this order are identical". It saw the duplicate and didn't treat
  the duplication itself as the anomaly.

Treating this as a real, useful finding rather than a failure to paper over:
the prompt design itself works end-to-end (schema-valid, grounded, cites real
figures), and a small 7B local model is measurably less reliable than the
deterministic rule engine (100% on Day 3) at catching structural cues like
"count how many lines exist", exactly the kind of gap a real project should
surface, not hide. Worth revisiting with a larger local model (Qwen2.5-14B)
or a hosted model once credits exist, to see whether the same prompt does
better.

### Not yet done

- Only 5 hand-picked orders tested, not automated across the full batch;
  that's explicitly Day 4 part 2 / a later day per the plan.
- Two of five categories showed a real accuracy gap at this model size,
  un-investigated beyond noting it.
- No Claude API path actually exercised end-to-end (blocked on credits).
  The `ant` CLI is installed and unauthenticated, ready whenever credits
  exist.

### Files touched

- `pyproject.toml` (dependency swap: `anthropic`+`pydantic` → `openai`+`pydantic`)
- `scripts/reason_about_exceptions.py` (new)
- `README.md` (Day 4 status, Stack, new "LLM reasoning layer" section)
- `docs/day-log.md` (this section, plus the Day 3 live-cluster update above)

## Day 5, 2026-09-01: Agent reasoning layer part 2 + audit trail

**Goal:** automate the reasoning loop across every flagged exception, build the
audit trail, and get the full pipeline running start to finish:
data → match → exception reasoning → audit log.

### Refactor first: three scripts were about to become three copies

Day 4's prototype held the prompt, response schema, and case-builder inline;
Day 3's test harness held the demo-batch Spark schemas inline. Automating the
loop meant a second copy of both, so they were extracted before anything new
was written:

- `scripts/reasoning_agent.py`: prompt, JSON schema, `ExceptionDiagnosis`,
  `build_case`, and a `ReasoningAgent` wrapper. The prompt text is
  **byte-for-byte the Day 4 prompt** on purpose: Day 5's job was to measure it
  at scale, not to quietly tune it and lose comparability.
- `scripts/local_spark_harness.py`: demo-batch schemas, the `dbutils`/`spark`
  stubs, and the notebook-exec shim.

`scripts/reason_about_exceptions.py` and `scripts/test_reconcile_local.py` now
import from these instead of carrying their own copies. Re-ran the Day 3 test
after the refactor: still 150/150, confirming the extraction was behaviour-
preserving before building on top of it.

### `scripts/run_pipeline.py`: the four stages

Runs data → match → reason → audit in one command. Stage 2 exec's the *real*
`notebooks/02_reconcile_settlements.py` under local Spark rather than
reimplementing it, the same single-source-of-truth trick `export_demo_batch.py`
uses for notebook 01.

Two deliberate choices in stage 3:

- **Clean controls.** Alongside the 14 flagged orders, a deterministic seeded
  sample of 5 *clean* orders also goes to the agent. Without them the run only
  measures whether the agent can name an exception it was already pointed at;
  with them it also measures whether it invents exceptions that aren't there.
- **`temperature=0`.** Day 4 left the server default. See the reproducibility
  finding below for how much this actually bought.

### `scripts/audit_trail.py`: the audit record

One record per order (all 150, not just the exceptions), carrying: the engine's
tier/category/reasoning **plus the figures it compared** (a bare category ages
badly, since nobody can re-check it six months later); the agent's cause,
explanation, confidence and recommended action; model name, prompt hash, and a
fingerprint of the exact evidence shown to the model; the engine-vs-agent
adjudication; and the governance fields.

The governance stance, encoded rather than documented:

- Engine is **authoritative**, agent is **advisory** and never overrides. Not
  modesty. Day 4 measured the 7B model below the engine, so letting it override
  would lower accuracy.
- **Disagreement escalates** to human review rather than resolving toward
  either side.
- **`action_taken: "none"` and `autonomous_action_taken: false` on every
  record**, plus a run-level count, so a reviewer reading any single line
  doesn't have to take the summary's word for it.

`ground_truth` is kept *out* of the audit schema entirely, so accuracy grading
goes to a separate `grading.json`, preserving the boundary notebook 02 already
draws with its quarantined accuracy cell.

### Result: engine 150/150, agent 17/19

Full run: 14 flagged + 5 controls diagnosed, 150 audit records written, 14
queued for human review, 136 auto-cleared, **0 autonomous actions**. ~75s warm
(the committed run reports 157s, because its first model call paid a ~69s cold load).

- **Engine 150/150 (100%)**: unchanged.
- **Agent 17/19 (89.5%)**, up from 3/5 on Day 4's hand-picked sample.
- **Controls 5/5**: no invented exceptions on clean orders.

Both misses were false negatives (real exception called `clean_match`), both on
small-value orders:

- `mdr_rate_mismatch` (504d281f): the model **correctly listed** expected MDR
  5.75 vs actual 6.04, then dismissed it as "minimal… could be due to rounding"
  at 0.95 confidence. A materiality error, and a genuinely interesting one:
  ₹0.29 is small in absolute terms but ~5% off the agreed rate. The engine's
  fixed 0.01 tolerance has no opinion about whether a number "looks small".
- `duplicate_transaction` (6b65a6a4): described "the actual settlement line" in
  the singular despite being shown two. Same structural counting blind spot as
  Day 4, still present at `temperature=0`.

Checked Day 4's five specific orders inside this run: the `timing_lag_refund`
one it missed on Day 4 is now correct, the `duplicate_transaction` one still
fails. Two things changed since Day 4 though (temperature *and* sample size), so
that improvement is **not** cleanly attributable to either. An A/B holding
sample fixed would be needed to say more, and wasn't run.

### Finding: confidence is not a usable routing signal

Mean agent confidence across the run was 0.961, and the two wrong answers came
in at **0.95 and 1.00**, at or above that mean. The model is confidently wrong
in exactly the cases it gets wrong, so confidence carries no signal about
correctness here.

This retroactively justifies a design choice that had been made on general
caution: every engine-flagged order goes to human review *regardless* of what
the agent's confidence says, and the agent can never clear work, only add
doubt. Had confidence been used as an auto-clear threshold (the obvious
"efficiency" feature to build next), both misses would have sailed through at
≥0.95. Worth remembering before anyone proposes that feature.

### Finding: `temperature=0` does not make the run reproducible

Worth recording because it contradicts what was initially written in the code.
Ran the identical 19 cases twice, confirming via the stored case fingerprints
that the inputs were byte-identical:

- **cause: 0/19 changed**: the field that drives routing is stable.
- **explanation prose: 11/19 differed.** `confidence` moved on 2 (1.0→0.95,
  0.95→0.85).

So the server is nondeterministic below the sampler (batching/kernel
non-associativity), and `temperature=0` buys stable classification, not
reproducibility. The docstring claiming otherwise was corrected to state the
measurement.

This turned out to be the strongest architectural argument for the audit trail
existing at all: **an explanation that can't be reproduced by re-running is only
preserved if it was written down when it was made.** The log isn't paperwork
about the reasoning; it's the only copy of it.

### Day 5, part 2: onto Databricks

The above ran locally only, because the reasoning stage talked to a model server
on `localhost` that a cluster can't reach. That was a real blocker, not a
preference, so the next move was to remove it.

**The workspace already had what was needed.** `databricks serving-endpoints
list` showed 11 Foundation Model API endpoints live and `READY`, including
`databricks-meta-llama-3-3-70b-instruct`, `databricks-qwen35-122b-a10b` and
`databricks-gpt-oss-120b`. Pay-per-token, reachable from the cluster, no
deployment needed. The Day 4 pivot to a local model had been driven by the
Anthropic account having no credits; nobody had checked whether Databricks
itself served models.

**Verified the hard dependency before building on it.** The whole prompt design
rests on `response_format: json_schema`, so that was tested first, on the case
the local 7B model got *wrong* (the MDR mismatch it dismissed as rounding). Two
findings:

- The CLI's `databricks serving-endpoints query` **silently drops
  `response_format`** ("Warning: unknown field"), a CLI schema limitation, not
  an endpoint one. Testing through the CLI alone would have wrongly concluded
  structured output was unsupported.
- Through the OpenAI-compatible protocol it works. Four of five candidate
  endpoints returned schema-valid JSON and **all four diagnosed correctly** the
  case the 7B model missed. `databricks-gpt-oss-120b` returns `content` as a
  list of blocks rather than a string, so `json.loads` chokes on it. Handled
  defensively in `extract_message_content`, but not made the default.

**One code path, two backends.** `ReasoningAgent(backend="databricks"|"local")`.
Databricks Foundation Model APIs speak the OpenAI chat protocol at
`<host>/serving-endpoints`, so only the base URL and credential change. Prompt,
schema and parsing are identical, which is what makes the two backends
measurable against each other. Auth resolves through `WorkspaceClient`: the
notebook's own identity on a cluster, the CLI profile locally, no secrets in the
notebook. (`WorkspaceClient.serving_endpoints.get_open_ai_client()` is deprecated
and pulls in `httpx`; building the `OpenAI` client from host + token directly
avoids the dependency and works in both places.)

**`notebooks/03_reason_and_audit.py`** is stages 3 and 4 as a real Databricks
notebook, writing the `audit_log` **Delta table**, closing the "JSONL, not
Delta" gap noted above. It imports the prompt and audit schema from `scripts/`
rather than restating them, so the cluster and the local runner can't drift.
Two wrinkles worth recording:

- `build_case` was written against the demo-batch CSVs where every field is a
  string; Delta returns typed values. Normalising the rows at the notebook
  boundary (rather than loosening `build_case`) keeps both paths feeding the
  model byte-identical prompts.
- The audit table uses an **explicit** schema, not inference: most agent columns
  are null for the ~90% of orders that match cleanly, and Spark would infer
  those as void and fail the write on a run where nothing was flagged.

**Tested locally first** (`scripts/test_notebook03_local.py`) by chaining
notebook 02 → notebook 03 under real local Spark, asserting the governance
invariants hold on the resulting table. Then synced to the workspace with
`databricks sync` (deliberately *not* a git push, which is the user's call) and
submitted via `databricks jobs submit`: **SUCCESS**.

**Verified by querying the table, not by trusting the notebook's print output**
using the same discipline as Day 3:

| Check | Result |
|-------|--------|
| `audit_log` rows | 150 |
| Review queue | 14 `needs_human_review`, 136 `auto_cleared` |
| `action_taken` distinct values | 1 (`none`) |
| `SUM(autonomous_action_taken)` | **0** |
| Engine vs. agent on flagged | 14 agree, 0 disagree |
| Agent vs. `ground_truth` (test-only) | **19/19 (100%)** |
| Provenance | `databricks-meta-llama-3-3-70b-instruct`, prompt `e07e929411a5` |

**The 70B model fixed both failure modes.** The `duplicate_transaction`
structural miss (wrong on Day 4 *and* on Day 5's local run) and the
`mdr_rate_mismatch` materiality error both came out correct. Same prompt, same
orders, bigger model. That reframes the Day 4/5 findings: they were a
7B-parameter limitation, not a prompt defect.

Note this does **not** retire the governance design. The agent agreeing with the
engine 14/14 is a result from one run on one 150-row batch; the confidence
finding above still stands, and a model that is right today is not a reason to
let it act tomorrow.

### Finding: the frozen demo batch and the cluster tables have drifted

Caught while cross-checking the cluster's `duplicate_transaction` orders against
the local ones. `data/demo_batch/` and `workspace.settletrace` were both
generated at `num_orders=150, seed=42` and are structurally identical (same
136/6/4/2/2 category split) but **4 of 150 `order_id`s differ**, and one of the
four is a `duplicate_transaction` order. The differing rows are the first four
orders of the batch (all dated 2026-01-05); rows 4 onward match exactly.

That signature rules out a simple RNG-stream offset (which would change
everything after the divergence point). Plausible causes not yet tested: a
`faker` version difference changing how many draws are taken from the shared
`random` stream, a Python version difference (local 3.13 vs serverless 3.11/3.12)
in `random.sample`/`shuffle` internals, or the cluster tables having been written
by an older commit of notebook 01.

This is a **third** determinism leak, after the `uuid.uuid4()` and `set()`
-iteration bugs fixed on Day 2, the "frozen, reproducible demo batch" premise
is weaker than the Day 2 entry claims. It means local and cluster runs grade
against slightly different answer keys and aren't order-for-order comparable,
even though both currently score identically. Left un-root-caused rather than
papered over by regenerating one side to match the other; tracked as a
follow-up.

### Known gaps

- The batch-drift finding immediately above is diagnosed but not fixed.
- The Day 5 local-model failure modes are fixed *by using a larger model*, not
  by understanding the prompt's role in them. The prompt was deliberately left
  byte-identical throughout so the model comparison stayed clean.
- `databricks-gpt-oss-120b` (the largest reasoning endpoint available) is
  handled but untested end-to-end.
- The repo reached the cluster via `databricks sync` to
  `/Users/<user>/SettleTrace-dev`, not through the workspace Git folder. The Git
  folder still points at the last pushed commit, so a `git push` is needed
  before the workspace copy and the repo agree.
- Nothing is committed yet; the whole of Day 5 is uncommitted working tree.

## Day 6, 2026-09-03: Dashboard / presentation layer

**Goal:** a Streamlit dashboard showing match rate %, ₹ cleared vs flagged, and
the exception list with agent explanations, clean enough to demo live.

### What it reads

`dashboard/app.py` reads the **audit trail**, not the raw tables. That falls out
of Day 5's design: the audit log already carries the engine verdict, the evidence
figures, the agent's diagnosis, the adjudication, and the governance fields, so
the dashboard is a view over one artefact rather than a second place where
reconciliation logic gets re-implemented.

Two sources, switchable in the sidebar:

- **Local run**: the committed `data/audit_log/audit_log.jsonl`. Instant, works
  offline, and is what a live demo should use.
- **Databricks (live)**: queries the `audit_log` Delta table through the SQL
  Statement Execution API.

Local is the default deliberately: a stopped SQL warehouse takes ~30s to wake,
and that is not a thing to discover in front of judges.

### Design

Built dark first, then redone light against the BI reference the user pointed
at: white cards on a light grey plane, headline figures large and light-weight
in the sequential blue, labels above them in plain sentence case, generous
whitespace. Layout follows the same references: KPI row across the top, filters
down the left, detail below. Charts were built against the `dataviz` skill's
method rather than by taste:

- **KPI row is stat tiles, not charts.** Five headline numbers, including
  **Autonomous actions: 0** as a first-class figure rather than a footnote, and
  the only one in the status-good green, since that figure encodes a safety
  state rather than a series.
- **Value cleared vs held** is one stacked proportion bar, not a two-slice pie.
  The large segment is direct-labelled (91.6%); the ~8% held sliver is too narrow
  for a label that wouldn't be clipped, so its value is carried by the legend and
  the tooltip.
- **Exceptions by type** is a horizontal bar in a single hue. Colouring each bar
  darker-where-bigger would double-encode length as hue on nominal categories.
- **Clean matches are excluded from that chart.** At 136 of 150 they compressed
  every exception bar to 3-8px. The clean count is already the KPI row's
  headline; the chart's job is the 14 orders that need a person.
- Every chart has a table-view twin (expander at the bottom, with CSV export).

### The exception queue

The part that actually carries the project. One card per held order, sorted by
value, each showing the rule engine and the agent **side by side**:

- Engine: its reasoning plus the figures it compared, with any actual that
  differs from expected rendered in the critical colour.
- Agent: its explanation, confidence, and recommended action.
- A chip saying whether the two agree; where they don't, the card says so
  explicitly rather than picking a winner.
- A footer on every card: `Action taken: none · autonomous_action_taken = false`.

The demo case is order `504d281f`: the engine flags an MDR mismatch, the agent
calls it clean at 0.95 confidence, and the card shows the expected-vs-actual
figures next to the agent's own words dismissing a ~5% rate error as "minimal…
could be due to rounding". The disagreement, the evidence, and the fact that
nothing was actioned are all visible in one card.

### Escaping model output

The agent's explanation is model-written text being rendered into a page with
`unsafe_allow_html=True`, which would let anything the model emitted execute as
markup in the reviewer's browser. It is escaped through a `quote_html` helper
before rendering. This also fixed a cosmetic bug: Streamlit was parsing the
model's `- ` lines as a markdown list and hoisting them out of the styled quote
block, so the agent column lost the rule the engine column had.

### Known gaps

- The committed `data/audit_log/` artefact is the **local 7B run**, so the
  dashboard's default view shows `qwen2.5-7b-instruct` as the model and includes
  the two disagreements. That is the better demo (an empty disagreement state
  would be less informative) but it does not match the Databricks run, which
  agreed 14/14. Switching the source to Databricks shows the 70B numbers.
- No automated test for the dashboard; it was verified by rendering and reading
  it, plus a DOM check that both segments of the value bar actually draw.

### Files touched

- `dashboard/app.py` (new)
- `.streamlit/config.toml` (new: light theme matching the chart palette)
- `pyproject.toml` / `uv.lock` (added `streamlit`, `plotly`)
- `README.md`, `docs/day-log.md` (this section)

### Files touched

- `notebooks/03_reason_and_audit.py` (new: reasoning + audit as a Databricks
  notebook, writing the `audit_log` Delta table)
- `scripts/reasoning_agent.py` (new: shared reasoning layer, two backends)
- `scripts/local_spark_harness.py` (new: extracted shared Spark harness)
- `scripts/audit_trail.py` (new: audit record/summary/grading schemas + writers)
- `scripts/run_pipeline.py` (new: the four-stage local runner)
- `scripts/test_notebook03_local.py` (new: chains notebooks 02 to 03 locally and
  asserts the governance invariants)
- `scripts/reason_about_exceptions.py` (now imports the shared reasoning layer)
- `scripts/test_reconcile_local.py` (now imports the shared Spark harness)
- `pyproject.toml` (added `databricks-sdk`)
- `data/audit_log/*` (new: audit trail from the canonical local run)
- `README.md` (Day 5 status, Databricks notebook + local pipeline sections)
- `docs/day-log.md` (this section)

## Day 7, 2026-09-03: Scale test + polish

**Goal:** re-run the pipeline at a much larger batch size and capture real
timing numbers, fix whatever that surfaces, tighten the agent's explanations,
and rewrite the README into something submission-ready.

### First: the frozen batch was not actually frozen

This predates today's plan and had to be cleared before any of it was worth
doing. Measured on 2026-09-01: `data/demo_batch/` and the Delta tables in
`workspace.settletrace`, both generated at `num_orders=150, seed=42`, were
structurally identical (same 136/6/4/2/2 split) but disagreed on 4 of 150
`order_id`s, one of them a `duplicate_transaction` order. Local runs and cluster
runs were grading against different answer keys.

Lining the two UUIDs up in hex gave it away immediately:

```
local   ad3c2d6d 1a3d4fa7 bc8960a9 23b8c1e9
cluster bd9c66b3 ad3c4d6d 9a3d1fa7 bc8960a9
```

The cluster's first id is the local one shifted right by exactly one 32-bit
word, with a fresh word on the front, and the only mismatched nibbles sit
precisely where `uuid.UUID(version=4)` overwrites the version and variant bits.
So the cluster's Mersenne Twister was four 32-bit words (128 bits) ahead at the
first `new_id()`.

Confirmed by construction rather than by argument. Re-running the generator
locally with N extra `getrandbits(32)` draws injected after seeding:

| extra words | positional match vs. cluster tables |
| --- | --- |
| 0 (the committed batch) | 146/150 |
| 1 | 143/150 |
| 2 | 143/150 |
| 3 | 146/150 |
| **4** | **150/150, exact** |

**Why "146 of 150 match" was a red herring.** A four-word offset should corrupt
everything downstream, so partial alignment looked like evidence against a
stream shift. It is not. `random.randint()` goes through `_randbelow()`, which
uses rejection sampling, so the number of words one call consumes is
value-dependent. Two offset streams can re-converge, and here they did, after
four orders. That is also why this survived both Day 2 determinism fixes.

**Ruled out by measurement, not assumption.** A probe notebook on live
serverless reported Python 3.11.10 against 3.13.14 locally, with a bit-identical
stream after `seed(42)` on both, and Faker 40.37.0 on *both* sides consuming
zero draws from the global module. Neither the runtime nor the library version
explains it. `DESCRIBE HISTORY` showed the tables had exactly one version,
written 2026-08-26T21:41:43Z and never regenerated, from an interactive session
whose environment no longer exists.

**Stated honestly: the exact statement that consumed those 128 bits is not
identified, and now cannot be.** What is established is the mechanism, and that
the notebook was structurally open to it.

The fix removes the class rather than the instance. `random.seed()` plus the
module-level `random.*` functions share one Mersenne Twister with every library
in the interpreter, so any import or library init that draws from it shifts the
output for a fixed seed. Now:

- `rng = random.Random(SEED)`, a private generator unreachable from anything
  else, so output depends on SEED alone.
- Faker removed entirely. `fake = Faker()` was constructed and never used, a
  dead import that was also the prime suspect. That also removed the
  `%pip install -q faker` cell, the `restartPython()` cell, and the dependency.
- The canonical `order_id` sequence pinned to a SHA-256 digest, asserted at
  generation time. Two determinism bugs had already shipped without anything
  failing; a third now fails loudly.

`random.Random(42)` yields the same stream as `random.seed(42)` did on the
global instance, so the frozen batch did not move. The cluster was what was
wrong, and the cluster is what changed: the tables were regenerated and now
match the committed CSVs byte for byte across all four.

### Tier 1 was not testing what it claimed

While in there: the stated exact-match test is the settlement identity,
`net_amount == order_amount - mdr_fee - gst_on_mdr - refund_adjustment`. The
engine only compared `mdr_fee` and `refund_adjustment`. `expected_gst_on_mdr`
and `expected_net_amount` were computed, written into `reconciliation_result`,
and never compared to anything, so a line with a correct fee and refund but a
corrupted GST or net passed as `clean_match`.

Added those comparisons plus an `unexplained_value_break` category for a line
that links correctly, whose fee and refund tie out, and whose own arithmetic
still does not hold.

### The scale test, and the bug it found within a minute

`scripts/scale_test.py` runs the same two notebooks at increasing sizes and
reports wall clock, throughput and accuracy at each. Accuracy is asserted at
every size, which is the whole reason it was worth writing:

**At 1,000 orders, one order misclassified.** Order amount 1462.75, and every
field a paisa apart between the two rounding conventions:

```
                 generator (HALF_EVEN)   engine (HALF_UP)
    mdr_fee                     29.25              29.26
    gst_on_mdr                   5.26               5.27
    net_amount                1428.24            1428.22
```

Each field is inside the one-paisa tolerance on its own. But the identity check
rebuilt the net from *expected* fee and GST, so two independent rounding
differences compounded into two paise and broke it, on an order where nothing
was wrong.

Fixed by comparing like with like rather than widening the tolerance. The
identity is now evaluated on the figures the settlement file actually charged
(`gst_on_charged_fee`, `identity_net_amount`), which is what the identity means:
the line has to add up against itself. Charged-versus-expected comparison is
still done separately by the `mdr_rate_mismatch` and `timing_lag_refund` checks,
so coverage is unchanged.

This is the second rounding-convention bug in two days (Day 5's
`breaks_tolerance` was the first). The pattern is the same both times: Python
rounds HALF_EVEN, Spark rounds HALF_UP, and the demo batch is too small and too
round for it to show.

### Numbers

Local Spark, `local[*]`, one laptop, not a cluster:

| orders | lines | batches | exceptions | generate | reconcile | orders/s | accuracy |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 150 | 150 | 6 | 14 | 0.01s | 2.75s | 55 | 100% |
| 1,000 | 1,000 | 40 | 95 | 0.03s | 2.68s | 373 | 100% |
| 5,000 | 5,000 | 200 | 475 | 0.13s | 2.95s | 1,696 | 100% |
| 20,000 | 20,000 | 800 | 1,900 | 1.75s | 3.25s | 6,155 | 100% |
| 50,000 | 50,000 | 2,000 | 4,750 | 12.93s | 4.88s | 10,242 | 100% |

**333x the orders costs 1.8x the reconcile time.** The work is dominated by
fixed Spark overhead rather than per-order cost. Session startup (6.3s) is
measured once and excluded, since it is paid per job and not per order, and a
discarded warm-up run absorbs first-job JIT so that cost does not land on
whichever size happens to be measured first. Without the warm-up the 150-order
row was the slowest in the table, which made the growth figures meaningless.

Generation is single-threaded plain Python and is the floor on data prep, not an
interesting number. Stage 3 is deliberately absent: it is one network call per
flagged order, so its cost is endpoint latency and concurrency, not this engine.

### Agent explanations: could not measure, so did not guess

The plan was to re-prompt anything reading generic. Reviewing the committed run,
17 of 19 explanations cite an actual figure; the two that do not are both
arguable, and one is legitimate, since a missing payout has no settlement
figures to cite.

Both backends were down at this point: the local server refused connections and
the Databricks CLI refresh token had expired. **Editing a measured prompt with
no ability to re-measure it, the day before submission, would trade a known
quantity for an unknown one**, so the prompt was left alone.

Instead the question became a tracked number. `explanation_cites_figures()` in
`scripts/audit_trail.py` records per row and counts per run whether the agent
argued from evidence or from the category name. It is deliberately blunt (any
digit outside an identifier) and nothing branches on it: it is a signal to read,
not a gate. Current run scores 17/19, and the next run grades itself.

### Comments and docs rewritten without dashes

Separate request, folded into the same polish pass. Every em and en dash in
comments, docstrings, notebook markdown, user-facing strings and this log
replaced with ordinary punctuation, rewording where a colon or full stop alone
read badly. Left alone: `# COMMAND ----------` markers, `# --- Section ---`
dividers, CLI flags, markdown bullets, and the minus sign in negative currency.

One thing this broke and the linter caught: `# noqa: S102 -- reason` became
`# noqa: S102: reason`, and ruff parses everything after `noqa:` as error codes,
so four directives became invalid. A second `#` terminates it cleanly.

`notebooks/02`'s reasoning strings changed, which flows into
`reconciliation_result` and the audit trail. The generator's
`expected_reasoning` strings were already dash-free, so the frozen batch is
untouched and still exports byte-identical.

### README rewritten

Problem framing first, since the old one opened with a day-by-day status that
read as a dev log. Added a mermaid architecture diagram, and an explicit table
mapping each part of "every money action explainable, bounded and gated" to the
mechanism that enforces it and the file to verify it in. Consolidated the run
instructions into one quickstart. Removed the "Known gap: the frozen batch and
the cluster tables have drifted" section, which is now fixed.

### Not yet done

- **The agent's explanations are still unmeasured against a fresh run.** The
  metric exists and the current run scores 17/19, but no prompt change was
  attempted and none was validated.
- **`PROMPT_VERSION` changed** as a side effect of the dash rewrite, since it is
  a hash of the prompt text. The committed audit artifacts record
  `e07e929411a5`; the next run will record something different for a prompt that
  differs only in punctuation. The 19/19 and 17/19 figures both belong to the
  old hash.
- **The Databricks workspace Git folders may still hold the pre-fix notebook.**
  Verified stale earlier; not re-checkable after the token expired. A git push
  does not update them.
- `unexplained_value_break` and orphan settlement lines still never fire on this
  batch, so both remain unexercised by real data.
- The mermaid diagram has not been eyeballed rendered.

### Files touched

- `notebooks/01_generate_synthetic_data.py` (private `rng`, Faker removed,
  reproducibility fingerprint)
- `notebooks/02_reconcile_settlements.py` (settlement identity on charged
  figures, `breaks_tolerance`, `unexplained_value_break`)
- `notebooks/03_reason_and_audit.py` (records the explanation-quality field)
- `scripts/scale_test.py` (new: throughput and accuracy at increasing sizes)
- `scripts/audit_trail.py` (`explanation_cites_figures`, per-record and per-run)
- `scripts/run_pipeline.py` (wires the new field through)
- `scripts/local_spark_harness.py` (`data_dir` parameter so the scale test drives
  the same notebook the correctness test does)
- `pyproject.toml`, `uv.lock` (dropped `faker`)
- `README.md` (rewritten)
- `docs/day-log.md` (this section)
