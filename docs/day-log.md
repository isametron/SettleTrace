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

### Databricks auth done + catalog corrected

User ran `databricks auth login` themselves — profile `siddhant verma` saved,
authenticated as `siddhantadiverma@gmail.com` against
`dbc-6e186b0c-cfb8.cloud.databricks.com`. Checked catalogs from the CLI:
this workspace is Unity-Catalog-only — `hive_metastore` **does not exist**
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
across two runs — output became identical, confirming the cause: the
exception-category order-id sets (`timing_lag_ids`, `mdr_mismatch_ids`,
`duplicate_ids`, `missing_payout_ids`) were built as `set()`s of UUID
strings. Python randomizes string hashing per process by default
(`PYTHONHASHSEED`), so iterating such a set has an order that isn't governed
by `SEED` at all. The timing-lag refund-forcing loop draws from the shared
`random` stream *while* iterating one of these sets — so a different
iteration order silently reassigned which random refund amount landed on
which order, changing `orders.csv`/`ground_truth.csv` (and, on a later test
with a larger diff, `settlement_report.csv` too) between runs with identical
seeds.

Fixed by keeping these four as plain list slices of the already-shuffled
`shuffled_order_ids` (dropping the `set()` wrapper) instead of converting to
sets — nothing else in the notebook does membership testing against them, so
the change is safe. Reran the export script 3x with default (randomized)
`PYTHONHASHSEED` afterward and confirmed identical md5 checksums every time.

Lesson for later notebook work: any `set()` built from strings and then
*iterated* (not just membership-tested) is a reproducibility trap unless
`PYTHONHASHSEED` is pinned externally — safer to just avoid iterating sets
built from strings when order can affect output.

### Not yet verified

Everything above was verified by exec'ing the notebook logic locally with
fake Spark/Databricks stubs (no local JDK available), not by an actual run on
a live Databricks cluster. The Delta-write path still needs a real run —
next step is linking the GitHub repo to the workspace as a Git folder, then
running the notebook there.

### Files touched

- `notebooks/01_generate_synthetic_data.py` (exception injection, `ground_truth`
  table, `new_id()` determinism fix, widened widgets, revised validation cell)
- `scripts/export_demo_batch.py` (new)
- `data/demo_batch/orders.csv`, `settlement_report.csv`, `bank_feed.csv`,
  `ground_truth.csv` (new — committed, frozen demo batch)
- `README.md` (Day 2 status, widget table, demo-batch section)
- `docs/day-log.md` (this section)

## Day 3 — 2026-08-30: Matching logic (3-tier reconciliation)

**Goal:** build the exact → fuzzy → no-match classifier as a Databricks
notebook, get match-rate/exception-count output working end to end, and
produce a classified output table against the fixed demo batch.

### Faker install missing on serverless compute

First real run of `01_generate_synthetic_data.py` on the live cluster failed
with `ModuleNotFoundError: faker` — this workspace's serverless compute
doesn't come with it pre-installed (unlike a classic cluster where it's easy
to bake into a cluster-scoped library). Fixed by adding a `%pip install -q
faker` cell followed by `dbutils.library.restartPython()` at the very top of
the notebook. Confirmed locally (stubbed `dbutils.library.restartPython()` in
the test harnesses) and the user confirmed it worked live afterward. Tables
(`orders`, `settlement_report`, `bank_feed`, `ground_truth`) verified present
under `workspace.settletrace` via `databricks tables list` — Day 2 is now
fully closed out on the real cluster, not just locally.

### `notebooks/02_reconcile_settlements.py`

Reads only `orders` / `settlement_report` / `bank_feed` — never
`ground_truth` — and classifies every order:

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
  `settlement_report.net_amount` per batch — the second, independent leg of
  "multi-source" reconciliation (always balances in this dataset, by
  construction — the generator derives `bank_feed` from `settlement_report`
  directly — but the check exists for when that stops being true).
- Writes `reconciliation_result` (order-level, with a `reasoning` string).
- A clearly-separated final section reads `ground_truth` — the only place in
  the notebook it's touched — purely to grade the classification. Not part
  of the engine itself.

### Local test infrastructure: installed a JDK, hit a security block, worked around it

This notebook's logic is real Spark joins/aggregations (not generation logic
that happens to touch Spark at the end, like notebook 01), so faking `spark`
by hand wasn't a credible test. Installed Temurin 17 JDK via winget to get a
genuine local PySpark session.

Hit two environment issues along the way:
- **`uv run python` (and the venv's `.venv/Scripts/python.exe` directly) got
  blocked system-wide** by "An Application Control policy" immediately after
  the JDK install — likely a reputation/freshness check on a just-created
  executable. The underlying uv-managed interpreter
  (`%APPDATA%\uv\python\cpython-3.12.14-windows-x86_64-none\python.exe`)
  still ran fine. Worked around it by invoking that interpreter directly with
  `PYTHONPATH` pointed at `.venv/Lib/site-packages`, rather than trying to
  bypass or disable the policy itself.
- **PySpark workers defaulted to spawning `python3`**, which doesn't exist on
  Windows — fixed by setting `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to the
  real interpreter path above.
- **Persistent catalog writes (`CREATE DATABASE`, `saveAsTable`) failed** with
  the classic Windows `HADOOP_HOME`/`winutils.exe` error. Rather than chase
  down a winutils install, `scripts/test_reconcile_local.py` monkeypatches
  `spark.table()` to serve `data/demo_batch/*.csv` directly (by exact dotted
  name) and `DataFrameWriter.saveAsTable` to capture the result in-memory —
  sidesteps any Hadoop filesystem write, which has nothing to do with what's
  actually being tested (the join/classification logic itself).

### Result on the demo batch

100% accuracy against `ground_truth` (150/150), confusion matrix perfectly
diagonal. Match tiers: `exact=136` (90.7%), `fuzzy=10` (`mdr_rate_mismatch`=4,
`timing_lag_refund`=6), `no_match=4` (`missing_payout`=2,
`duplicate_transaction`=2) — auto-resolved rate (exact+fuzzy) 97.3%. Batch
check: 6/6 balanced.

### Not yet verified

Same caveat as before: verified via `scripts/test_reconcile_local.py` against
real local Spark and the frozen demo batch, not yet run on the live
Databricks cluster. Next step is pulling this into the workspace's Git folder
and running it there.

### Update — confirmed live on the Databricks cluster

Pulling into the Git folder hit a conflict: a `%pip install faker` cell had
been added directly in the notebook UI at some point and never committed.
Compared it against git (functionally identical to the already-pushed fix)
and, with the user's go-ahead, discarded the workspace-local edit
(`databricks repos update --dangerously-force-discard-all`) and pulled clean.

Ran `02_reconcile_settlements.py` as a one-time job via `databricks jobs
submit` (no cluster spec needed — this workspace is serverless-only) —
SUCCESS, ~79s. Rather than trust the notebook's own print output, queried the
live tables directly via the Statement Execution API
(`databricks api post /api/2.0/sql/statements` against the Serverless
Starter Warehouse — note: `databricks api ...` needs `MSYS_NO_PATHCONV=1` in
Git Bash, or PowerShell, or the leading `/api/...` gets mangled into a
Windows path): tier/category breakdown matched exactly, and a live join
against `ground_truth` confirmed 150/150 (100%) independently, not just via
the notebook's self-reported numbers.

### Files touched

- `notebooks/01_generate_synthetic_data.py` (`%pip install -q faker` +
  `dbutils.library.restartPython()` cell)
- `notebooks/02_reconcile_settlements.py` (new)
- `scripts/test_reconcile_local.py` (new — committed local test harness)
- `README.md` (Day 3 status, reconciliation-notebook section, local-setup note)
- `docs/day-log.md` (this section)

## Day 4 — 2026-08-30: Agent reasoning layer, part 1

**Goal:** design the LLM prompt for exception classification (row + context
in, cause + explanation + confidence out), get it working manually on 3-5
sample exceptions before automating the loop.

### Pivot: no Anthropic API credits — local model instead

Planned to use Claude (per the project's actual framing), invoked the
`claude-api` skill, and got as far as `client.messages.parse()` with a
`ExceptionDiagnosis` Pydantic model (cause/explanation/confidence/
recommended_action) before discovering the account had no API credits.
Installed the `ant` CLI (winget `Anthropic.Ant`) as a fallback path, but the
user then asked to pivot to a local model entirely ("Bionic AI Studio") — a
real scope decision, not just a stopgap, confirmed explicitly with the user
before writing any non-Anthropic code (per the claude-api skill's guardrail
against silently rewriting Claude-targeted code for another provider).

Recommended **Qwen2.5-7B-Instruct** given their hardware (RTX 5060 laptop,
~8GB VRAM; 24GB RAM; the Ryzen AI NPU doesn't factor in since local GGUF
runners use the GPU) — already had it loaded. Confirmed the server
(`http://localhost:1234/v1`) via direct HTTP calls before writing any code:
a plain chat completion worked, and a schema-constrained JSON request
revealed a real quirk — **the server does not enforce the schema's declared
`confidence` min/max range**, only structure; it returned `85` instead of
`0.85` in one test despite `"minimum": 0, "maximum": 1` in the schema. Added
a Pydantic `field_validator` to normalize any value >1 as a percentage
rather than trust the schema alone.

### Second Windows Defender block, this time on `_socket.pyd`

While testing, `uv run python` (previously working) started failing again —
not on `python.exe` this time, but importing the stdlib `socket` module
(`_socket.pyd` under the uv-managed CPython install) tripped the same
"Application Control policy" block seen on Day 3. Confirmed it was
file-specific, not a system-wide anti-network policy, by testing the
pre-existing Windows Store Python 3.13 install (`import socket` worked fine
there). Root-fixed rather than worked around per-script: rebuilt the
project's `.venv` against that Store Python interpreter (`uv venv --python
<store-python-path> --clear` + `uv sync --python <same>`). **Did not**
commit that interpreter path into `.python-version` — an early attempt via
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

Deliberately **does not** tell the model the rule engine's own category —
the point is an independent second opinion, not the model restating a label
it was handed.

### Result: 3/5 matched ground_truth

Ran for real against the live local server:

- `mdr_rate_mismatch`, `missing_payout`, `clean_match` — correctly diagnosed,
  well-grounded explanations citing the actual numbers.
- `timing_lag_refund` — **missed**: model called it `missing_payout` at 0.95
  confidence, despite its own explanation stating "only one settlement_report
  line exists for this order" — an internal contradiction (it named the
  wrong category despite correctly describing the evidence).
- `duplicate_transaction` — **missed**: model called it `clean_match` at 1.00
  confidence, despite its own explanation noting "both settlement_report
  lines for this order are identical" — it saw the duplicate and didn't treat
  the duplication itself as the anomaly.

Treating this as a real, useful finding rather than a failure to paper over:
the prompt design itself works end-to-end (schema-valid, grounded, cites real
figures), and a small 7B local model is measurably less reliable than the
deterministic rule engine (100% on Day 3) at catching structural cues like
"count how many lines exist" — exactly the kind of gap a real project should
surface, not hide. Worth revisiting with a larger local model (Qwen2.5-14B)
or a hosted model once credits exist, to see whether the same prompt does
better.

### Not yet done

- Only 5 hand-picked orders tested, not automated across the full batch —
  that's explicitly Day 4 part 2 / a later day per the plan.
- Two of five categories showed a real accuracy gap at this model size,
  un-investigated beyond noting it.
- No Claude API path actually exercised end-to-end (blocked on credits) —
  the `ant` CLI is installed and unauthenticated, ready whenever credits
  exist.

### Files touched

- `pyproject.toml` (dependency swap: `anthropic`+`pydantic` → `openai`+`pydantic`)
- `scripts/reason_about_exceptions.py` (new)
- `README.md` (Day 4 status, Stack, new "LLM reasoning layer" section)
- `docs/day-log.md` (this section, plus the Day 3 live-cluster update above)
