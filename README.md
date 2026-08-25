# SettleTrace

A settlement reconciliation agent that explains its exceptions instead of just flagging them.

It unpacks a lumped settlement (one bank credit netting hundreds of orders against MDR
fees, GST-on-MDR, and refunds) back to order-level, classifies each line as an exact
match, a fuzzy match, or an exception — and reasons about *why* on the exceptions
instead of just flagging them.

## Status

Day 1: repo + environment setup, and a synthetic data generator producing clean,
linked `orders` / `settlement_report` / `bank_feed` data at any scale. No
reconciliation/matching logic yet, and no injected exceptions yet — that's next.

## Stack

- Databricks + PySpark for batch reconciliation
- `uv` for local Python env/dependency management
- Prophet, later, for a cash-forecasting angle

## Local setup

```
uv sync
```

This installs `pyspark` and `faker` locally, purely so notebook cell logic can be
authored and sanity-checked in a plain Python REPL before running on the real
Databricks cluster (the local `pyspark` version does not need to match the
cluster's Databricks Runtime version — it's a dev convenience, not an execution
target). Running a full local Spark session additionally requires a JDK, which
isn't installed here; the pure-generation logic can still be exercised without
one (see `notebooks/01_generate_synthetic_data.py`'s docstring-style comments).

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

| Widget                   | Default          | Meaning                                      |
|---------------------------|------------------|-----------------------------------------------|
| `num_orders`               | `50`             | Number of orders to generate                  |
| `seed`                      | `42`             | RNG seed, for reproducibility                  |
| `settlement_batch_size`     | `25`             | Orders per lumped settlement batch             |
| `catalog`                   | `hive_metastore` | Target catalog (falls back here if Unity Catalog isn't usable) |
| `schema_name`               | `settletrace`    | Schema the three tables are written under      |

It writes `orders`, `settlement_report`, and `bank_feed` as Delta tables under
`<catalog>.<schema_name>`, after an inline validation cell asserts the batch is
fully linked (every settlement line maps to a real order, every bank credit
sums to its batch's net settlement amount, no orphans).
