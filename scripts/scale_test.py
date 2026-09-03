"""Throughput test: generate and reconcile batches well past demo size.

The 150-order demo batch proves the logic is right. It proves nothing about
whether the engine is a toy. This runs the same two notebooks at increasing
batch sizes and reports wall-clock time and orders/sec for each, so the claim
"this scales" is a measurement rather than an assertion.

What is measured:

- **Generate**: `notebooks/01_generate_synthetic_data.py` exec'd with a larger
  `num_orders`, writing CSVs to a scratch directory. Plain Python, single
  threaded, so this is a floor on data-prep cost and not the interesting number.
- **Reconcile**: `notebooks/02_reconcile_settlements.py` on a real local Spark
  session, the same code path scripts/test_reconcile_local.py verifies for
  correctness. This is the number that matters.

Accuracy is re-checked against `ground_truth` at every size. A throughput
figure from a run that stopped classifying correctly would be worthless, and
scale is exactly where a tolerance or join bug would first show up.

Caveats stated up front, because they change how the numbers should be read:

- Local Spark on one laptop with `local[*]`, not a cluster. Absolute times are
  not Databricks times; the useful signal is how cost grows with batch size.
- Spark session startup (JVM boot, a few seconds) is measured once and excluded
  from per-size timings, since it is paid once per job, not per order.
- Stage 3 (LLM reasoning) is deliberately not timed here. It is one network
  call per flagged order against a serving endpoint, so its cost is set by
  endpoint latency and concurrency, not by this engine. Timing it on a laptop
  would measure the model host, not SettleTrace.

Usage:

    uv run python scripts/scale_test.py
    uv run python scripts/scale_test.py --sizes 150 1000 5000 20000
    uv run python scripts/scale_test.py --keep    # leave the generated CSVs

Needs the same JDK/PYSPARK_PYTHON setup as scripts/test_reconcile_local.py.
"""

import argparse
import csv
import shutil
import sys
import tempfile
import time
from pathlib import Path

from local_spark_harness import (
    build_local_spark,
    check_java_available,
    run_reconciliation_notebook,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
GENERATE_NOTEBOOK = REPO_ROOT / "notebooks" / "01_generate_synthetic_data.py"

DEFAULT_SIZES = (150, 1_000, 5_000, 20_000)
SEED = "42"
BATCH_SIZE = "25"

CSV_COLUMNS = {
    "orders": [
        "order_id", "customer_id", "order_amount", "order_date",
        "refund_amount", "refund_date", "expected_mdr_rate",
    ],
    "settlement_report": [
        "transaction_id", "order_id", "gross_amount", "mdr_fee", "gst_on_mdr",
        "refund_adjustment", "net_amount", "utr_number", "settlement_batch_id",
    ],
    "bank_feed": ["utr_number", "bank_credit_amount", "credit_date", "settlement_batch_id"],
    "ground_truth": ["order_id", "exception_type", "expected_reasoning", "related_transaction_id"],
}


class FakeWidgets:
    def __init__(self, values):
        self._values = dict(values)

    def text(self, name, default):
        self._values.setdefault(name, default)

    def get(self, name):
        return self._values[name]


class FakeLibrary:
    def restartPython(self):
        pass


class FakeDbutils:
    def __init__(self, values):
        self.widgets = FakeWidgets(values)
        self.library = FakeLibrary()


class FakeDataFrame:
    """Just enough DataFrame for notebook 01's display/write calls to no-op."""

    def __init__(self, rows, schema):
        self.rows = rows
        self.schema = schema

    def count(self):
        return len(self.rows)

    def limit(self, n):
        return FakeDataFrame(self.rows[:n], self.schema)

    def filter(self, condition):
        if condition == "exception_type != 'clean_match'":
            return FakeDataFrame(
                [r for r in self.rows if r["exception_type"] != "clean_match"], self.schema
            )
        raise NotImplementedError(condition)

    @property
    def write(self):
        return self

    def mode(self, _):
        return self

    def saveAsTable(self, name):
        pass


class FakeSpark:
    def sql(self, query):
        pass

    def createDataFrame(self, rows, schema):
        return FakeDataFrame(rows, schema)


def generate_batch(num_orders: int, out_dir: Path) -> tuple[dict, float]:
    """Run notebook 01 at `num_orders` and write its four tables as CSVs."""
    dbutils = FakeDbutils(
        {
            "num_orders": str(num_orders),
            "seed": SEED,
            "settlement_batch_size": BATCH_SIZE,
            "catalog": "workspace",
            "schema_name": "settletrace",
        }
    )
    exec_globals = {"dbutils": dbutils, "spark": FakeSpark(), "display": lambda _df: None}
    source = GENERATE_NOTEBOOK.read_text(encoding="utf-8")

    started = time.perf_counter()
    # The notebook prints validation output; a scale run has its own summary.
    original_stdout = sys.stdout
    sys.stdout = open(Path(tempfile.gettempdir()) / "scale_test_gen.log", "w", encoding="utf-8")  # noqa: SIM115
    try:
        exec(compile(source, str(GENERATE_NOTEBOOK), "exec"), exec_globals)  # noqa: S102  # our own notebook source
    finally:
        sys.stdout.close()
        sys.stdout = original_stdout
    elapsed = time.perf_counter() - started

    out_dir.mkdir(parents=True, exist_ok=True)
    for table, columns in CSV_COLUMNS.items():
        rows = exec_globals[table]
        with (out_dir / f"{table}.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            for row in rows:
                writer.writerow({c: "" if row[c] is None else row[c] for c in columns})

    return exec_globals, elapsed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", default=list(DEFAULT_SIZES))
    parser.add_argument("--keep", action="store_true", help="keep the generated CSV batches")
    args = parser.parse_args()

    java_problem = check_java_available()
    if java_problem:
        print(java_problem)
        return 1

    print("Starting local Spark (once, excluded from per-size timings)...")
    spark_started = time.perf_counter()
    spark = build_local_spark("settletrace-scale-test")
    spark_startup = time.perf_counter() - spark_started
    print(f"Spark ready in {spark_startup:.1f}s\n")

    workdir = Path(tempfile.mkdtemp(prefix="settletrace_scale_"))
    results = []
    try:
        # One discarded run first. Spark's first job on a fresh session pays JIT
        # compilation and codegen that every later job reuses, and it lands
        # entirely on whichever size happens to be measured first. Left in, it
        # made the 150-order row the slowest in the table and the growth figures
        # meaningless. Warming up is not flattering the numbers; it is charging
        # the one-off cost to the one-off column, where `spark_startup` already
        # reports it.
        print("Warming up Spark (discarded)...", end="", flush=True)
        warmup_dir = workdir / "warmup"
        generate_batch(150, warmup_dir)
        warm_started = time.perf_counter()
        run_reconciliation_notebook(spark, quiet=True, data_dir=warmup_dir)
        print(f" {time.perf_counter() - warm_started:.2f}s (not counted)\n")

        for size in args.sizes:
            batch_dir = workdir / str(size)
            print(f"[{size:>6} orders] generating...", end="", flush=True)
            gen_globals, gen_s = generate_batch(size, batch_dir)
            settlement_lines = len(gen_globals["settlement_report"])
            batches = len(gen_globals["bank_feed"])

            print(f" {gen_s:.2f}s   reconciling...", end="", flush=True)
            rec_started = time.perf_counter()
            exec_globals, _written = run_reconciliation_notebook(
                spark, quiet=True, data_dir=batch_dir
            )
            rec_s = time.perf_counter() - rec_started

            total_orders = exec_globals["total_orders"]
            correct = exec_globals["correct"]
            exceptions = total_orders - exec_globals["category_counts"].get("clean_match", 0)
            accuracy = correct / total_orders if total_orders else 0.0
            print(f" {rec_s:.2f}s   accuracy {accuracy:.1%}")

            # A throughput number from a run that misclassified is meaningless.
            assert total_orders == size, f"expected {size} orders, reconciled {total_orders}"
            assert correct == total_orders, (
                f"accuracy dropped to {accuracy:.1%} at {size} orders; "
                f"{total_orders - correct} misclassified"
            )

            results.append(
                {
                    "orders": total_orders,
                    "settlement_lines": settlement_lines,
                    "batches": batches,
                    "exceptions": exceptions,
                    "generate_s": gen_s,
                    "reconcile_s": rec_s,
                    "orders_per_s": total_orders / rec_s if rec_s else 0.0,
                    "accuracy": accuracy,
                }
            )
    finally:
        spark.stop()
        if args.keep:
            print(f"\nGenerated batches kept in {workdir}")
        else:
            shutil.rmtree(workdir, ignore_errors=True)

    print(f"\nSpark session startup (paid once per job): {spark_startup:.1f}s\n")
    header = (
        f"{'orders':>8} {'lines':>8} {'batches':>8} {'exc':>6} "
        f"{'generate':>9} {'reconcile':>10} {'orders/s':>10} {'accuracy':>9}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['orders']:>8,} {r['settlement_lines']:>8,} {r['batches']:>8,} "
            f"{r['exceptions']:>6,} {r['generate_s']:>8.2f}s {r['reconcile_s']:>9.2f}s "
            f"{r['orders_per_s']:>10,.0f} {r['accuracy']:>8.1%}"
        )

    if len(results) >= 2:
        first, last = results[0], results[-1]
        order_growth = last["orders"] / first["orders"]
        time_growth = last["reconcile_s"] / first["reconcile_s"] if first["reconcile_s"] else 0
        print(
            f"\n{order_growth:.0f}x the orders cost {time_growth:.1f}x the reconcile time "
            f"({last['orders_per_s'] / first['orders_per_s']:.1f}x the throughput per order)."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
