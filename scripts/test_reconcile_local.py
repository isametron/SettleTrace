r"""Local, real-Spark test of notebooks/02_reconcile_settlements.py.

Unlike scripts/export_demo_batch.py (which fakes `spark` entirely, since
notebook 01's heavy lifting is plain Python), this notebook's whole point is
Spark join/aggregation logic, so it's tested against a real local
SparkSession here. `spark.table()` is monkeypatched to serve
data/demo_batch/*.csv directly by name instead of hitting a real catalog, and
`DataFrameWriter.saveAsTable` is monkeypatched to capture the result
in-memory -- both sidestep Windows' well-known requirement of a
HADOOP_HOME/winutils.exe setup for any *persistent* table write, which has
nothing to do with what's actually being verified (the classification logic).

Requires a local JDK (tested with Temurin 17) and PYSPARK_PYTHON pointed at a
real python.exe, since PySpark's workers otherwise default to `python3`,
which doesn't exist on Windows. Example (PowerShell):

    $env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
    $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
    $env:PYSPARK_PYTHON = "<path to a real python.exe with pyspark installed>"
    $env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
    uv run python scripts/test_reconcile_local.py

If `uv run python` itself is blocked by a local Application Control policy
(seen on this machine right after installing new tooling), invoke the
underlying interpreter directly instead, e.g.:

    & "$env:USERPROFILE\AppData\Roaming\uv\python\cpython-3.12.14-windows-x86_64-none\python.exe" scripts/test_reconcile_local.py

with PYTHONPATH set to `.venv/Lib/site-packages` so it can still see the
project's installed packages.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "demo_batch"
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "02_reconcile_settlements.py"

orders_schema = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_amount", DoubleType(), False),
        StructField("order_date", DateType(), False),
        StructField("refund_amount", DoubleType(), True),
        StructField("refund_date", DateType(), True),
        StructField("expected_mdr_rate", DoubleType(), False),
    ]
)
settlement_report_schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("gross_amount", DoubleType(), False),
        StructField("mdr_fee", DoubleType(), False),
        StructField("gst_on_mdr", DoubleType(), False),
        StructField("refund_adjustment", DoubleType(), False),
        StructField("net_amount", DoubleType(), False),
        StructField("utr_number", StringType(), False),
        StructField("settlement_batch_id", StringType(), False),
    ]
)
bank_feed_schema = StructType(
    [
        StructField("utr_number", StringType(), False),
        StructField("bank_credit_amount", DoubleType(), False),
        StructField("credit_date", DateType(), False),
        StructField("settlement_batch_id", StringType(), False),
    ]
)
ground_truth_schema = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("exception_type", StringType(), False),
        StructField("expected_reasoning", StringType(), False),
        StructField("related_transaction_id", StringType(), True),
    ]
)


class FakeWidgets:
    def __init__(self):
        self._values = {}

    def text(self, name, default):
        self._values.setdefault(name, default)

    def get(self, name):
        return self._values[name]


class FakeDbutils:
    def __init__(self):
        self.widgets = FakeWidgets()


def main():
    spark = SparkSession.builder.master("local[2]").appName("reconcile-test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    tables = {
        "workspace.settletrace.orders": spark.read.csv(
            str(DATA_DIR / "orders.csv"), header=True, schema=orders_schema
        ),
        "workspace.settletrace.settlement_report": spark.read.csv(
            str(DATA_DIR / "settlement_report.csv"), header=True, schema=settlement_report_schema
        ),
        "workspace.settletrace.bank_feed": spark.read.csv(
            str(DATA_DIR / "bank_feed.csv"), header=True, schema=bank_feed_schema
        ),
        "workspace.settletrace.ground_truth": spark.read.csv(
            str(DATA_DIR / "ground_truth.csv"), header=True, schema=ground_truth_schema
        ),
    }
    spark.table = lambda name: tables[name]

    written_tables = {}

    def fake_save_as_table(self, name):
        written_tables[name] = self._df

    DataFrameWriter.saveAsTable = fake_save_as_table

    dbutils = FakeDbutils()
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema_name", "settletrace")
    dbutils.widgets.text("tolerance", "0.01")

    def display(df):
        df.show(30, truncate=False)

    source = NOTEBOOK_PATH.read_text(encoding="utf-8")
    exec_globals = {"dbutils": dbutils, "spark": spark, "display": display}
    exec(compile(source, str(NOTEBOOK_PATH), "exec"), exec_globals)  # noqa: S102 -- running our own notebook source, not untrusted input

    assert "workspace.settletrace.reconciliation_result" in written_tables, "result table wasn't written"

    total_orders = exec_globals["total_orders"]
    correct = exec_globals["correct"]
    mismatch_count = exec_globals["mismatch_count"]
    print(f"\n[test] total_orders={total_orders} correct={correct} mismatch_count={mismatch_count}")
    assert mismatch_count == 0, "expected 100% accuracy vs ground_truth"
    assert correct == total_orders
    print("[test] PASS")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
