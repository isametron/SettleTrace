r"""Shared local-Spark harness for running notebook 02 against the frozen demo batch.

Notebook 02's whole point is Spark join/aggregation logic, so it can't be
credibly tested by faking `spark` the way notebook 01 is (see
scripts/export_demo_batch.py). Instead a real local SparkSession runs the real
notebook source, with two targeted monkeypatches:

- `spark.table()` serves data/demo_batch/*.csv by exact dotted table name,
  instead of hitting a real Unity Catalog.
- `DataFrameWriter.saveAsTable` captures the DataFrame in memory instead of
  writing it.

Both sidestep Windows' HADOOP_HOME/winutils.exe requirement for *persistent*
table writes, which has nothing to do with the logic under test.

This module exists so scripts/test_reconcile_local.py (correctness test) and
scripts/run_pipeline.py (end-to-end pipeline) share one copy of the table
schemas and the notebook-execution shim, rather than each carrying their own.

Requires a local JDK (tested with Temurin 17) and PYSPARK_PYTHON pointed at a
real python.exe, since PySpark's workers otherwise default to `python3`, which
doesn't exist on Windows. Example (PowerShell):

    $env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
    $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
    $env:PYSPARK_PYTHON = "<path to a real python.exe with pyspark installed>"
    $env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "demo_batch"
RECONCILE_NOTEBOOK = REPO_ROOT / "notebooks" / "02_reconcile_settlements.py"

DEFAULT_CATALOG = "workspace"
DEFAULT_SCHEMA = "settletrace"

ORDERS_SCHEMA = StructType(
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
SETTLEMENT_REPORT_SCHEMA = StructType(
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
BANK_FEED_SCHEMA = StructType(
    [
        StructField("utr_number", StringType(), False),
        StructField("bank_credit_amount", DoubleType(), False),
        StructField("credit_date", DateType(), False),
        StructField("settlement_batch_id", StringType(), False),
    ]
)
GROUND_TRUTH_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("exception_type", StringType(), False),
        StructField("expected_reasoning", StringType(), False),
        StructField("related_transaction_id", StringType(), True),
    ]
)

DEMO_TABLE_SCHEMAS = {
    "orders": ORDERS_SCHEMA,
    "settlement_report": SETTLEMENT_REPORT_SCHEMA,
    "bank_feed": BANK_FEED_SCHEMA,
    "ground_truth": GROUND_TRUTH_SCHEMA,
}


class FakeWidgets:
    """Stand-in for `dbutils.widgets`, with the same text/get surface the notebooks use."""

    def __init__(self, values: dict | None = None):
        self._values = dict(values or {})

    def text(self, name, default):
        self._values.setdefault(name, default)

    def get(self, name):
        return self._values[name]


class FakeLibrary:
    """`dbutils.library.restartPython()` is a no-op outside Databricks.

    The notebooks call it after their `%pip install` cell; locally the packages
    are already in the venv, so there is nothing to restart.
    """

    def restartPython(self):
        pass


class FakeDbutils:
    def __init__(self, widget_values: dict | None = None):
        self.widgets = FakeWidgets(widget_values)
        self.library = FakeLibrary()


def check_java_available() -> str | None:
    """Return an actionable message if a local JDK isn't visible, else None.

    PySpark's failure mode without a JDK is a long opaque gateway traceback, so
    it's worth catching the common cause up front.
    """
    if os.environ.get("JAVA_HOME"):
        return None
    from shutil import which

    if which("java"):
        return None
    return (
        "No JDK found (JAVA_HOME unset and `java` not on PATH). Local Spark needs one. "
        "see this module's docstring for the environment variables to set."
    )


def build_local_spark(app_name: str = "settletrace-local") -> SparkSession:
    """A small local SparkSession, quiet enough to read pipeline output around."""
    spark = SparkSession.builder.master("local[2]").appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_demo_tables(
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
    schema_name: str = DEFAULT_SCHEMA,
    data_dir: Path = DATA_DIR,
) -> dict:
    """Read a batch of CSVs, keyed by the dotted table name the notebooks use.

    `data_dir` defaults to the frozen demo batch. scripts/scale_test.py points it
    at a larger generated batch instead, so throughput is measured against the
    same notebook code the correctness test runs.
    """
    return {
        f"{catalog}.{schema_name}.{table}": spark.read.csv(
            str(data_dir / f"{table}.csv"), header=True, schema=schema
        )
        for table, schema in DEMO_TABLE_SCHEMAS.items()
    }


def run_reconciliation_notebook(
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
    schema_name: str = DEFAULT_SCHEMA,
    tolerance: str = "0.01",
    quiet: bool = False,
    data_dir: Path = DATA_DIR,
) -> tuple[dict, dict]:
    """Execute notebooks/02_reconcile_settlements.py against the demo batch.

    Returns `(exec_globals, written_tables)`: the notebook's own module-level
    names (so callers can read `total_orders`, `correct`, etc.) and whatever it
    tried to `saveAsTable`, keyed by dotted table name.

    Running the real notebook source, rather than a reimplementation of it,
    keeps one source of truth for the reconciliation logic, the same reason
    scripts/export_demo_batch.py exec's notebook 01 instead of copying it.
    """
    tables = load_demo_tables(spark, catalog, schema_name, data_dir)
    spark.table = lambda name: tables[name]

    written_tables = {}

    def fake_save_as_table(self, name):
        written_tables[name] = self._df

    DataFrameWriter.saveAsTable = fake_save_as_table

    dbutils = FakeDbutils(
        {"catalog": catalog, "schema_name": schema_name, "tolerance": tolerance}
    )

    def display(df):
        if not quiet:
            df.show(30, truncate=False)

    source = RECONCILE_NOTEBOOK.read_text(encoding="utf-8")
    exec_globals = {"dbutils": dbutils, "spark": spark, "display": display}
    if quiet:
        # The notebook prints its own summaries; a pipeline run has its own.
        original_stdout = sys.stdout
        sys.stdout = open(os.devnull, "w", encoding="utf-8")  # noqa: SIM115
        try:
            exec(compile(source, str(RECONCILE_NOTEBOOK), "exec"), exec_globals)  # noqa: S102  # our own notebook source, not untrusted input
        finally:
            sys.stdout.close()
            sys.stdout = original_stdout
    else:
        exec(compile(source, str(RECONCILE_NOTEBOOK), "exec"), exec_globals)  # noqa: S102  # our own notebook source, not untrusted input

    return exec_globals, written_tables
