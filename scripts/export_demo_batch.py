"""Export the canonical demo batch to fixed CSV files under data/demo_batch/.

notebooks/01_generate_synthetic_data.py is the single source of truth for the
generation logic. This script runs it outside Databricks — stubbing the
injected `dbutils` / `spark` / `display` globals with local equivalents — and
writes its output to CSV so the demo batch is a checked-in, reproducible file
rather than something regenerated (with the same seed, but on cluster state
that isn't ours to freeze) every time.

Usage: uv run python scripts/export_demo_batch.py
"""

import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "01_generate_synthetic_data.py"
OUTPUT_DIR = REPO_ROOT / "data" / "demo_batch"

# The canonical demo parameters: a fixed, reproducible 150-row batch.
NUM_ORDERS = "150"
SEED = "42"
SETTLEMENT_BATCH_SIZE = "25"


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


class FakeDataFrame:
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
        if "SHOW SCHEMAS" not in query:
            pass

    def createDataFrame(self, rows, schema):
        return FakeDataFrame(rows, schema)


def write_csv(rows: list[dict], columns: list[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: "" if row[col] is None else row[col] for col in columns})


def main():
    dbutils = FakeDbutils()
    dbutils.widgets.text("num_orders", NUM_ORDERS)
    dbutils.widgets.text("seed", SEED)
    dbutils.widgets.text("settlement_batch_size", SETTLEMENT_BATCH_SIZE)
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema_name", "settletrace")

    def display(_df):
        pass

    source = NOTEBOOK_PATH.read_text(encoding="utf-8")
    exec_globals = {"dbutils": dbutils, "spark": FakeSpark(), "display": display}
    exec(compile(source, str(NOTEBOOK_PATH), "exec"), exec_globals)  # noqa: S102 -- running our own notebook source, not untrusted input

    write_csv(
        exec_globals["orders"],
        ["order_id", "customer_id", "order_amount", "order_date", "refund_amount", "refund_date", "expected_mdr_rate"],
        OUTPUT_DIR / "orders.csv",
    )
    write_csv(
        exec_globals["settlement_report"],
        [
            "transaction_id", "order_id", "gross_amount", "mdr_fee", "gst_on_mdr",
            "refund_adjustment", "net_amount", "utr_number", "settlement_batch_id",
        ],
        OUTPUT_DIR / "settlement_report.csv",
    )
    write_csv(
        exec_globals["bank_feed"],
        ["utr_number", "bank_credit_amount", "credit_date", "settlement_batch_id"],
        OUTPUT_DIR / "bank_feed.csv",
    )
    write_csv(
        exec_globals["ground_truth"],
        ["order_id", "exception_type", "expected_reasoning", "related_transaction_id"],
        OUTPUT_DIR / "ground_truth.csv",
    )

    print(f"Wrote demo batch (num_orders={NUM_ORDERS}, seed={SEED}) to {OUTPUT_DIR}")


if __name__ == "__main__":
    sys.exit(main())
