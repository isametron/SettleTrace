# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Generate Synthetic Settlement Data
# MAGIC
# MAGIC Produces four linked tables that simulate a lumped Razorpay-style settlement,
# MAGIC with realistic messiness injected on top of an otherwise clean batch:
# MAGIC
# MAGIC - `orders` — the internal ledger: what the merchant's system thinks happened.
# MAGIC - `settlement_report` — per-order line items (gross amount, MDR fee, GST-on-MDR,
# MAGIC   refund adjustment, net amount) grouped into settlement batches.
# MAGIC - `bank_feed` — the lumped bank-side reality: one credit per settlement batch,
# MAGIC   landing T+2 after the batch's order date.
# MAGIC - `ground_truth` — the answer key: which orders are exact matches vs. which of
# MAGIC   four exception types they hit, and why. Not something a real reconciliation
# MAGIC   engine gets to see — this exists purely to measure that engine's accuracy.
# MAGIC
# MAGIC Exception categories injected (the rest are clean matches):
# MAGIC - **timing_lag_refund** — the order has a refund, but it isn't netted into this
# MAGIC   settlement batch yet (expected to land next cycle).
# MAGIC - **mdr_rate_mismatch** — MDR was charged at a different rate than agreed.
# MAGIC - **duplicate_transaction** — the same settlement line appears twice.
# MAGIC - **missing_payout** — the order has no settlement line in this batch at all.
# MAGIC
# MAGIC Scale is controlled by the `num_orders` widget; exception rates by the
# MAGIC `*_rate` widgets. Everything is seeded (`seed` widget), so a given
# MAGIC `num_orders` + `seed` always reproduces byte-identical output — re-running
# MAGIC is idempotent (tables are overwritten each run).

# COMMAND ----------

import random
import uuid
from collections import Counter
from datetime import date, timedelta

from faker import Faker
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

dbutils.widgets.text("num_orders", "150")
dbutils.widgets.text("seed", "42")
dbutils.widgets.text("settlement_batch_size", "25")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema_name", "settletrace")
dbutils.widgets.text("timing_lag_rate", "0.04")
dbutils.widgets.text("mdr_mismatch_rate", "0.025")
dbutils.widgets.text("duplicate_rate", "0.015")
dbutils.widgets.text("missing_payout_rate", "0.015")

NUM_ORDERS = int(dbutils.widgets.get("num_orders"))
SEED = int(dbutils.widgets.get("seed"))
BATCH_SIZE = int(dbutils.widgets.get("settlement_batch_size"))
CATALOG = dbutils.widgets.get("catalog")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TIMING_LAG_RATE = float(dbutils.widgets.get("timing_lag_rate"))
MDR_MISMATCH_RATE = float(dbutils.widgets.get("mdr_mismatch_rate"))
DUPLICATE_RATE = float(dbutils.widgets.get("duplicate_rate"))
MISSING_PAYOUT_RATE = float(dbutils.widgets.get("missing_payout_rate"))

MDR_RATE = 0.02
MDR_MISMATCH_DELTA = 0.001  # charged rate = expected + this, e.g. 2% -> 2.1%
GST_RATE = 0.18
REFUND_PROBABILITY = 0.15
SETTLEMENT_LAG_DAYS = 2

random.seed(SEED)
fake = Faker()
Faker.seed(SEED)


def new_id() -> str:
    # uuid.uuid4() draws from os.urandom(), not Python's random module, so it
    # would ignore SEED and break reproducibility. This derives a UUID4 from
    # the seeded random module instead.
    return str(uuid.UUID(int=random.getrandbits(128), version=4))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate `orders`
# MAGIC
# MAGIC Orders are laid out `BATCH_SIZE` per day, so grouping by order date later
# MAGIC produces the same batches used for settlement — mirroring a daily settlement cycle.
# MAGIC ~15% get a natural refund a few days later; this is independent of the
# MAGIC `timing_lag_refund` exception injected below.

# COMMAND ----------

BASE_DATE = date(2026, 1, 5)
NUM_CUSTOMERS = max(1, NUM_ORDERS // 3)

orders = []
for i in range(NUM_ORDERS):
    batch_index = i // BATCH_SIZE
    order_date = BASE_DATE + timedelta(days=batch_index)
    order_amount = round(random.uniform(100.0, 5000.0), 2)

    has_refund = random.random() < REFUND_PROBABILITY
    if has_refund:
        refund_fraction = random.uniform(0.2, 1.0)
        refund_amount = round(order_amount * refund_fraction, 2)
        refund_date = order_date + timedelta(days=random.randint(1, 3))
    else:
        refund_amount = None
        refund_date = None

    orders.append(
        {
            "order_id": new_id(),
            "customer_id": f"CUST-{random.randint(1, NUM_CUSTOMERS):05d}",
            "order_amount": order_amount,
            "order_date": order_date,
            "refund_amount": refund_amount,
            "refund_date": refund_date,
            "expected_mdr_rate": MDR_RATE,
            "_batch_index": batch_index,  # internal only, dropped before write
        }
    )

orders_by_id = {o["order_id"]: o for o in orders}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assign exception labels
# MAGIC
# MAGIC Every order gets exactly one label — one of the four exception types, or
# MAGIC `clean_match`. Labels are assigned to disjoint, seeded-random subsets of
# MAGIC orders before settlement lines are generated, so the generation step below
# MAGIC can just branch on the label.

# COMMAND ----------

shuffled_order_ids = [o["order_id"] for o in orders]
random.shuffle(shuffled_order_ids)

n_timing_lag = round(TIMING_LAG_RATE * NUM_ORDERS)
n_mdr_mismatch = round(MDR_MISMATCH_RATE * NUM_ORDERS)
n_duplicate = round(DUPLICATE_RATE * NUM_ORDERS)
n_missing_payout = round(MISSING_PAYOUT_RATE * NUM_ORDERS)

# Kept as lists, not sets: iterating a set of strings has an order that
# depends on Python's per-process hash randomization, not just SEED. The
# timing-lag loop below draws from the shared `random` stream while
# iterating, so a randomized iteration order would silently make the output
# non-reproducible even with a fixed seed.
cursor = 0
timing_lag_ids = shuffled_order_ids[cursor : cursor + n_timing_lag]
cursor += n_timing_lag
mdr_mismatch_ids = shuffled_order_ids[cursor : cursor + n_mdr_mismatch]
cursor += n_mdr_mismatch
duplicate_ids = shuffled_order_ids[cursor : cursor + n_duplicate]
cursor += n_duplicate
missing_payout_ids = shuffled_order_ids[cursor : cursor + n_missing_payout]

exception_type_by_order_id = {}
for oid in timing_lag_ids:
    exception_type_by_order_id[oid] = "timing_lag_refund"
for oid in mdr_mismatch_ids:
    exception_type_by_order_id[oid] = "mdr_rate_mismatch"
for oid in duplicate_ids:
    exception_type_by_order_id[oid] = "duplicate_transaction"
for oid in missing_payout_ids:
    exception_type_by_order_id[oid] = "missing_payout"

# A timing-lag order must actually have a refund on it; force one if the
# natural 15% roll didn't already give it one.
for oid in timing_lag_ids:
    o = orders_by_id[oid]
    if o["refund_amount"] is None:
        refund_fraction = random.uniform(0.2, 1.0)
        o["refund_amount"] = round(o["order_amount"] * refund_fraction, 2)
        o["refund_date"] = o["order_date"] + timedelta(days=random.randint(1, 3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate `settlement_report`
# MAGIC
# MAGIC One line per order (skipped entirely for `missing_payout`), netting MDR fee,
# MAGIC GST-on-MDR, and any refund out of the gross amount. `mdr_rate_mismatch` charges
# MAGIC the wrong rate; `timing_lag_refund` deliberately fails to net the refund.
# MAGIC `duplicate_transaction` is applied afterwards, as a literal copy of an
# MAGIC existing line. All orders in the same batch share one `utr_number`.

# COMMAND ----------

batch_indices = sorted({o["_batch_index"] for o in orders})
utr_by_batch = {
    batch_index: f"UTR{batch_index:06d}{random.randint(100000, 999999)}"
    for batch_index in batch_indices
}
batch_id_by_batch = {
    batch_index: f"BATCH-{batch_index:04d}" for batch_index in batch_indices
}

settlement_report = []
for o in orders:
    exception_type = exception_type_by_order_id.get(o["order_id"], "clean_match")
    if exception_type == "missing_payout":
        continue

    gross_amount = o["order_amount"]
    charged_rate = o["expected_mdr_rate"]
    if exception_type == "mdr_rate_mismatch":
        charged_rate += MDR_MISMATCH_DELTA
    mdr_fee = round(gross_amount * charged_rate, 2)
    gst_on_mdr = round(mdr_fee * GST_RATE, 2)

    if exception_type == "timing_lag_refund":
        refund_adjustment = 0.0
    else:
        refund_adjustment = o["refund_amount"] if o["refund_amount"] is not None else 0.0

    net_amount = round(gross_amount - mdr_fee - gst_on_mdr - refund_adjustment, 2)

    settlement_report.append(
        {
            "transaction_id": new_id(),
            "order_id": o["order_id"],
            "gross_amount": gross_amount,
            "mdr_fee": mdr_fee,
            "gst_on_mdr": gst_on_mdr,
            "refund_adjustment": refund_adjustment,
            "net_amount": net_amount,
            "utr_number": utr_by_batch[o["_batch_index"]],
            "settlement_batch_id": batch_id_by_batch[o["_batch_index"]],
            "_batch_index": o["_batch_index"],  # internal only, dropped before write
        }
    )

settlement_by_order_id: dict[str, list] = {}
for s in settlement_report:
    settlement_by_order_id.setdefault(s["order_id"], []).append(s)

for oid in duplicate_ids:
    duplicate_row = dict(settlement_by_order_id[oid][0])  # same transaction_id, verbatim copy
    settlement_report.append(duplicate_row)
    settlement_by_order_id[oid].append(duplicate_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate `bank_feed`
# MAGIC
# MAGIC One row per settlement batch: the lumped credit that actually lands, T+2 after
# MAGIC the batch's order date. Computed from the (possibly messy) `settlement_report`
# MAGIC above, so a duplicate or a missing payout flows through into what the bank
# MAGIC actually credits — exactly like production.

# COMMAND ----------

bank_feed = []
for batch_index in batch_indices:
    batch_lines = [s for s in settlement_report if s["_batch_index"] == batch_index]
    batch_orders = [o for o in orders if o["_batch_index"] == batch_index]
    credit_date = max(o["order_date"] for o in batch_orders) + timedelta(
        days=SETTLEMENT_LAG_DAYS
    )

    bank_feed.append(
        {
            "utr_number": utr_by_batch[batch_index],
            "bank_credit_amount": round(
                sum(s["net_amount"] for s in batch_lines), 2
            ),
            "credit_date": credit_date,
            "settlement_batch_id": batch_id_by_batch[batch_index],
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build `ground_truth`
# MAGIC
# MAGIC The answer key: one row per order, with the label and a short human-readable
# MAGIC explanation — the same kind of reasoning the reconciliation agent itself should
# MAGIC eventually produce on the exceptions it finds.

# COMMAND ----------


def reasoning_for(exception_type, order, rows_for_order):
    if exception_type == "clean_match":
        return "Amounts reconcile exactly; no action needed."
    if exception_type == "timing_lag_refund":
        return (
            f"Refund of {order['refund_amount']} issued on {order['refund_date']} "
            "was not netted in this settlement batch; expected in the next cycle."
        )
    if exception_type == "mdr_rate_mismatch":
        charged_fee = rows_for_order[0]["mdr_fee"]
        expected_fee = round(order["order_amount"] * order["expected_mdr_rate"], 2)
        return (
            f"MDR fee charged ({charged_fee}) does not match the agreed rate "
            f"(expected {expected_fee})."
        )
    if exception_type == "duplicate_transaction":
        return (
            f"Settlement contains a duplicate entry for transaction_id "
            f"{rows_for_order[0]['transaction_id']}."
        )
    if exception_type == "missing_payout":
        return "No settlement record found for this order in this batch."
    raise ValueError(f"unknown exception_type: {exception_type}")


ground_truth = []
for o in orders:
    exception_type = exception_type_by_order_id.get(o["order_id"], "clean_match")
    rows_for_order = settlement_by_order_id.get(o["order_id"], [])
    ground_truth.append(
        {
            "order_id": o["order_id"],
            "exception_type": exception_type,
            "expected_reasoning": reasoning_for(exception_type, o, rows_for_order),
            "related_transaction_id": rows_for_order[0]["transaction_id"] if rows_for_order else None,
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate
# MAGIC
# MAGIC Every exception category is checked against what the raw data actually shows
# MAGIC (not just against its own label), so a bug in the injection logic would fail
# MAGIC loudly here rather than silently mislabeling the demo batch. The printed
# MAGIC samples at the end are for a manual spot-check on top of that.

# COMMAND ----------

assert len(orders) == NUM_ORDERS, f"expected {NUM_ORDERS} orders, got {len(orders)}"
assert len(ground_truth) == NUM_ORDERS, "ground_truth must have exactly one row per order"

label_counts = Counter(g["exception_type"] for g in ground_truth)
expected_counts = {
    "timing_lag_refund": n_timing_lag,
    "mdr_rate_mismatch": n_mdr_mismatch,
    "duplicate_transaction": n_duplicate,
    "missing_payout": n_missing_payout,
}
for label, expected_count in expected_counts.items():
    actual_count = label_counts.get(label, 0)
    assert actual_count == expected_count, f"{label}: expected {expected_count}, got {actual_count}"

clean_fraction = label_counts.get("clean_match", 0) / NUM_ORDERS
assert clean_fraction >= 0.90 - 1e-9, f"expected >=90% clean_match, got {clean_fraction:.1%}"

for g in ground_truth:
    order = orders_by_id[g["order_id"]]
    rows = settlement_by_order_id.get(g["order_id"], [])
    label = g["exception_type"]

    if label == "missing_payout":
        assert len(rows) == 0, f"{g['order_id']} labeled missing_payout but has settlement rows"
        continue
    if label == "duplicate_transaction":
        assert len(rows) == 2, f"{g['order_id']} labeled duplicate_transaction but has {len(rows)} rows"
        assert rows[0]["transaction_id"] == rows[1]["transaction_id"]
    else:
        assert len(rows) == 1, f"{g['order_id']} labeled {label} but has {len(rows)} settlement rows"

    if label == "timing_lag_refund":
        assert order["refund_amount"] is not None, f"{g['order_id']} labeled timing_lag_refund but has no refund"
        assert rows[0]["refund_adjustment"] == 0.0
    elif label == "mdr_rate_mismatch":
        expected_fee = round(order["order_amount"] * order["expected_mdr_rate"], 2)
        assert abs(rows[0]["mdr_fee"] - expected_fee) > 0.01, f"{g['order_id']} labeled mdr_rate_mismatch but fee matches"
    elif label == "clean_match":
        expected_refund = order["refund_amount"] if order["refund_amount"] is not None else 0.0
        expected_fee = round(order["order_amount"] * order["expected_mdr_rate"], 2)
        assert abs(rows[0]["refund_adjustment"] - expected_refund) < 0.01
        assert abs(rows[0]["mdr_fee"] - expected_fee) < 0.01

net_by_batch: dict[int, float] = {}
for s in settlement_report:
    net_by_batch[s["_batch_index"]] = net_by_batch.get(s["_batch_index"], 0.0) + s["net_amount"]

batch_index_by_id = {v: k for k, v in batch_id_by_batch.items()}
for bf in bank_feed:
    batch_index = batch_index_by_id[bf["settlement_batch_id"]]
    expected = round(net_by_batch[batch_index], 2)
    assert abs(bf["bank_credit_amount"] - expected) < 0.01, (
        f"bank_feed credit for {bf['settlement_batch_id']} ({bf['bank_credit_amount']}) "
        f"does not match summed net_amount ({expected})"
    )

print(
    f"Validation passed: {len(orders)} orders, {len(settlement_report)} settlement lines "
    f"(incl. duplicates), {len(bank_feed)} bank credits.\n"
    f"Label distribution: {dict(label_counts)} ({clean_fraction:.1%} clean)"
)

print("\nSample rows per exception category (for spot-checking):")
for label in [
    "clean_match",
    "timing_lag_refund",
    "mdr_rate_mismatch",
    "duplicate_transaction",
    "missing_payout",
]:
    for g in [row for row in ground_truth if row["exception_type"] == label][:2]:
        print(f"  [{label}] order={g['order_id'][:8]}...  {g['expected_reasoning']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta tables
# MAGIC
# MAGIC Falls back to `workspace` if the requested catalog isn't usable
# MAGIC (e.g. this workspace is Unity-Catalog-only and has no `hive_metastore`).

# COMMAND ----------

try:
    spark.sql(f"SHOW SCHEMAS IN {CATALOG}")
except Exception as e:
    print(f"Catalog '{CATALOG}' not usable ({e}); falling back to 'workspace'")
    CATALOG = "workspace"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_NAME}")

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


def drop_internal_fields(rows):
    return [{k: v for k, v in row.items() if not k.startswith("_")} for row in rows]


orders_df = spark.createDataFrame(drop_internal_fields(orders), schema=orders_schema)
settlement_report_df = spark.createDataFrame(
    drop_internal_fields(settlement_report), schema=settlement_report_schema
)
bank_feed_df = spark.createDataFrame(bank_feed, schema=bank_feed_schema)
ground_truth_df = spark.createDataFrame(ground_truth, schema=ground_truth_schema)

orders_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_NAME}.orders")
settlement_report_df.write.mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA_NAME}.settlement_report"
)
bank_feed_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_NAME}.bank_feed")
ground_truth_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_NAME}.ground_truth")

print(f"Wrote orders, settlement_report, bank_feed, ground_truth to {CATALOG}.{SCHEMA_NAME}")

# COMMAND ----------

display(orders_df.limit(10))

# COMMAND ----------

display(settlement_report_df.limit(10))

# COMMAND ----------

display(bank_feed_df.limit(10))

# COMMAND ----------

display(ground_truth_df.filter("exception_type != 'clean_match'"))
