# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Generate Synthetic Settlement Data
# MAGIC
# MAGIC Produces three linked, **clean** (no injected exceptions yet) tables that simulate a
# MAGIC lumped Razorpay-style settlement:
# MAGIC
# MAGIC - `orders` — the internal ledger: what the merchant's system thinks happened.
# MAGIC - `settlement_report` — per-order line items (gross amount, MDR fee, GST-on-MDR,
# MAGIC   refund adjustment, net amount) grouped into settlement batches.
# MAGIC - `bank_feed` — the lumped bank-side reality: one credit per settlement batch,
# MAGIC   landing T+2 after the batch's order date.
# MAGIC
# MAGIC Scale is controlled by the `num_orders` widget. Re-running is idempotent
# MAGIC (tables are overwritten each run).

# COMMAND ----------

import random
import uuid
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

dbutils.widgets.text("num_orders", "50")
dbutils.widgets.text("seed", "42")
dbutils.widgets.text("settlement_batch_size", "25")
dbutils.widgets.text("catalog", "hive_metastore")
dbutils.widgets.text("schema_name", "settletrace")

NUM_ORDERS = int(dbutils.widgets.get("num_orders"))
SEED = int(dbutils.widgets.get("seed"))
BATCH_SIZE = int(dbutils.widgets.get("settlement_batch_size"))
CATALOG = dbutils.widgets.get("catalog")
SCHEMA_NAME = dbutils.widgets.get("schema_name")

MDR_RATE = 0.02
GST_RATE = 0.18
REFUND_PROBABILITY = 0.15
SETTLEMENT_LAG_DAYS = 2

random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate `orders`
# MAGIC
# MAGIC Orders are laid out `BATCH_SIZE` per day, so grouping by order date later
# MAGIC produces the same batches used for settlement — mirroring a daily settlement cycle.

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
            "order_id": str(uuid.uuid4()),
            "customer_id": f"CUST-{random.randint(1, NUM_CUSTOMERS):05d}",
            "order_amount": order_amount,
            "order_date": order_date,
            "refund_amount": refund_amount,
            "refund_date": refund_date,
            "expected_mdr_rate": MDR_RATE,
            "_batch_index": batch_index,  # internal only, dropped before write
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate `settlement_report`
# MAGIC
# MAGIC One line per order, netting MDR fee, GST-on-MDR, and any refund out of the gross
# MAGIC amount. All orders in the same batch share one `utr_number`.

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
    gross_amount = o["order_amount"]
    mdr_fee = round(gross_amount * o["expected_mdr_rate"], 2)
    gst_on_mdr = round(mdr_fee * GST_RATE, 2)
    refund_adjustment = o["refund_amount"] if o["refund_amount"] is not None else 0.0
    net_amount = round(gross_amount - mdr_fee - gst_on_mdr - refund_adjustment, 2)

    settlement_report.append(
        {
            "transaction_id": str(uuid.uuid4()),
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate `bank_feed`
# MAGIC
# MAGIC One row per settlement batch: the lumped credit that actually lands, T+2 after
# MAGIC the batch's order date.

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
# MAGIC ## Validate — prove the batch is clean and linked before writing anything

# COMMAND ----------

order_ids = {o["order_id"] for o in orders}
settlement_order_ids = [s["order_id"] for s in settlement_report]

assert len(orders) == NUM_ORDERS, f"expected {NUM_ORDERS} orders, got {len(orders)}"
assert len(settlement_report) == NUM_ORDERS, "settlement_report must be 1:1 with orders"
assert set(settlement_order_ids) == order_ids, "every settlement line must reference a real order"
assert len(set(settlement_order_ids)) == len(settlement_order_ids), "no duplicate order_id in settlement_report"
assert len(bank_feed) == len(batch_indices), "one bank_feed row per settlement batch"

for row in orders:
    assert row["order_amount"] > 0
    if row["refund_amount"] is not None:
        assert row["refund_amount"] <= row["order_amount"], "refund cannot exceed order amount"
        assert row["refund_date"] is not None and row["refund_date"] >= row["order_date"]

net_by_batch = {}
for s in settlement_report:
    net_by_batch.setdefault(s["_batch_index"], 0.0)
    net_by_batch[s["_batch_index"]] += s["net_amount"]

batch_index_by_id = {v: k for k, v in batch_id_by_batch.items()}
for bf in bank_feed:
    batch_index = batch_index_by_id[bf["settlement_batch_id"]]
    expected = round(net_by_batch[batch_index], 2)
    assert abs(bf["bank_credit_amount"] - expected) < 0.01, (
        f"bank_feed credit for {bf['settlement_batch_id']} ({bf['bank_credit_amount']}) "
        f"does not match summed net_amount ({expected})"
    )

print(
    f"Validation passed: {len(orders)} orders, {len(settlement_report)} settlement lines, "
    f"{len(bank_feed)} bank credits across {len(batch_indices)} batches."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta tables
# MAGIC
# MAGIC Falls back to `hive_metastore` if the requested catalog isn't usable
# MAGIC (e.g. Unity Catalog isn't enabled on this workspace).

# COMMAND ----------

try:
    spark.sql(f"SHOW SCHEMAS IN {CATALOG}")
except Exception as e:
    print(f"Catalog '{CATALOG}' not usable ({e}); falling back to 'hive_metastore'")
    CATALOG = "hive_metastore"

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


def drop_internal_fields(rows):
    return [{k: v for k, v in row.items() if not k.startswith("_")} for row in rows]


orders_df = spark.createDataFrame(drop_internal_fields(orders), schema=orders_schema)
settlement_report_df = spark.createDataFrame(
    drop_internal_fields(settlement_report), schema=settlement_report_schema
)
bank_feed_df = spark.createDataFrame(bank_feed, schema=bank_feed_schema)

orders_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_NAME}.orders")
settlement_report_df.write.mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA_NAME}.settlement_report"
)
bank_feed_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_NAME}.bank_feed")

print(f"Wrote orders, settlement_report, bank_feed to {CATALOG}.{SCHEMA_NAME}")

# COMMAND ----------

display(orders_df.limit(10))

# COMMAND ----------

display(settlement_report_df.limit(10))

# COMMAND ----------

display(bank_feed_df.limit(10))
