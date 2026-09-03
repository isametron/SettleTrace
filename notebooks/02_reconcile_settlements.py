# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Reconcile Settlements (3-tier match)
# MAGIC
# MAGIC Reads only the "operational" tables a real reconciliation engine would ever
# MAGIC see — `orders`, `settlement_report`, `bank_feed` — and classifies every order
# MAGIC into one of three tiers, purely by recomputing what settlement *should* look
# MAGIC like from `orders` and comparing it against what `settlement_report` actually
# MAGIC shows. `ground_truth` (written by the generator notebook) is deliberately not
# MAGIC touched until the very last, clearly-separated "accuracy check" section —
# MAGIC everything above that point is what a real engine would have to do blind.
# MAGIC
# MAGIC Tiers:
# MAGIC - **exact** — the order has one settlement line and the full settlement
# MAGIC   identity ties out within tolerance:
# MAGIC   `net_amount == order_amount - mdr_fee - gst_on_mdr - refund_adjustment`,
# MAGIC   with the fee and GST also matching the agreed rate. All four figures are
# MAGIC   compared, not just the two that have exception types attached to them.
# MAGIC - **fuzzy** — the order has one settlement line, it's linked correctly, but a
# MAGIC   specific figure doesn't tie out: `mdr_rate_mismatch` (fee doesn't match the
# MAGIC   agreed rate), `timing_lag_refund` (order has a refund the settlement line
# MAGIC   doesn't reflect), or `unexplained_value_break` (fee and refund both tie out
# MAGIC   but the settlement identity still doesn't hold — a real break with no known
# MAGIC   cause, which would otherwise have passed as a clean match).
# MAGIC - **no_match** — the order can't be linked to settlement at all, structurally:
# MAGIC   `missing_payout` (zero settlement lines) or `duplicate_transaction` (two or
# MAGIC   more lines for the same order/transaction).
# MAGIC
# MAGIC A separate, batch-level check cross-references `bank_feed` against the sum of
# MAGIC `settlement_report.net_amount` per batch — the "multi-source" half of the
# MAGIC reconciliation, on top of the order-level checks above.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema_name", "settletrace")
dbutils.widgets.text("tolerance", "0.01")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TOLERANCE = float(dbutils.widgets.get("tolerance"))
GST_RATE = 0.18


def breaks_tolerance(actual, expected):
    """True when two rupee figures differ by *more* than TOLERANCE.

    The delta is rounded to paise before comparing, for two reasons that both
    bite in practice:

    1. Floating point. `17.51 - 17.5` is 0.010000000000001563, which is
       strictly greater than a 0.01 tolerance, so a difference of exactly one
       paisa would flag as a break without this.
    2. Rounding conventions genuinely differ across the pipeline. Python's
       `round()` is banker's rounding (HALF_EVEN) and Spark's `F.round()` is
       HALF_UP, so the generator and this engine can legitimately land one
       paisa apart on the same arithmetic -- three orders in the 150-order demo
       batch do exactly that on `gst_on_mdr`. A one-paisa rounding convention
       gap is precisely what a one-paisa tolerance exists to absorb; it is not
       a settlement break, and treating it as one would bury the real ones.
    """
    return F.round(F.abs(actual - expected), 2) > TOLERANCE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the operational tables (not `ground_truth`)

# COMMAND ----------

orders_df = spark.table(f"{CATALOG}.{SCHEMA_NAME}.orders")
settlement_df = spark.table(f"{CATALOG}.{SCHEMA_NAME}.settlement_report")
bank_feed_df = spark.table(f"{CATALOG}.{SCHEMA_NAME}.bank_feed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recompute the expected settlement line for every order

# COMMAND ----------

expected_df = (
    orders_df.withColumn(
        "expected_mdr_fee", F.round(F.col("order_amount") * F.col("expected_mdr_rate"), 2)
    )
    .withColumn("expected_gst_on_mdr", F.round(F.col("expected_mdr_fee") * F.lit(GST_RATE), 2))
    .withColumn("expected_refund_adjustment", F.coalesce(F.col("refund_amount"), F.lit(0.0)))
    .withColumn(
        "expected_net_amount",
        F.round(
            F.col("order_amount")
            - F.col("expected_mdr_fee")
            - F.col("expected_gst_on_mdr")
            - F.col("expected_refund_adjustment"),
            2,
        ),
    )
    .select(
        "order_id",
        "order_amount",
        "refund_amount",
        "expected_mdr_fee",
        "expected_gst_on_mdr",
        "expected_refund_adjustment",
        "expected_net_amount",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Structural shape of settlement per order
# MAGIC
# MAGIC How many settlement lines does each order actually have? 0 = missing payout,
# MAGIC 2+ = duplicate. For orders with exactly 1, `first(...)` picks that one line
# MAGIC (safe for the 2+ case too, since duplicates in this pipeline are always
# MAGIC verbatim copies of each other — a genuinely different-valued double payout
# MAGIC would need its own category, not something this batch injects).

# COMMAND ----------

settlement_per_order = settlement_df.groupBy("order_id").agg(
    F.count("*").alias("settlement_row_count"),
    F.first("transaction_id").alias("transaction_id"),
    F.first("mdr_fee").alias("mdr_fee"),
    F.first("gst_on_mdr").alias("gst_on_mdr"),
    F.first("refund_adjustment").alias("refund_adjustment"),
    F.first("net_amount").alias("net_amount"),
    F.first("utr_number").alias("utr_number"),
    F.first("settlement_batch_id").alias("settlement_batch_id"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classify
# MAGIC
# MAGIC Checks run in priority order: structural problems (missing/duplicate) are
# MAGIC decided before any value comparison, since a duplicated or absent line makes
# MAGIC a fee/refund comparison meaningless. Within a single linked line, MDR is
# MAGIC checked before refund — the two exception types never overlap on the same
# MAGIC order in this dataset, but the priority still needs to be well-defined.

# COMMAND ----------

joined = expected_df.join(settlement_per_order, "order_id", "left").withColumn(
    "settlement_row_count", F.coalesce(F.col("settlement_row_count"), F.lit(0))
)

classified = joined.withColumn(
    "category",
    F.when(F.col("settlement_row_count") == 0, F.lit("missing_payout"))
    .when(F.col("settlement_row_count") >= 2, F.lit("duplicate_transaction"))
    .when(
        breaks_tolerance(F.col("mdr_fee"), F.col("expected_mdr_fee")),
        F.lit("mdr_rate_mismatch"),
    )
    .when(
        breaks_tolerance(F.col("refund_adjustment"), F.col("expected_refund_adjustment")),
        F.lit("timing_lag_refund"),
    )
    # Fee and refund both tie out, so no known exception type explains this —
    # but the identity still has to hold on GST and on the net itself.
    .when(
        breaks_tolerance(F.col("gst_on_mdr"), F.col("expected_gst_on_mdr"))
        | breaks_tolerance(F.col("net_amount"), F.col("expected_net_amount")),
        F.lit("unexplained_value_break"),
    )
    .otherwise(F.lit("clean_match")),
).withColumn(
    "match_tier",
    F.when(F.col("category") == "clean_match", F.lit("exact"))
    .when(
        F.col("category").isin(
            "mdr_rate_mismatch", "timing_lag_refund", "unexplained_value_break"
        ),
        F.lit("fuzzy"),
    )
    .otherwise(F.lit("no_match")),
).withColumn(
    "reasoning",
    F.when(
        F.col("category") == "clean_match",
        F.lit("Settlement matches the expected MDR fee, GST, and refund adjustment exactly."),
    )
    .when(
        F.col("category") == "mdr_rate_mismatch",
        F.concat(
            F.lit("MDR fee charged ("),
            F.col("mdr_fee").cast("string"),
            F.lit(") does not match the agreed rate (expected "),
            F.col("expected_mdr_fee").cast("string"),
            F.lit(")."),
        ),
    )
    .when(
        F.col("category") == "timing_lag_refund",
        F.concat(
            F.lit("Order has a refund of "),
            F.col("refund_amount").cast("string"),
            F.lit(" not reflected in this settlement's refund_adjustment; likely pending the next cycle."),
        ),
    )
    .when(
        F.col("category") == "duplicate_transaction",
        F.concat(
            F.lit("Order has "),
            F.col("settlement_row_count").cast("string"),
            F.lit(" settlement lines for the same transaction — investigate duplicate payout."),
        ),
    )
    .when(
        F.col("category") == "missing_payout",
        F.lit("No settlement record found for this order in this batch."),
    )
    .when(
        F.col("category") == "unexplained_value_break",
        F.concat(
            F.lit("MDR fee and refund adjustment both tie out, but the settlement identity "),
            F.lit("does not: expected gst_on_mdr "),
            F.col("expected_gst_on_mdr").cast("string"),
            F.lit(" / net_amount "),
            F.col("expected_net_amount").cast("string"),
            F.lit(", actual gst_on_mdr "),
            F.col("gst_on_mdr").cast("string"),
            F.lit(" / net_amount "),
            F.col("net_amount").cast("string"),
            F.lit(". No known exception type explains this — investigate."),
        ),
    )
    .otherwise(F.lit("")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch-level cross-check: `bank_feed` vs. summed `settlement_report`
# MAGIC
# MAGIC The order-level checks above never touch `bank_feed`. This is the second,
# MAGIC independent leg of "multi-source" reconciliation: does what the bank actually
# MAGIC credited match what `settlement_report` says it should have, per batch?

# COMMAND ----------

batch_actual_net = settlement_df.groupBy("settlement_batch_id").agg(
    F.round(F.sum("net_amount"), 2).alias("computed_batch_net")
)
batch_check = (
    bank_feed_df.join(batch_actual_net, "settlement_batch_id", "left")
    .withColumn("batch_diff", F.round(F.col("bank_credit_amount") - F.col("computed_batch_net"), 2))
    .withColumn("batch_balanced", F.abs(F.col("batch_diff")) <= TOLERANCE)
)

unbalanced_batches = batch_check.filter(~F.col("batch_balanced")).count()
total_batches = batch_check.count()
print(f"Batch-level check: {total_batches - unbalanced_batches}/{total_batches} batches balanced.")
if unbalanced_batches:
    print("Unbalanced batches:")
    batch_check.filter(~F.col("batch_balanced")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match-rate / exception-count summary

# COMMAND ----------

total_orders = classified.count()
tier_counts = {row["match_tier"]: row["count"] for row in classified.groupBy("match_tier").count().collect()}
category_counts = {row["category"]: row["count"] for row in classified.groupBy("category").count().collect()}

exact_rate = tier_counts.get("exact", 0) / total_orders
auto_resolved_rate = (tier_counts.get("exact", 0) + tier_counts.get("fuzzy", 0)) / total_orders

print(f"Total orders: {total_orders}")
print(f"Match tiers: {tier_counts}")
print(f"  exact match rate:            {exact_rate:.1%}")
print(f"  auto-resolved (exact+fuzzy): {auto_resolved_rate:.1%}")
print(f"Categories: {category_counts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write `reconciliation_result`

# COMMAND ----------

result_df = classified.select(
    "order_id",
    "match_tier",
    "category",
    "reasoning",
    "order_amount",
    "expected_mdr_fee",
    F.col("mdr_fee").alias("actual_mdr_fee"),
    "expected_refund_adjustment",
    F.col("refund_adjustment").alias("actual_refund_adjustment"),
    "expected_net_amount",
    F.col("net_amount").alias("actual_net_amount"),
    "settlement_row_count",
    "transaction_id",
    "utr_number",
    "settlement_batch_id",
)

result_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_NAME}.reconciliation_result")
print(f"Wrote reconciliation_result to {CATALOG}.{SCHEMA_NAME}")

# COMMAND ----------

display(result_df.orderBy(F.col("match_tier") != "exact", "category").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accuracy check against `ground_truth` — test-only, not part of the engine
# MAGIC
# MAGIC Everything above this cell only ever reads `orders` / `settlement_report` /
# MAGIC `bank_feed`. This section reads `ground_truth` purely to grade the
# MAGIC classification above — it isn't a step a real reconciliation run would have.

# COMMAND ----------

ground_truth_df = spark.table(f"{CATALOG}.{SCHEMA_NAME}.ground_truth")

graded = result_df.join(
    ground_truth_df.select("order_id", F.col("exception_type").alias("true_category")),
    "order_id",
    "inner",
)
assert graded.count() == total_orders, "every order should have a ground_truth row to grade against"

correct = graded.filter(F.col("category") == F.col("true_category")).count()
accuracy = correct / total_orders
print(f"Category accuracy vs. ground_truth: {correct}/{total_orders} ({accuracy:.1%})")

mismatches = graded.filter(F.col("category") != F.col("true_category"))
mismatch_count = mismatches.count()
if mismatch_count:
    print(f"{mismatch_count} mismatches:")
    mismatches.select("order_id", "category", "true_category", "reasoning").show(
        mismatch_count, truncate=False
    )
else:
    print("No mismatches — every order's category matches its ground_truth label.")

# COMMAND ----------

display(
    graded.groupBy("true_category", "category")
    .count()
    .orderBy("true_category", "category")
)
