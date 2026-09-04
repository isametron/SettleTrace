# Databricks notebook source
# MAGIC %md
# MAGIC # 03: Exception Reasoning and Audit Trail
# MAGIC
# MAGIC Stage 3 and 4 of the SettleTrace pipeline, running entirely on Databricks.
# MAGIC
# MAGIC Reads `reconciliation_result` (written by `02_reconcile_settlements.py`) and,
# MAGIC for every order the deterministic engine **flagged**, asks an LLM to diagnose
# MAGIC the cause *independently*, without being told the engine's own category,
# MAGIC via a Foundation Model API serving endpoint in this workspace. A small
# MAGIC deterministic sample of *clean* orders goes through too, as controls, so the
# MAGIC agent's false-positive behaviour is measured rather than assumed.
# MAGIC
# MAGIC Everything lands in the `audit_log` Delta table: one row per order carrying the
# MAGIC engine's verdict and the figures behind it, the agent's diagnosis and
# MAGIC recommendation, whether the two agree, and, on every row, an explicit
# MAGIC `autonomous_action_taken = false`.
# MAGIC
# MAGIC ## The governance model
# MAGIC
# MAGIC - The **deterministic engine is authoritative**. It decides the category.
# MAGIC - The **agent is advisory**. It explains and recommends; it never overrides,
# MAGIC   and it can never clear work, only add doubt.
# MAGIC - **Disagreement escalates** to human review rather than resolving toward
# MAGIC   either side.
# MAGIC - **Nothing is actioned.** No ledger entry, payout, refund, or write-back to
# MAGIC   any source system happens here.
# MAGIC
# MAGIC The prompt, response schema, and case-builder are imported from
# MAGIC `scripts/reasoning_agent.py`, and the audit record schema from
# MAGIC `scripts/audit_trail.py`. These are the same modules the local runner
# MAGIC (`scripts/run_pipeline.py`) uses, so cluster and laptop cannot drift apart.

# COMMAND ----------

# MAGIC %pip install -q openai pydantic

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import the shared reasoning + audit modules from the repo
# MAGIC
# MAGIC This notebook lives in `notebooks/` inside a Databricks Git folder, so the
# MAGIC repo's `scripts/` directory is one level up from the working directory.
# MAGIC Importing it (rather than restating the prompt here) is what keeps one source
# MAGIC of truth for the prompt across the cluster and the local runner.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.getcwd())
SCRIPTS_DIR = os.path.join(REPO_ROOT, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

import audit_trail
import reasoning_agent
from audit_trail import (
    AuditRecord,
    EngineEvidence,
    decide_review,
    utc_now_iso,
)
from reasoning_agent import DiagnosisFailed, ReasoningAgent

print(f"repo root: {REPO_ROOT}")
print(f"prompt version: {reasoning_agent.PROMPT_VERSION}")

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema_name", "settletrace")
dbutils.widgets.text("endpoint", reasoning_agent.DEFAULT_DATABRICKS_ENDPOINT)
dbutils.widgets.text("clean_controls", "5")
dbutils.widgets.text("max_flagged", "0")
dbutils.widgets.text("temperature", "0.0")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
ENDPOINT = dbutils.widgets.get("endpoint")
CLEAN_CONTROLS = int(dbutils.widgets.get("clean_controls"))
# 0 means "every flagged order"; a small number is for smoke-testing the notebook
# without paying for a full pass.
MAX_FLAGGED = int(dbutils.widgets.get("max_flagged"))
TEMPERATURE = float(dbutils.widgets.get("temperature"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the engine's output and the operational tables
# MAGIC
# MAGIC `ground_truth` is deliberately not read here. It appears only in the
# MAGIC clearly-separated grading section at the very bottom, same boundary
# MAGIC `02_reconcile_settlements.py` draws.

# COMMAND ----------

import uuid

run_id = uuid.uuid4().hex[:12]
run_started_at = utc_now_iso()

result_rows = [
    row.asDict()
    for row in spark.table(f"{CATALOG}.{SCHEMA_NAME}.reconciliation_result").collect()
]
orders = [row.asDict() for row in spark.table(f"{CATALOG}.{SCHEMA_NAME}.orders").collect()]
settlement_report = [
    row.asDict() for row in spark.table(f"{CATALOG}.{SCHEMA_NAME}.settlement_report").collect()
]
bank_feed = [row.asDict() for row in spark.table(f"{CATALOG}.{SCHEMA_NAME}.bank_feed").collect()]

print(f"run_id={run_id}")
print(
    f"reconciliation_result={len(result_rows)}  orders={len(orders)}  "
    f"settlement_report={len(settlement_report)}  bank_feed={len(bank_feed)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `build_case` expects strings, Delta gives typed values
# MAGIC
# MAGIC The shared case-builder was written against the demo-batch CSVs, where every
# MAGIC field arrives as a string. Reading Delta gives real doubles/dates instead, so
# MAGIC the rows are normalised to the CSV shape here rather than by loosening
# MAGIC `build_case`. The local runner and this notebook then feed the model
# MAGIC byte-identical prompts, which is the whole point of sharing the module.

# COMMAND ----------


def stringify(row: dict) -> dict:
    return {key: ("" if value is None else str(value)) for key, value in row.items()}


orders_s = [stringify(row) for row in orders]
settlement_s = [stringify(row) for row in settlement_report]
bank_feed_s = [stringify(row) for row in bank_feed]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select what the agent looks at
# MAGIC
# MAGIC Every flagged order, plus a seeded sample of clean ones. The seed is fixed so
# MAGIC two runs over the same batch review the same controls and stay comparable.

# COMMAND ----------

import random

CONTROL_SAMPLE_SEED = 42

rows_by_id = {row["order_id"]: row for row in result_rows}
flagged_ids = sorted(r["order_id"] for r in result_rows if r["category"] != "clean_match")
clean_ids = sorted(r["order_id"] for r in result_rows if r["category"] == "clean_match")

if MAX_FLAGGED > 0:
    flagged_ids = flagged_ids[:MAX_FLAGGED]

if CLEAN_CONTROLS <= 0:
    control_ids = []
elif CLEAN_CONTROLS >= len(clean_ids):
    control_ids = clean_ids
else:
    control_ids = sorted(random.Random(CONTROL_SAMPLE_SEED).sample(clean_ids, CLEAN_CONTROLS))

to_reason = [(oid, "flagged_by_engine") for oid in flagged_ids] + [
    (oid, "clean_control_sample") for oid in control_ids
]
print(f"{len(flagged_ids)} flagged + {len(control_ids)} clean controls = {len(to_reason)} to diagnose")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnose
# MAGIC
# MAGIC `ReasoningAgent(backend="databricks")` resolves workspace auth from the
# MAGIC notebook's own identity and talks to the serving endpoint over the
# MAGIC OpenAI-compatible protocol.

# COMMAND ----------

agent = ReasoningAgent(backend="databricks", model=ENDPOINT, temperature=TEMPERATURE)
print(f"endpoint: {agent.model} @ {agent.base_url}")

diagnoses = {}
total_latency_ms = 0

for index, (order_id, selection_reason) in enumerate(to_reason, start=1):
    case = reasoning_agent.build_case(order_id, orders_s, settlement_s, bank_feed_s)
    engine_category = rows_by_id[order_id]["category"]
    label = "flagged" if selection_reason == "flagged_by_engine" else "control"

    try:
        diagnosis, latency_ms = agent.diagnose(case)
    except DiagnosisFailed as e:
        # Logged against the order, never dropped: an audit trail that silently
        # omits the orders the agent choked on is worse than useless.
        diagnoses[order_id] = {
            "error": str(e),
            "case_fingerprint": reasoning_agent.case_fingerprint(case),
        }
        print(f"[{index}/{len(to_reason)}] {order_id[:8]} ({label}) DIAGNOSIS FAILED: {e}")
        continue

    total_latency_ms += latency_ms
    diagnoses[order_id] = {
        "diagnosis": diagnosis,
        "latency_ms": latency_ms,
        "case_fingerprint": reasoning_agent.case_fingerprint(case),
    }
    marker = "agree   " if diagnosis.cause == engine_category else "DISAGREE"
    print(
        f"[{index}/{len(to_reason)}] {order_id[:8]} ({label:7}) "
        f"engine={engine_category:22} agent={diagnosis.cause:22} "
        f"conf={diagnosis.confidence:.2f} {marker} {latency_ms:>6}ms"
    )

print(f"\nDiagnosed {len(diagnoses)} orders in {total_latency_ms / 1000:.1f}s of model time.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the audit records
# MAGIC
# MAGIC One per order, including the 130-odd the agent never saw, which are recorded
# MAGIC as auto-cleared with the reason they weren't escalated. A log that only
# MAGIC contains exceptions can't answer "was this order looked at?".

# COMMAND ----------

selection_by_id = dict(to_reason)
records = []
tier_order = {"no_match": 0, "fuzzy": 1, "exact": 2}

for row in sorted(result_rows, key=lambda r: (tier_order[r["match_tier"]], r["order_id"])):
    order_id = row["order_id"]
    selection_reason = selection_by_id.get(order_id, "not_selected")
    entry = diagnoses.get(order_id)

    agent_fields = {}
    agreement = "not_assessed"
    skip_reason = None
    agent_error = None

    if entry is None:
        if selection_reason == "not_selected":
            skip_reason = (
                "engine matched this order cleanly and it was not drawn as a control; "
                "advisory diagnosis is reserved for flagged orders"
            )
        else:
            skip_reason = "not reached (run limited via max_flagged)"
    elif "error" in entry:
        agent_error = entry["error"]
        agent_fields = {
            "agent_invoked": True,
            "agent_model": agent.model,
            "agent_prompt_version": reasoning_agent.PROMPT_VERSION,
            "agent_case_fingerprint": entry["case_fingerprint"],
        }
    else:
        diagnosis = entry["diagnosis"]
        agreement = "agree" if diagnosis.cause == row["category"] else "disagree"
        agent_fields = {
            "agent_invoked": True,
            "agent_cause": diagnosis.cause,
            "agent_explanation": diagnosis.explanation,
            "agent_confidence": diagnosis.confidence,
            "agent_recommended_action": diagnosis.recommended_action,
            "agent_model": agent.model,
            "agent_prompt_version": reasoning_agent.PROMPT_VERSION,
            "agent_case_fingerprint": entry["case_fingerprint"],
            "agent_latency_ms": entry["latency_ms"],
        }

    review_status, review_reason = decide_review(row["category"], agreement, agent_error)

    records.append(
        AuditRecord(
            run_id=run_id,
            run_started_at=run_started_at,
            order_id=order_id,
            engine_match_tier=row["match_tier"],
            engine_category=row["category"],
            engine_reasoning=row["reasoning"],
            engine_evidence=EngineEvidence(
                order_amount=row.get("order_amount"),
                expected_mdr_fee=row.get("expected_mdr_fee"),
                actual_mdr_fee=row.get("actual_mdr_fee"),
                expected_refund_adjustment=row.get("expected_refund_adjustment"),
                actual_refund_adjustment=row.get("actual_refund_adjustment"),
                expected_net_amount=row.get("expected_net_amount"),
                actual_net_amount=row.get("actual_net_amount"),
                settlement_row_count=row.get("settlement_row_count"),
                transaction_id=row.get("transaction_id"),
                settlement_batch_id=row.get("settlement_batch_id"),
            ),
            agent_selection_reason=selection_reason,
            agent_skip_reason=skip_reason,
            agent_error=agent_error,
            agent_explanation_cites_figures=audit_trail.explanation_cites_figures(
                agent_fields.get("agent_explanation")
            ),
            engine_agent_agreement=agreement,
            review_status=review_status,
            review_reason=review_reason,
            **agent_fields,
        )
    )

print(f"Built {len(records)} audit records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the `audit_log` Delta table
# MAGIC
# MAGIC Explicit schema rather than inference: most agent columns are null for the
# MAGIC ~90% of orders that matched cleanly, and Spark would infer those as void and
# MAGIC fail the write on a run where nothing was flagged.

# COMMAND ----------

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

AUDIT_COLUMN_TYPES = {
    "agent_invoked": BooleanType(),
    "agent_explanation_cites_figures": BooleanType(),
    "autonomous_action_taken": BooleanType(),
    "agent_confidence": DoubleType(),
    "agent_latency_ms": LongType(),
    "evidence_order_amount": DoubleType(),
    "evidence_expected_mdr_fee": DoubleType(),
    "evidence_actual_mdr_fee": DoubleType(),
    "evidence_expected_refund_adjustment": DoubleType(),
    "evidence_actual_refund_adjustment": DoubleType(),
    "evidence_expected_net_amount": DoubleType(),
    "evidence_actual_net_amount": DoubleType(),
    "evidence_settlement_row_count": IntegerType(),
}

columns = audit_trail.flat_column_names()
audit_schema = StructType(
    [StructField(name, AUDIT_COLUMN_TYPES.get(name, StringType()), True) for name in columns]
)

flat_rows = [audit_trail.record_to_flat_dict(record) for record in records]
audit_df = spark.createDataFrame(
    [tuple(row.get(name) for name in columns) for row in flat_rows], schema=audit_schema
)

audit_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{SCHEMA_NAME}.audit_log"
)
print(f"Wrote audit_log ({audit_df.count()} rows) to {CATALOG}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run summary

# COMMAND ----------

from pyspark.sql import functions as F

needs_review = sum(1 for r in records if r.review_status == "needs_human_review")
auto_cleared = sum(1 for r in records if r.review_status == "auto_cleared")
agreement_on_flagged = {}
for r in records:
    if r.agent_selection_reason == "flagged_by_engine":
        agreement_on_flagged[r.engine_agent_agreement] = (
            agreement_on_flagged.get(r.engine_agent_agreement, 0) + 1
        )

print(f"run_id:                {run_id}")
print(f"endpoint:              {agent.model}")
print(f"prompt version:        {reasoning_agent.PROMPT_VERSION}")
print(f"orders:                {len(records)}")
print(f"agent invoked on:      {sum(1 for r in records if r.agent_invoked)}")
print(f"agent failures:        {sum(1 for r in records if r.agent_error)}")
print(f"agreement (flagged):   {agreement_on_flagged}")
print(f"needs human review:    {needs_review}")
print(f"auto-cleared:          {auto_cleared}")
print(f"autonomous actions:    0  ({audit_trail.ACTION_POLICY})")

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SCHEMA_NAME}.audit_log")
    .filter(F.col("review_status") == "needs_human_review")
    .select(
        "order_id",
        "engine_category",
        "agent_cause",
        "engine_agent_agreement",
        "agent_confidence",
        "agent_explanation",
        "agent_recommended_action",
        "action_taken",
        "autonomous_action_taken",
    )
    .orderBy("engine_category", "order_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accuracy check against `ground_truth`: test-only, not part of the pipeline
# MAGIC
# MAGIC Everything above reads only `reconciliation_result` and the operational
# MAGIC tables. This section reads the answer key purely to grade the agent, exactly
# MAGIC as `02_reconcile_settlements.py` does for the engine. It is not a step a real
# MAGIC reconciliation run would have.

# COMMAND ----------

truth = {
    row["order_id"]: row["exception_type"]
    for row in spark.table(f"{CATALOG}.{SCHEMA_NAME}.ground_truth").collect()
}

graded = [r for r in records if r.agent_invoked and r.agent_cause]
agent_correct = sum(1 for r in graded if r.agent_cause == truth.get(r.order_id))
engine_correct = sum(1 for r in records if r.engine_category == truth.get(r.order_id))

print(f"engine: {engine_correct}/{len(records)} ({engine_correct / len(records):.1%})")
if graded:
    print(
        f"agent:  {agent_correct}/{len(graded)} ({agent_correct / len(graded):.1%}) "
        f"on the orders it was asked about"
    )
    for r in graded:
        if r.agent_cause != truth.get(r.order_id):
            print(
                f"  MISS {r.order_id[:8]}: truth={truth.get(r.order_id)} "
                f"agent={r.agent_cause} conf={r.agent_confidence}"
            )
