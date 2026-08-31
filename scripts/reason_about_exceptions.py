"""Day 4, part 1 — manual prototype of the LLM reasoning layer.

Feeds a handful of hand-picked rows from the frozen demo batch
(data/demo_batch/) to a local model and asks it to *independently* diagnose
the cause of each order's settlement exception — cause + explanation +
confidence + recommended action — without telling it the deterministic rule
engine's own category (notebooks/02_reconcile_settlements.py). That keeps
this a genuine second opinion rather than the model just restating a label
it was handed, so its output is worth comparing against both the rule
engine's classification and ground_truth.

Targets a local OpenAI-compatible server (tested against Bionic AI Studio /
LM Studio-style servers running Qwen2.5-7B-Instruct) rather than a hosted
API, so this costs nothing to run repeatedly. Point LOCAL_MODEL_BASE_URL /
LOCAL_MODEL_NAME below at whatever you have loaded.

Deliberately not automated across the full batch yet: one order per
category (four exception types + one clean control), printed for manual
review. Automating this into the reconciliation pipeline is a later step
once the prompt itself is validated here.

Usage: uv run python scripts/reason_about_exceptions.py
"""

import csv
import json
import sys
from pathlib import Path
from typing import Literal

from openai import APIConnectionError, APIStatusError, OpenAI
from pydantic import BaseModel, Field, field_validator

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "demo_batch"

GST_RATE = 0.18
LOCAL_MODEL_BASE_URL = "http://localhost:1234/v1"
LOCAL_MODEL_NAME = "qwen2.5-7b-instruct"

SYSTEM_PROMPT = """You are a settlement reconciliation analyst at a payments company.

Background: a single lumped bank credit ("bank_feed") pays out many orders at \
once, netted against MDR (merchant discount rate) fees, GST charged on those \
fees, and any refunds. Before the credit lands, each order gets one \
"settlement_report" line recording what was actually netted for it. Orders \
sharing a settlement_batch_id were paid out together in one bank_feed credit.

You will be given one order's facts: what the order itself says (amount, any \
refund), what the settlement math *should* produce if everything reconciles \
cleanly (recomputed from the order alone, at the agreed MDR rate), what the \
actual settlement_report line(s) for that order show, and batch-level context \
(whether the bank credit for that batch matches the sum of its settlement \
lines).

Diagnose the order. Pick exactly one cause:
- clean_match: the actual settlement line matches the recomputed expected \
figures; there is no real issue.
- timing_lag_refund: the order has a refund that is not reflected in this \
settlement's refund_adjustment — it was likely processed too late for this \
cycle and should net out next time.
- mdr_rate_mismatch: the MDR fee actually charged does not match the agreed \
rate implied by the order's expected_mdr_rate.
- duplicate_transaction: more than one settlement_report line exists for \
this order.
- missing_payout: no settlement_report line exists for this order at all.
- other: none of the above cleanly fits — say in your explanation what you \
actually see instead of forcing it into one of the categories above.

Ground every explanation in the specific numbers you were given — cite the \
actual figures, not just the category name. confidence must be a decimal \
between 0.0 and 1.0 (e.g. 0.85), never a percentage like 85. It should \
reflect how unambiguous the evidence is: a clean arithmetic match or a \
single obviously wrong figure deserves high confidence; anything requiring \
you to guess at intent (e.g. a discrepancy that could plausibly be timing OR \
a genuine fee dispute) should get a middling confidence, not a forced high \
one."""

RESPONSE_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "cause": {
            "type": "string",
            "enum": [
                "clean_match",
                "timing_lag_refund",
                "mdr_rate_mismatch",
                "duplicate_transaction",
                "missing_payout",
                "other",
            ],
        },
        "explanation": {"type": "string"},
        "confidence": {"type": "number"},
        "recommended_action": {"type": "string"},
    },
    "required": ["cause", "explanation", "confidence", "recommended_action"],
    "additionalProperties": False,
}


class ExceptionDiagnosis(BaseModel):
    cause: Literal[
        "clean_match",
        "timing_lag_refund",
        "mdr_rate_mismatch",
        "duplicate_transaction",
        "missing_payout",
        "other",
    ]
    explanation: str = Field(
        description="1-3 sentences, grounded in the specific numbers given, not a restatement of the cause label"
    )
    confidence: float = Field(ge=0.0, le=1.0)
    recommended_action: str = Field(description="what should happen next, in one sentence")

    @field_validator("confidence", mode="before")
    @classmethod
    def normalize_confidence(cls, value: float) -> float:
        # Local models often ignore the schema's 0-1 range and return a
        # 0-100 percentage instead (observed with qwen2.5-7b-instruct on
        # LM Studio-style servers) despite the prompt saying otherwise.
        if value > 1:
            value = value / 100
        return max(0.0, min(1.0, value))


def load_csv(name: str) -> list[dict]:
    with (DATA_DIR / name).open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_case(order_id: str, orders: list[dict], settlement_report: list[dict], bank_feed: list[dict]) -> dict:
    order = next(o for o in orders if o["order_id"] == order_id)
    lines = [s for s in settlement_report if s["order_id"] == order_id]

    order_amount = float(order["order_amount"])
    expected_mdr_rate = float(order["expected_mdr_rate"])
    expected_mdr_fee = round(order_amount * expected_mdr_rate, 2)
    expected_gst_on_mdr = round(expected_mdr_fee * GST_RATE, 2)
    expected_refund_adjustment = float(order["refund_amount"]) if order["refund_amount"] else 0.0
    expected_net_amount = round(
        order_amount - expected_mdr_fee - expected_gst_on_mdr - expected_refund_adjustment, 2
    )

    batch_id = lines[0]["settlement_batch_id"] if lines else None
    batch_bank_credit = None
    batch_computed_net = None
    if batch_id:
        bf_row = next((b for b in bank_feed if b["settlement_batch_id"] == batch_id), None)
        batch_bank_credit = float(bf_row["bank_credit_amount"]) if bf_row else None
        batch_lines = [s for s in settlement_report if s["settlement_batch_id"] == batch_id]
        batch_computed_net = round(sum(float(s["net_amount"]) for s in batch_lines), 2)

    return {
        "order_id": order_id,
        "order_amount": order_amount,
        "order_date": order["order_date"],
        "refund_amount": order["refund_amount"] or None,
        "refund_date": order["refund_date"] or None,
        "expected_mdr_rate": expected_mdr_rate,
        "expected_mdr_fee": expected_mdr_fee,
        "expected_gst_on_mdr": expected_gst_on_mdr,
        "expected_refund_adjustment": expected_refund_adjustment,
        "expected_net_amount": expected_net_amount,
        "settlement_lines": [
            {
                "transaction_id": s["transaction_id"],
                "mdr_fee": float(s["mdr_fee"]),
                "gst_on_mdr": float(s["gst_on_mdr"]),
                "refund_adjustment": float(s["refund_adjustment"]),
                "net_amount": float(s["net_amount"]),
                "settlement_batch_id": s["settlement_batch_id"],
            }
            for s in lines
        ],
        "batch_bank_credit_amount": batch_bank_credit,
        "batch_computed_settlement_net": batch_computed_net,
    }


def format_case_for_prompt(case: dict) -> str:
    if case["settlement_lines"]:
        lines_text = "\n".join(
            f"  - transaction_id={line['transaction_id']}, mdr_fee={line['mdr_fee']}, "
            f"gst_on_mdr={line['gst_on_mdr']}, refund_adjustment={line['refund_adjustment']}, "
            f"net_amount={line['net_amount']}, batch={line['settlement_batch_id']}"
            for line in case["settlement_lines"]
        )
    else:
        lines_text = "  (none — no settlement_report row exists for this order)"

    return f"""Order {case["order_id"]}:
- order_amount: {case["order_amount"]}
- order_date: {case["order_date"]}
- refund_amount on the order: {case["refund_amount"]}
- refund_date: {case["refund_date"]}
- expected_mdr_rate (agreed): {case["expected_mdr_rate"]}

Recomputed expected settlement figures (from the order alone, at the agreed rate):
- expected_mdr_fee: {case["expected_mdr_fee"]}
- expected_gst_on_mdr: {case["expected_gst_on_mdr"]}
- expected_refund_adjustment: {case["expected_refund_adjustment"]}
- expected_net_amount: {case["expected_net_amount"]}

Actual settlement_report line(s) for this order ({len(case["settlement_lines"])} found):
{lines_text}

Batch-level context:
- bank_feed credit for this order's settlement batch: {case["batch_bank_credit_amount"]}
- sum of settlement_report.net_amount for that batch: {case["batch_computed_settlement_net"]}

Diagnose this order."""


def diagnose(client: OpenAI, case: dict) -> ExceptionDiagnosis:
    response = client.chat.completions.create(
        model=LOCAL_MODEL_NAME,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": format_case_for_prompt(case)},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "exception_diagnosis", "strict": True, "schema": RESPONSE_JSON_SCHEMA},
        },
        max_tokens=600,
    )
    content = response.choices[0].message.content
    return ExceptionDiagnosis.model_validate(json.loads(content))


def main() -> int:
    orders = load_csv("orders.csv")
    settlement_report = load_csv("settlement_report.csv")
    bank_feed = load_csv("bank_feed.csv")
    ground_truth = load_csv("ground_truth.csv")
    ground_truth_by_id = {g["order_id"]: g for g in ground_truth}

    sample_categories = [
        "timing_lag_refund",
        "mdr_rate_mismatch",
        "duplicate_transaction",
        "missing_payout",
        "clean_match",
    ]
    sample_order_ids = [
        next(g["order_id"] for g in ground_truth if g["exception_type"] == category)
        for category in sample_categories
    ]

    client = OpenAI(base_url=LOCAL_MODEL_BASE_URL, api_key="not-needed")

    correct = 0
    for order_id in sample_order_ids:
        case = build_case(order_id, orders, settlement_report, bank_feed)
        truth = ground_truth_by_id[order_id]

        try:
            diagnosis = diagnose(client, case)
        except APIConnectionError:
            print(f"Could not reach {LOCAL_MODEL_BASE_URL} — is the local server running?")
            return 1
        except APIStatusError as e:
            print(f"Local server returned an error: {e}")
            return 1

        is_match = diagnosis.cause == truth["exception_type"]
        correct += is_match

        print(f"\n{'=' * 70}")
        print(f"order_id: {order_id}")
        print(f"ground_truth: {truth['exception_type']}  ({truth['expected_reasoning']})")
        print(f"model cause: {diagnosis.cause}  (confidence {diagnosis.confidence:.2f})")
        print(f"model explanation: {diagnosis.explanation}")
        print(f"recommended action: {diagnosis.recommended_action}")
        print(f"MATCH: {is_match}")

    print(f"\n{correct}/{len(sample_order_ids)} matched ground_truth.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
