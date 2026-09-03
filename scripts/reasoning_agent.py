"""The LLM reasoning layer: diagnose one settlement order, independently.

Extracted from the Day 4 prototype (scripts/reason_about_exceptions.py) so the
manual 5-sample tool and the automated pipeline (scripts/run_pipeline.py) share
one copy of the prompt, response schema, and case-building logic. The prompt
text here is byte-for-byte the Day 4 prompt, deliberately: Day 5's job is to
run it across every flagged exception and measure it at scale, not to quietly
tune it and lose comparability with the Day 4 result.

The agent is *advisory*. It never sees the deterministic rule engine's own
category (notebooks/02_reconcile_settlements.py), so its diagnosis is a genuine
second opinion rather than a restatement of a label it was handed. That is
what makes engine-vs-agent agreement a meaningful signal rather than a
tautology. It also never takes action; see scripts/audit_trail.py.

Two interchangeable backends, selected by `backend=`:

- **`databricks`** (default): a Foundation Model API endpoint inside the
  workspace, e.g. `databricks-meta-llama-3-3-70b-instruct`. This is the one that
  runs in production, because a Databricks cluster can reach it; it cannot reach
  a model server on someone's laptop.
- **`local`**: an OpenAI-compatible local server (tested against Bionic AI
  Studio / LM Studio-style servers running Qwen2.5-7B-Instruct), kept because it
  costs nothing to iterate against.

Both speak the same OpenAI chat protocol, so the backend only changes the base
URL and credential. Prompt, schema, and parsing are identical either way, which
is what makes the two measurable against each other on the same demo batch.
"""

import csv
import hashlib
import json
import time
from pathlib import Path
from typing import Literal

from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError, field_validator

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "demo_batch"

GST_RATE = 0.18

# Two backends, one code path. "databricks" is the one that matters: it targets
# a Foundation Model API endpoint inside the workspace, which a Databricks
# cluster can actually reach, unlike a model server on the developer's laptop.
# "local" is kept because it costs nothing to iterate against.
DEFAULT_BACKEND = "databricks"
DEFAULT_LOCAL_BASE_URL = "http://localhost:1234/v1"
DEFAULT_LOCAL_MODEL = "qwen2.5-7b-instruct"
DEFAULT_DATABRICKS_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

# Back-compat aliases for the Day 4 prototype's names.
DEFAULT_BASE_URL = DEFAULT_LOCAL_BASE_URL
DEFAULT_MODEL = DEFAULT_LOCAL_MODEL

EXCEPTION_CAUSES = (
    "clean_match",
    "timing_lag_refund",
    "mdr_rate_mismatch",
    "duplicate_transaction",
    "missing_payout",
    "other",
)

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
settlement's refund_adjustment. It was likely processed too late for this \
cycle and should net out next time.
- mdr_rate_mismatch: the MDR fee actually charged does not match the agreed \
rate implied by the order's expected_mdr_rate.
- duplicate_transaction: more than one settlement_report line exists for \
this order.
- missing_payout: no settlement_report line exists for this order at all.
- other: none of the above cleanly fits. Say in your explanation what you \
actually see instead of forcing it into one of the categories above.

Ground every explanation in the specific numbers you were given. Cite the \
actual figures, not just the category name. confidence must be a decimal \
between 0.0 and 1.0 (e.g. 0.85), never a percentage like 85. It should \
reflect how unambiguous the evidence is: a clean arithmetic match or a \
single obviously wrong figure deserves high confidence; anything requiring \
you to guess at intent (e.g. a discrepancy that could plausibly be timing OR \
a genuine fee dispute) should get a middling confidence, not a forced high \
one."""

# Recorded in every audit record, so a diagnosis can always be traced back to
# the exact prompt that produced it. Editing the prompt above changes this
# automatically, which a hand-maintained version number wouldn't.
PROMPT_VERSION = hashlib.sha256(SYSTEM_PROMPT.encode("utf-8")).hexdigest()[:12]

RESPONSE_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "cause": {"type": "string", "enum": list(EXCEPTION_CAUSES)},
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


def build_databricks_client() -> tuple[OpenAI, str]:
    """An OpenAI-compatible client pointed at this workspace's serving endpoints.

    Databricks Foundation Model APIs speak the OpenAI chat protocol at
    `<host>/serving-endpoints`, so the only thing that changes between backends
    is the base URL and the credential. The prompt, schema and parsing are
    untouched.

    Auth resolves the same way everywhere `WorkspaceClient` runs: the notebook's
    own identity on a cluster, the CLI profile locally. That's what lets stage 3
    run in either place without a code change.
    """
    from databricks.sdk import WorkspaceClient

    cfg = WorkspaceClient().config
    token = cfg.authenticate()["Authorization"].split(" ", 1)[1]
    host = cfg.host.rstrip("/")
    return OpenAI(api_key=token, base_url=f"{host}/serving-endpoints"), host


def extract_message_content(message) -> str:
    """Return the assistant text, whether the server sends a string or blocks.

    Most endpoints return `content` as a plain string. Reasoning-style models
    (e.g. `databricks-gpt-oss-120b`) return a list of typed content blocks
    instead, which `json.loads` cannot take directly.
    """
    content = message.content
    if isinstance(content, list):
        parts = []
        for block in content:
            text = getattr(block, "text", None)
            if text is None and isinstance(block, dict):
                text = block.get("text")
            if text:
                parts.append(text)
        return "".join(parts)
    return content


class DiagnosisFailed(RuntimeError):
    """The model never returned a schema-valid diagnosis for this order.

    Raised only after retries are exhausted. Callers should log this against the
    order rather than dropping it: an audit trail that silently omits the orders
    the agent choked on is worse than useless.
    """


def load_demo_csv(name: str) -> list[dict]:
    with (DATA_DIR / name).open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_case(
    order_id: str, orders: list[dict], settlement_report: list[dict], bank_feed: list[dict]
) -> dict:
    """Assemble everything the agent is allowed to see about one order.

    Note what's absent: the rule engine's category, and `ground_truth`. The
    expected figures here are recomputed from the order alone, the same way the
    engine does it, so the agent gets the same raw evidence, not the conclusion.
    """
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
        lines_text = "  (none: no settlement_report row exists for this order)"

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


def case_fingerprint(case: dict) -> str:
    """Hash of the exact evidence shown to the model, for audit reproducibility.

    Lets a later reader confirm a logged diagnosis was produced from this
    snapshot of the data, not a since-changed one.
    """
    payload = json.dumps(case, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class ReasoningAgent:
    """Thin wrapper over an OpenAI-compatible chat endpoint, one order at a time.

    `temperature=0` by default, but note what that does and doesn't buy. Measured
    on Day 5 by running the same 19 orders twice against this server, with
    identical input fingerprints both times:

    - **cause was stable**: 0 of 19 diagnoses changed category;
    - **the prose was not**: explanation text differed on 11 of 19, and
      `confidence` moved on 2.

    So temperature=0 is worth setting (it stabilises the part that drives
    routing) but it does *not* make a run reproducible: this server's kernels
    are nondeterministic below the sampler. That is the load-bearing reason the
    audit trail stores each explanation verbatim (scripts/audit_trail.py) rather
    than regenerating it on demand. Re-running tomorrow yields different words
    for the same order, so the log is the only record of what was actually said.
    """

    def __init__(
        self,
        backend: str = DEFAULT_BACKEND,
        model: str | None = None,
        base_url: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 600,
        max_attempts: int = 3,
    ):
        if backend not in ("local", "databricks"):
            raise ValueError(f"unknown backend {backend!r} (expected 'local' or 'databricks')")

        self.backend = backend
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.max_attempts = max_attempts

        if backend == "databricks":
            self.model = model or DEFAULT_DATABRICKS_ENDPOINT
            self.client, self.base_url = build_databricks_client()
        else:
            self.model = model or DEFAULT_LOCAL_MODEL
            self.base_url = base_url or DEFAULT_LOCAL_BASE_URL
            self.client = OpenAI(base_url=self.base_url, api_key="not-needed")

    def diagnose(self, case: dict) -> tuple[ExceptionDiagnosis, int]:
        """Diagnose one order. Returns (diagnosis, latency_ms).

        Retries only on a malformed/schema-invalid response. A 7B local model
        occasionally emits one, and a retry usually fixes it. Connection and
        HTTP errors are left to propagate: those mean the server is down or
        misconfigured, which no amount of retrying fixes and which the caller
        should surface immediately rather than absorb per-order.
        """
        last_error = None
        started = time.perf_counter()

        for _ in range(self.max_attempts):
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": format_case_for_prompt(case)},
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "exception_diagnosis",
                        "strict": True,
                        "schema": RESPONSE_JSON_SCHEMA,
                    },
                },
                temperature=self.temperature,
                max_tokens=self.max_tokens,
            )
            content = extract_message_content(response.choices[0].message)
            try:
                diagnosis = ExceptionDiagnosis.model_validate(json.loads(content))
            except (json.JSONDecodeError, ValidationError) as e:
                last_error = e
                continue
            latency_ms = int((time.perf_counter() - started) * 1000)
            return diagnosis, latency_ms

        raise DiagnosisFailed(
            f"no schema-valid response after {self.max_attempts} attempts: {last_error}"
        )
