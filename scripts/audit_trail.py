"""The audit trail: one immutable record per order, per pipeline run.

This is the artifact that makes SettleTrace defensible rather than merely
clever. For every order it records, in one place: what the deterministic engine
matched or flagged and on what numbers, what the LLM agent independently
diagnosed and recommended, whether those two agree, and, explicitly on every
single record, that **no autonomous action was taken**.

The governance stance encoded here:

- The **deterministic engine is authoritative**. It is the thing that decides an
  order's category. It scored 100% on the demo batch and its logic is
  inspectable line by line.
- The **LLM agent is advisory**. It produces an explanation and a recommended
  action for a human, and it never overrides the engine. This is not modesty
  for its own sake: the Day 4 measurement showed a 7B local model
  misclassifying structural cases the engine gets right, so treating its output
  as authoritative would *lower* accuracy.
- **Disagreement is a signal, not a resolution.** Where the two differ, the
  record is escalated for human review rather than silently resolved toward
  either one.
- **Nothing is actioned.** No ledger entry, payout, refund, or write-back to any
  source system happens anywhere in this pipeline. Every record says so, so that
  a reviewer reading any single line, not just the summary, can see it.

Records are written as JSONL (full fidelity, one object per line) plus a
flattened CSV for spreadsheet review. In a production deployment this would be
an append-only Delta table under the same schema as the other SettleTrace
tables; the CSV/JSONL pair is the local equivalent against the frozen demo
batch.

Why the agent's words are stored verbatim rather than regenerated on demand:
measured on Day 5, re-running the identical 19 cases at `temperature=0` produced
the same *category* every time but different *prose* on 11 of 19 (see
scripts/reasoning_agent.py). An explanation that cannot be reproduced by
re-running is only preserved if it was written down when it was made, which is
the whole job of this module.
"""

import csv
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

AUDIT_SCHEMA_VERSION = "1.0"

ACTION_POLICY = "advisory_only"
ACTION_NOTE = (
    "SettleTrace produced a classification and, where applicable, a diagnosis and "
    "recommendation. No autonomous action was taken: no ledger entry, payout, refund, "
    "settlement correction, or write-back to any source system was performed. Any "
    "corrective action requires human review and approval."
)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class EngineEvidence(BaseModel):
    """The numbers the deterministic engine actually compared, kept with the verdict.

    A category on its own ages badly. Six months later nobody can tell whether
    it was right. The figures it was derived from make the record re-checkable
    without re-running anything.
    """

    order_amount: float | None = None
    expected_mdr_fee: float | None = None
    actual_mdr_fee: float | None = None
    expected_refund_adjustment: float | None = None
    actual_refund_adjustment: float | None = None
    expected_net_amount: float | None = None
    actual_net_amount: float | None = None
    settlement_row_count: int | None = None
    transaction_id: str | None = None
    settlement_batch_id: str | None = None


class AuditRecord(BaseModel):
    """One order's full trace through the pipeline, for one run."""

    schema_version: str = AUDIT_SCHEMA_VERSION
    run_id: str
    run_started_at: str
    order_id: str

    # --- Stage 2: deterministic reconciliation engine (authoritative) ---
    engine_match_tier: Literal["exact", "fuzzy", "no_match"]
    engine_category: str
    engine_reasoning: str
    engine_evidence: EngineEvidence

    # --- Stage 3: LLM reasoning agent (advisory) ---
    agent_selection_reason: Literal[
        "flagged_by_engine", "clean_control_sample", "not_selected"
    ]
    agent_invoked: bool = False
    agent_skip_reason: str | None = None
    agent_cause: str | None = None
    agent_explanation: str | None = None
    agent_confidence: float | None = None
    agent_recommended_action: str | None = None
    agent_model: str | None = None
    agent_prompt_version: str | None = None
    agent_case_fingerprint: str | None = None
    agent_latency_ms: int | None = None
    agent_error: str | None = None
    agent_explanation_cites_figures: bool = False

    # --- Adjudication between the two ---
    engine_agent_agreement: Literal["agree", "disagree", "not_assessed"] = "not_assessed"
    review_status: Literal["auto_cleared", "needs_human_review"]
    review_reason: str

    # --- Governance: constant by construction, recorded per row on purpose ---
    action_taken: Literal["none"] = "none"
    autonomous_action_taken: Literal[False] = False
    action_policy: str = ACTION_POLICY
    action_note: str = ACTION_NOTE


def explanation_cites_figures(explanation: str | None) -> bool:
    """Does the explanation quote an actual number, rather than just naming a category?

    The prompt asks the agent to ground every explanation in the specific
    figures it was given. Whether it complied is checkable without a model, so
    it is recorded per row and counted per run instead of being left to whoever
    happens to read the output.

    A deliberately blunt test: any digit outside an identifier. It cannot judge
    whether the *right* figure was cited, only whether the explanation is
    arguing from evidence or from the category name. Two of the 19 explanations
    in the committed run fail it, and one of those is legitimate: a missing
    payout has no settlement figures to cite. So this is a signal to read, not a
    gate to enforce, which is why nothing branches on it.
    """
    if not explanation:
        return False
    # Strip UUIDs first, or a transaction_id would count as "citing a figure".
    without_ids = re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "", explanation)
    return bool(re.search(r"\d", without_ids))


class RunSummary(BaseModel):
    """Run-level rollup: the page a reviewer reads before the line items."""

    schema_version: str = AUDIT_SCHEMA_VERSION
    run_id: str
    run_started_at: str
    run_completed_at: str
    duration_seconds: float

    data_source: str
    total_orders: int
    tier_counts: dict[str, int]
    category_counts: dict[str, int]
    exact_match_rate: float
    auto_resolved_rate: float
    batches_checked: int
    batches_balanced: int

    flagged_orders: int
    agent_invoked_count: int
    agent_failed_count: int
    control_sample_count: int
    agent_model: str | None
    agent_prompt_version: str | None
    agent_temperature: float | None
    agent_total_latency_ms: int
    agreement_on_flagged: dict[str, int]
    mean_agent_confidence: float | None
    explanations_citing_figures: int = 0

    orders_needing_human_review: int
    orders_auto_cleared: int

    # Stated at run level as well as per record, so neither a summary reader nor
    # a line-item reader has to take the other's word for it.
    autonomous_actions_taken: int = 0
    action_policy: str = ACTION_POLICY
    action_note: str = ACTION_NOTE


CSV_COLUMNS = [
    "run_id",
    "order_id",
    "engine_match_tier",
    "engine_category",
    "engine_reasoning",
    "agent_selection_reason",
    "agent_invoked",
    "agent_cause",
    "agent_confidence",
    "agent_explanation",
    "agent_explanation_cites_figures",
    "agent_recommended_action",
    "agent_error",
    "engine_agent_agreement",
    "review_status",
    "review_reason",
    "action_taken",
    "autonomous_action_taken",
]


def decide_review(
    engine_category: str, agreement: str, agent_error: str | None
) -> tuple[str, str]:
    """Route an order to auto-clear or human review. Returns (status, reason).

    Deliberately conservative in both directions: anything the engine flagged
    needs a human regardless of how confident the agent sounds, and an order the
    engine cleared still gets escalated if the agent saw something in it. The
    agent can't clear work, only add doubt.
    """
    if engine_category != "clean_match":
        if agreement == "disagree":
            return (
                "needs_human_review",
                (
                    "Engine flagged an exception and the advisory agent diagnosed a "
                    "different cause. Review both before acting."
                ),
            )
        return (
            "needs_human_review",
            "Engine flagged an exception; advisory diagnosis attached for context.",
        )

    if agent_error:
        return (
            "needs_human_review",
            (
                "Engine matched cleanly, but the advisory agent failed to return a valid "
                "diagnosis for this control sample, flagged so the failure isn't invisible."
            ),
        )
    if agreement == "disagree":
        return (
            "needs_human_review",
            (
                "Engine matched cleanly but the advisory agent disagreed, escalated "
                "rather than resolved toward either."
            ),
        )
    return ("auto_cleared", "Engine matched cleanly; no exception detected.")


EVIDENCE_PREFIX = "evidence_"


def record_to_flat_dict(record: AuditRecord) -> dict:
    """Flatten one record for a columnar store (the Delta `audit_log` table).

    JSONL keeps `engine_evidence` nested, which reads well as a document. A table
    people will actually query wants the figures as plain columns, so a reviewer
    can filter on `evidence_actual_mdr_fee` without unpacking a struct.
    """
    data = record.model_dump()
    evidence = data.pop("engine_evidence")
    data.update({f"{EVIDENCE_PREFIX}{key}": value for key, value in evidence.items()})
    return data


def flat_column_names() -> list[str]:
    """Column order for the flattened table, derived from the models, not typed twice."""
    columns = [name for name in AuditRecord.model_fields if name != "engine_evidence"]
    evidence = [f"{EVIDENCE_PREFIX}{name}" for name in EngineEvidence.model_fields]
    return columns + evidence


def write_audit_log(records: list[AuditRecord], out_dir: Path) -> dict[str, Path]:
    """Write the run's audit trail. Returns the paths written."""
    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = out_dir / "audit_log.jsonl"
    csv_path = out_dir / "audit_log.csv"

    with jsonl_path.open("w", encoding="utf-8", newline="\n") as f:
        for record in records:
            f.write(json.dumps(record.model_dump(), default=str) + "\n")

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for record in records:
            row = record.model_dump()
            writer.writerow({column: row.get(column) for column in CSV_COLUMNS})

    return {"jsonl": jsonl_path, "csv": csv_path}


def write_run_summary(summary: RunSummary, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "run_summary.json"
    path.write_text(json.dumps(summary.model_dump(), indent=2, default=str), encoding="utf-8")
    return path


class GradingReport(BaseModel):
    """Test-only accuracy check against `ground_truth`.

    Written to a separate file, never merged into the audit records themselves,
    for the same reason notebook 02 quarantines its accuracy cell: `ground_truth`
    is an answer key that exists to grade this system, and no part of the system
    that runs in production may read it. Keeping it out of the audit schema keeps
    that boundary structural rather than a matter of discipline.
    """

    run_id: str
    graded_at: str = Field(default_factory=utc_now_iso)
    total_orders: int
    engine_correct: int
    engine_accuracy: float
    engine_confusion: dict[str, dict[str, int]]

    agent_graded_orders: int
    agent_correct: int
    agent_accuracy: float | None
    agent_confusion: dict[str, dict[str, int]]
    agent_misses: list[dict]


def write_grading_report(report: GradingReport, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "grading.json"
    path.write_text(json.dumps(report.model_dump(), indent=2, default=str), encoding="utf-8")
    return path
