r"""SettleTrace end-to-end: data -> match -> exception reasoning -> audit log.

Runs all four stages in one command against the frozen demo batch:

1. **Data** — the committed `data/demo_batch/*.csv` snapshot (regenerate with
   scripts/export_demo_batch.py).
2. **Match** — the real `notebooks/02_reconcile_settlements.py`, executed
   against a local SparkSession via scripts/local_spark_harness.py. Not a
   reimplementation: the same notebook source that runs on Databricks.
3. **Reason** — every order the engine flagged is sent to the advisory LLM agent
   (scripts/reasoning_agent.py) for an independent diagnosis, plus a small
   deterministic sample of *clean* orders as controls, so the agent's
   false-positive behaviour is measured rather than assumed.
4. **Audit** — one record per order written to the audit trail
   (scripts/audit_trail.py), each stating explicitly that no autonomous action
   was taken.

Why the reasoning stage runs locally rather than inside notebook 03: the agent
targets a local model server on `localhost`, which a Databricks cluster cannot
reach. Moving this stage onto the cluster needs a cluster-reachable endpoint
(Databricks Model Serving or a hosted API) — the stage boundary here is drawn
so that swapping the endpoint is the only change required.

Usage (PowerShell, with a JDK and the local model server both up):

    $env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
    $env:PYSPARK_PYTHON = ".venv\Scripts\python.exe"
    $env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
    .venv\Scripts\python.exe scripts/run_pipeline.py

Add `--no-llm` to exercise stages 1, 2 and 4 without a model server running;
the audit trail still gets a record per order, with the skipped diagnosis
logged as such rather than left blank.
"""

import argparse
import random
import sys
import time
from pathlib import Path
from uuid import uuid4

import reasoning_agent
from audit_trail import (
    AuditRecord,
    EngineEvidence,
    GradingReport,
    RunSummary,
    decide_review,
    utc_now_iso,
    write_audit_log,
    write_grading_report,
    write_run_summary,
)
from local_spark_harness import (
    DATA_DIR,
    DEMO_TABLE_SCHEMAS,
    build_local_spark,
    check_java_available,
    run_reconciliation_notebook,
)
from openai import APIConnectionError, APIStatusError
from reasoning_agent import DiagnosisFailed, ReasoningAgent

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "audit_log"
CONTROL_SAMPLE_SEED = 42


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT_DIR, help="audit trail output directory"
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="skip the reasoning stage (still writes a complete audit trail)",
    )
    parser.add_argument(
        "--clean-controls",
        type=int,
        default=5,
        help="how many clean orders to also send to the agent, as controls (default 5)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="cap how many flagged orders are reasoned about (for quick runs)",
    )
    parser.add_argument(
        "--backend",
        choices=("databricks", "local"),
        default=reasoning_agent.DEFAULT_BACKEND,
        help="where the reasoning model runs (default: databricks)",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="override the local backend's server URL (ignored for databricks)",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="model/endpoint name; defaults per backend",
    )
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument(
        "--no-grading",
        action="store_true",
        help="skip the test-only accuracy check against ground_truth",
    )
    return parser.parse_args()


def stage_header(number: int, title: str) -> None:
    print(f"\n{'=' * 72}\nStage {number}: {title}\n{'=' * 72}")


def display_path(path: Path) -> str:
    """Repo-relative when it is inside the repo, absolute otherwise (`--out` need not be)."""
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def run_matching(quiet: bool = True) -> tuple[list[dict], dict]:
    """Stage 2 — run the real reconciliation notebook, return its result rows."""
    java_problem = check_java_available()
    if java_problem:
        raise RuntimeError(java_problem)

    spark = build_local_spark("settletrace-pipeline")
    try:
        exec_globals, written_tables = run_reconciliation_notebook(spark, quiet=quiet)
        table_name = "workspace.settletrace.reconciliation_result"
        if table_name not in written_tables:
            raise RuntimeError(f"notebook did not write {table_name}")
        rows = [row.asDict() for row in written_tables[table_name].collect()]
        batch_stats = {
            "total_batches": exec_globals.get("total_batches", 0),
            "unbalanced_batches": exec_globals.get("unbalanced_batches", 0),
        }
    finally:
        spark.stop()

    return rows, batch_stats


def select_control_orders(clean_order_ids: list[str], count: int) -> list[str]:
    """Deterministically pick clean orders to double-check the agent against.

    Seeded and sorted so the same demo batch always yields the same controls —
    an audit trail whose sample set moves between runs can't be compared to the
    previous run's.
    """
    if count <= 0:
        return []
    pool = sorted(clean_order_ids)
    if count >= len(pool):
        return pool
    return sorted(random.Random(CONTROL_SAMPLE_SEED).sample(pool, count))


def build_grading_report(
    run_id: str, records: list[AuditRecord], ground_truth: list[dict]
) -> GradingReport:
    """Test-only: grade both the engine and the agent against the answer key."""
    truth_by_id = {row["order_id"]: row["exception_type"] for row in ground_truth}

    engine_confusion: dict[str, dict[str, int]] = {}
    agent_confusion: dict[str, dict[str, int]] = {}
    engine_correct = 0
    agent_correct = 0
    agent_graded = 0
    agent_misses = []

    for record in records:
        true_category = truth_by_id.get(record.order_id)
        if true_category is None:
            continue

        engine_confusion.setdefault(true_category, {}).setdefault(record.engine_category, 0)
        engine_confusion[true_category][record.engine_category] += 1
        if record.engine_category == true_category:
            engine_correct += 1

        if record.agent_invoked and record.agent_cause:
            agent_graded += 1
            agent_confusion.setdefault(true_category, {}).setdefault(record.agent_cause, 0)
            agent_confusion[true_category][record.agent_cause] += 1
            if record.agent_cause == true_category:
                agent_correct += 1
            else:
                agent_misses.append(
                    {
                        "order_id": record.order_id,
                        "ground_truth": true_category,
                        "engine_category": record.engine_category,
                        "agent_cause": record.agent_cause,
                        "agent_confidence": record.agent_confidence,
                        "agent_explanation": record.agent_explanation,
                    }
                )

    total = len([r for r in records if r.order_id in truth_by_id])
    return GradingReport(
        run_id=run_id,
        total_orders=total,
        engine_correct=engine_correct,
        engine_accuracy=engine_correct / total if total else 0.0,
        engine_confusion=engine_confusion,
        agent_graded_orders=agent_graded,
        agent_correct=agent_correct,
        agent_accuracy=(agent_correct / agent_graded) if agent_graded else None,
        agent_confusion=agent_confusion,
        agent_misses=agent_misses,
    )


def main() -> int:
    args = parse_args()
    run_id = uuid4().hex[:12]
    run_started_at = utc_now_iso()
    started = time.perf_counter()

    print(f"SettleTrace pipeline — run_id={run_id} started {run_started_at}")

    # --- Stage 1: data -----------------------------------------------------
    stage_header(1, "Data — frozen demo batch")
    missing = [name for name in DEMO_TABLE_SCHEMAS if not (DATA_DIR / f"{name}.csv").exists()]
    if missing:
        print(f"Missing demo batch files: {missing}")
        print("Regenerate with: uv run python scripts/export_demo_batch.py")
        return 1
    orders = reasoning_agent.load_demo_csv("orders.csv")
    settlement_report = reasoning_agent.load_demo_csv("settlement_report.csv")
    bank_feed = reasoning_agent.load_demo_csv("bank_feed.csv")
    print(
        f"orders={len(orders)}  settlement_report={len(settlement_report)}  "
        f"bank_feed={len(bank_feed)}  (source: data/demo_batch)"
    )

    # --- Stage 2: match ----------------------------------------------------
    stage_header(2, "Match — notebooks/02_reconcile_settlements.py on local Spark")
    try:
        result_rows, batch_stats = run_matching(quiet=True)
    except RuntimeError as e:
        print(f"Matching stage failed: {e}")
        return 1

    tier_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    for row in result_rows:
        tier_counts[row["match_tier"]] = tier_counts.get(row["match_tier"], 0) + 1
        category_counts[row["category"]] = category_counts.get(row["category"], 0) + 1

    total_orders = len(result_rows)
    exact_rate = tier_counts.get("exact", 0) / total_orders
    auto_resolved_rate = (tier_counts.get("exact", 0) + tier_counts.get("fuzzy", 0)) / total_orders
    balanced = batch_stats["total_batches"] - batch_stats["unbalanced_batches"]

    print(f"Classified {total_orders} orders.")
    print(f"  tiers:      {tier_counts}")
    print(f"  categories: {category_counts}")
    print(f"  exact {exact_rate:.1%} | auto-resolved (exact+fuzzy) {auto_resolved_rate:.1%}")
    print(f"  batch cross-check: {balanced}/{batch_stats['total_batches']} balanced")

    rows_by_id = {row["order_id"]: row for row in result_rows}
    flagged_ids = sorted(r["order_id"] for r in result_rows if r["category"] != "clean_match")
    clean_ids = [r["order_id"] for r in result_rows if r["category"] == "clean_match"]

    if args.limit is not None:
        flagged_ids = flagged_ids[: args.limit]

    control_ids = select_control_orders(clean_ids, args.clean_controls)
    to_reason = [(oid, "flagged_by_engine") for oid in flagged_ids] + [
        (oid, "clean_control_sample") for oid in control_ids
    ]

    # --- Stage 3: reason ---------------------------------------------------
    stage_header(3, "Reason — advisory LLM diagnosis on flagged orders")
    diagnoses: dict[str, dict] = {}
    agent: ReasoningAgent | None = None
    total_latency_ms = 0

    if args.no_llm:
        print("--no-llm set: skipping the reasoning stage (audit trail records this per order).")
    else:
        agent = ReasoningAgent(
            backend=args.backend,
            base_url=args.base_url,
            model=args.model,
            temperature=args.temperature,
        )
        print(
            f"backend={agent.backend}  model={agent.model} @ {agent.base_url}\n"
            f"temperature={args.temperature}  prompt={reasoning_agent.PROMPT_VERSION}"
        )
        print(
            f"Diagnosing {len(flagged_ids)} flagged order(s) "
            f"+ {len(control_ids)} clean control(s)...\n"
        )

        for index, (order_id, selection_reason) in enumerate(to_reason, start=1):
            case = reasoning_agent.build_case(order_id, orders, settlement_report, bank_feed)
            engine_category = rows_by_id[order_id]["category"]
            label = "flagged" if selection_reason == "flagged_by_engine" else "control"

            try:
                diagnosis, latency_ms = agent.diagnose(case)
            except (APIConnectionError, APIStatusError) as e:
                print(f"\nLocal model server unreachable or erroring: {e}")
                print("Start the server, or re-run with --no-llm to skip the reasoning stage.")
                return 1
            except DiagnosisFailed as e:
                diagnoses[order_id] = {
                    "error": str(e),
                    "case_fingerprint": reasoning_agent.case_fingerprint(case),
                }
                print(f"[{index}/{len(to_reason)}] {order_id[:8]} ({label:7}) DIAGNOSIS FAILED: {e}")
                continue

            total_latency_ms += latency_ms
            diagnoses[order_id] = {
                "diagnosis": diagnosis,
                "latency_ms": latency_ms,
                "case_fingerprint": reasoning_agent.case_fingerprint(case),
            }
            agrees = diagnosis.cause == engine_category
            marker = "agree   " if agrees else "DISAGREE"
            print(
                f"[{index}/{len(to_reason)}] {order_id[:8]} ({label:7}) "
                f"engine={engine_category:22} agent={diagnosis.cause:22} "
                f"conf={diagnosis.confidence:.2f} {marker} {latency_ms:>6}ms"
            )

    # --- Stage 4: audit ----------------------------------------------------
    stage_header(4, "Audit — write the trail")
    # The endpoint actually used, not what was asked for: --model defaults to
    # None and is resolved per backend, so the audit record must record the
    # resolved name or its provenance is wrong.
    resolved_model = agent.model if agent else None
    selection_by_id = dict(to_reason)
    records: list[AuditRecord] = []

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
            if args.no_llm:
                skip_reason = "reasoning_stage_disabled (--no-llm)"
            elif selection_reason == "not_selected":
                skip_reason = (
                    "engine matched this order cleanly and it was not drawn as a control; "
                    "advisory diagnosis is reserved for flagged orders"
                )
            else:
                skip_reason = "not reached (run limited via --limit)"
        elif "error" in entry:
            agent_error = entry["error"]
            agent_fields = {
                "agent_invoked": True,
                "agent_model": resolved_model,
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
                "agent_model": resolved_model,
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
                engine_agent_agreement=agreement,
                review_status=review_status,
                review_reason=review_reason,
                **agent_fields,
            )
        )

    agreement_on_flagged: dict[str, int] = {}
    for record in records:
        if record.agent_selection_reason == "flagged_by_engine":
            key = record.engine_agent_agreement
            agreement_on_flagged[key] = agreement_on_flagged.get(key, 0) + 1

    confidences = [r.agent_confidence for r in records if r.agent_confidence is not None]
    completed_at = utc_now_iso()
    duration = time.perf_counter() - started

    summary = RunSummary(
        run_id=run_id,
        run_started_at=run_started_at,
        run_completed_at=completed_at,
        duration_seconds=round(duration, 1),
        data_source="data/demo_batch (frozen, num_orders=150 seed=42)",
        total_orders=total_orders,
        tier_counts=tier_counts,
        category_counts=category_counts,
        exact_match_rate=round(exact_rate, 4),
        auto_resolved_rate=round(auto_resolved_rate, 4),
        batches_checked=batch_stats["total_batches"],
        batches_balanced=balanced,
        flagged_orders=len(flagged_ids),
        agent_invoked_count=sum(1 for r in records if r.agent_invoked),
        agent_failed_count=sum(1 for r in records if r.agent_error),
        control_sample_count=len(control_ids),
        agent_model=resolved_model,
        agent_prompt_version=None if args.no_llm else reasoning_agent.PROMPT_VERSION,
        agent_temperature=None if args.no_llm else args.temperature,
        agent_total_latency_ms=total_latency_ms,
        agreement_on_flagged=agreement_on_flagged,
        mean_agent_confidence=(
            round(sum(confidences) / len(confidences), 3) if confidences else None
        ),
        orders_needing_human_review=sum(
            1 for r in records if r.review_status == "needs_human_review"
        ),
        orders_auto_cleared=sum(1 for r in records if r.review_status == "auto_cleared"),
    )

    paths = write_audit_log(records, args.out)
    summary_path = write_run_summary(summary, args.out)
    print(f"Wrote {len(records)} audit records:")
    print(f"  {display_path(paths['jsonl'])}")
    print(f"  {display_path(paths['csv'])}")
    print(f"  {display_path(summary_path)}")
    print(
        f"\nReview queue: {summary.orders_needing_human_review} need human review, "
        f"{summary.orders_auto_cleared} auto-cleared."
    )
    print(f"Autonomous actions taken this run: {summary.autonomous_actions_taken}.")

    if not args.no_grading:
        ground_truth = reasoning_agent.load_demo_csv("ground_truth.csv")
        report = build_grading_report(run_id, records, ground_truth)
        grading_path = write_grading_report(report, args.out)
        print(f"\n[test-only] graded against ground_truth -> {display_path(grading_path)}")
        print(
            f"  engine: {report.engine_correct}/{report.total_orders} "
            f"({report.engine_accuracy:.1%})"
        )
        if report.agent_accuracy is not None:
            print(
                f"  agent:  {report.agent_correct}/{report.agent_graded_orders} "
                f"({report.agent_accuracy:.1%}) on the orders it was asked about"
            )

    print(f"\nDone in {duration:.1f}s.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
