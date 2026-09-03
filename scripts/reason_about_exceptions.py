"""Day 4 prototype: a manual, 5-sample spot-check of the LLM reasoning layer.

Feeds one hand-picked order per category (four exception types + one clean
control) from the frozen demo batch to the local model and prints the diagnosis
against `ground_truth`, for eyeballing. This is the small, readable harness used
to validate the prompt before it was automated.

The prompt, response schema, and case-building all live in
scripts/reasoning_agent.py, shared with the automated pipeline
(scripts/run_pipeline.py), so what this prints is exactly what the pipeline
runs, not a drifting copy of it.

For the full automated run across *every* flagged exception, with an audit
trail, use scripts/run_pipeline.py instead.

Usage: uv run python scripts/reason_about_exceptions.py
"""

import argparse
import sys

import reasoning_agent
from openai import APIConnectionError, APIStatusError
from reasoning_agent import DiagnosisFailed, ReasoningAgent


def main() -> int:
    parser = argparse.ArgumentParser(description="5-sample spot-check of the reasoning layer")
    parser.add_argument(
        "--backend", choices=("databricks", "local"), default=reasoning_agent.DEFAULT_BACKEND
    )
    parser.add_argument("--model", default=None, help="model/endpoint; defaults per backend")
    args = parser.parse_args()

    orders = reasoning_agent.load_demo_csv("orders.csv")
    settlement_report = reasoning_agent.load_demo_csv("settlement_report.csv")
    bank_feed = reasoning_agent.load_demo_csv("bank_feed.csv")
    ground_truth = reasoning_agent.load_demo_csv("ground_truth.csv")
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

    agent = ReasoningAgent(backend=args.backend, model=args.model)
    print(f"backend={agent.backend}  model={agent.model}")

    correct = 0
    for order_id in sample_order_ids:
        case = reasoning_agent.build_case(order_id, orders, settlement_report, bank_feed)
        truth = ground_truth_by_id[order_id]

        try:
            diagnosis, latency_ms = agent.diagnose(case)
        except APIConnectionError:
            print(f"Could not reach {agent.base_url}. Is the local server running?")
            return 1
        except APIStatusError as e:
            print(f"Local server returned an error: {e}")
            return 1
        except DiagnosisFailed as e:
            print(f"{order_id}: {e}")
            continue

        is_match = diagnosis.cause == truth["exception_type"]
        correct += is_match

        print(f"\n{'=' * 70}")
        print(f"order_id: {order_id}")
        print(f"ground_truth: {truth['exception_type']}  ({truth['expected_reasoning']})")
        print(f"model cause: {diagnosis.cause}  (confidence {diagnosis.confidence:.2f}, {latency_ms}ms)")
        print(f"model explanation: {diagnosis.explanation}")
        print(f"recommended action: {diagnosis.recommended_action}")
        print(f"MATCH: {is_match}")

    print(f"\n{correct}/{len(sample_order_ids)} matched ground_truth.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
