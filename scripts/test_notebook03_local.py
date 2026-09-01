r"""Local, real-Spark test of notebooks/03_reason_and_audit.py.

Chains the two notebooks the way the cluster does: runs
`02_reconcile_settlements.py` against the frozen demo batch to produce
`reconciliation_result`, feeds that to `03_reason_and_audit.py`, and asserts the
resulting `audit_log` is well formed and its governance invariants hold.

This does make real calls to the configured serving endpoint — the point is to
exercise the notebook's actual code path, not a mock of it. Keep
`--clean-controls` small to keep the bill small.

Needs the same JDK/PYSPARK_PYTHON setup as scripts/test_reconcile_local.py, plus
Databricks CLI auth for the serving endpoint:

    .venv\Scripts\python.exe scripts/test_notebook03_local.py
"""

import argparse
import sys
from pathlib import Path

from local_spark_harness import (
    DEFAULT_CATALOG,
    DEFAULT_SCHEMA,
    FakeDbutils,
    build_local_spark,
    check_java_available,
    load_demo_tables,
    run_reconciliation_notebook,
)
from pyspark.sql.readwriter import DataFrameWriter

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_03 = REPO_ROOT / "notebooks" / "03_reason_and_audit.py"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--endpoint", default=None, help="serving endpoint (default: module default)")
    parser.add_argument("--clean-controls", type=int, default=2)
    parser.add_argument(
        "--max-flagged", type=int, default=4, help="0 for all flagged orders (default 4, to stay cheap)"
    )
    args = parser.parse_args()

    java_problem = check_java_available()
    if java_problem:
        print(java_problem)
        return 1

    spark = build_local_spark("notebook03-test")
    try:
        # Stage 2 first — notebook 03 reads what it writes.
        _, written = run_reconciliation_notebook(spark, quiet=True)
        result_table = f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.reconciliation_result"
        assert result_table in written, "notebook 02 didn't write reconciliation_result"

        tables = load_demo_tables(spark)
        tables[result_table] = written[result_table]
        spark.table = lambda name: tables[name]

        written_03 = {}

        def fake_save_as_table(self, name):
            written_03[name] = self._df
            # Register it as readable too: notebook 03 reads `audit_log` back
            # via spark.table() to display the review queue, which on a real
            # cluster is just a Delta read of what it wrote a cell earlier.
            tables[name] = self._df

        DataFrameWriter.saveAsTable = fake_save_as_table

        import reasoning_agent

        dbutils = FakeDbutils(
            {
                "catalog": DEFAULT_CATALOG,
                "schema_name": DEFAULT_SCHEMA,
                "endpoint": args.endpoint or reasoning_agent.DEFAULT_DATABRICKS_ENDPOINT,
                "clean_controls": str(args.clean_controls),
                "max_flagged": str(args.max_flagged),
                "temperature": "0.0",
            }
        )

        def display(df):
            df.show(30, truncate=60)

        source = NOTEBOOK_03.read_text(encoding="utf-8")
        exec_globals = {"dbutils": dbutils, "spark": spark, "display": display}
        exec(compile(source, str(NOTEBOOK_03), "exec"), exec_globals)  # noqa: S102 -- our own notebook source

        audit_table = f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.audit_log"
        assert audit_table in written_03, "notebook 03 didn't write audit_log"

        audit_df = written_03[audit_table]
        rows = [row.asDict() for row in audit_df.collect()]
        records = exec_globals["records"]

        assert len(rows) == len(records) == 150, f"expected 150 audit rows, got {len(rows)}"

        # Governance invariants — the claims the audit trail makes about itself.
        assert {r["action_taken"] for r in rows} == {"none"}, "action_taken must be 'none' everywhere"
        assert {r["autonomous_action_taken"] for r in rows} == {False}, (
            "autonomous_action_taken must be False everywhere"
        )
        assert all(
            r["review_status"] == "needs_human_review"
            for r in rows
            if r["engine_category"] != "clean_match"
        ), "every engine-flagged order must be escalated"

        diagnosed = [r for r in rows if r["agent_invoked"]]
        assert diagnosed, "no orders were diagnosed"
        assert all(r["agent_prompt_version"] for r in diagnosed), "missing prompt provenance"
        assert all(r["agent_case_fingerprint"] for r in diagnosed), "missing evidence fingerprint"

        print(f"\n[test] audit rows={len(rows)} diagnosed={len(diagnosed)}")
        print(f"[test] needs_human_review={sum(1 for r in rows if r['review_status'] == 'needs_human_review')}")
        print("[test] PASS")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
