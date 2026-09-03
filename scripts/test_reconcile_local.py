r"""Local, real-Spark correctness test of notebooks/02_reconcile_settlements.py.

Asserts the notebook classifies all 150 demo-batch orders in agreement with
`ground_truth`. The Spark session, demo-batch schemas, and notebook-execution
shim live in scripts/local_spark_harness.py, shared with scripts/run_pipeline.py.
See that module's docstring for why the notebook is exec'd against real Spark
rather than faked, and for the JDK/PYSPARK_PYTHON environment it needs.

    uv run python scripts/test_reconcile_local.py

If `uv run python` itself is blocked by a local Application Control policy
(seen on this machine right after installing new tooling), invoke the venv's
interpreter directly instead:

    .venv\Scripts\python.exe scripts/test_reconcile_local.py
"""

import sys

from local_spark_harness import (
    build_local_spark,
    check_java_available,
    run_reconciliation_notebook,
)


def main():
    java_problem = check_java_available()
    if java_problem:
        print(java_problem)
        return 1

    spark = build_local_spark("reconcile-test")
    try:
        exec_globals, written_tables = run_reconciliation_notebook(spark)

        assert "workspace.settletrace.reconciliation_result" in written_tables, (
            "result table wasn't written"
        )

        total_orders = exec_globals["total_orders"]
        correct = exec_globals["correct"]
        mismatch_count = exec_globals["mismatch_count"]
        print(f"\n[test] total_orders={total_orders} correct={correct} mismatch_count={mismatch_count}")
        assert mismatch_count == 0, "expected 100% accuracy vs ground_truth"
        assert correct == total_orders
        print("[test] PASS")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
