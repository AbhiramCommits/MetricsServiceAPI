"""Demonstrate the DAG failure/SLA alerting path without an Airflow scheduler.

Airflow's runtime pins a SQLAlchemy version that conflicts with the async
engine used by the API in this environment, so `airflow dags test` cannot run
here. The callback contract is identical: this script invokes the same
functions the DAG registers and shows the resulting logs/alerts.jsonl.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path("airflow/dags").resolve()))

from alerting import ALERTS_PATH, on_failure_callback, sla_miss_callback  # noqa: E402


class FakeTaskInstance:
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        self.dag_id = "warehouse_incremental"
        self.run_id = "manual__demo_run"


class FakeDag:
    dag_id = "warehouse_incremental"


class FakeTask:
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id


def main() -> None:
    if ALERTS_PATH.exists():
        ALERTS_PATH.unlink()

    on_failure_callback(
        {
            "task_instance": FakeTaskInstance("load_fact_orders"),
            "exception": "simulated transient DB connection drop",
        }
    )
    sla_miss_callback(
        FakeDag(),
        [FakeTask("load_dim_customer")],
        [],
        [],
        [],
    )

    print(f"Wrote alerts to {ALERTS_PATH}:")
    print(ALERTS_PATH.read_text())


if __name__ == "__main__":
    main()
