"""Structured alerting helpers shared by the Airflow DAG.

Kept free of Airflow imports so the failure/SLA paths can be unit-tested and
demonstrated without a running Airflow scheduler.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("metrics_service.alerts")

ALERTS_PATH = Path("logs/alerts.jsonl")


def write_alert(alert_type: str, message: str, **extra) -> dict:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "type": alert_type,
        "message": message,
    }
    record.update(extra)
    ALERTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with ALERTS_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")
    logger.error("ALERT [%s]: %s", alert_type, message)
    return record


def on_failure_callback(context) -> None:
    ti = context.get("task_instance")
    write_alert(
        "TASK_FAILURE",
        f"Task {ti.task_id} failed in DAG {ti.dag_id} on run {ti.run_id}",
        task_id=ti.task_id,
        dag_id=ti.dag_id,
        run_id=ti.run_id,
        exception=str(context.get("exception", "")),
    )


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    task_ids = [t.task_id for t in task_list]
    write_alert(
        "SLA_MISS",
        f"SLA miss in DAG {dag.dag_id} for tasks: {task_ids}",
        dag_id=dag.dag_id,
        task_ids=task_ids,
    )
