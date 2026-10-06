from __future__ import annotations

import json
import sys
from pathlib import Path


sys.path.insert(0, str(Path("airflow/dags").resolve()))

import alerting  # noqa: E402


class FakeTaskInstance:
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        self.dag_id = "warehouse_incremental"
        self.run_id = "manual__test"


class FakeDag:
    dag_id = "warehouse_incremental"


class FakeTask:
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id


def test_write_alert_appends_jsonl(tmp_path, monkeypatch):
    alerts_path = tmp_path / "alerts.jsonl"
    monkeypatch.setattr(alerting, "ALERTS_PATH", alerts_path)

    record = alerting.write_alert("TASK_FAILURE", "boom", task_id="load_dim_customer")
    assert record["type"] == "TASK_FAILURE"

    lines = alerts_path.read_text().strip().splitlines()
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["type"] == "TASK_FAILURE"
    assert parsed["task_id"] == "load_dim_customer"


def test_failure_callback_records_task_context(tmp_path, monkeypatch):
    alerts_path = tmp_path / "alerts.jsonl"
    monkeypatch.setattr(alerting, "ALERTS_PATH", alerts_path)

    alerting.on_failure_callback(
        {
            "task_instance": FakeTaskInstance("load_fact_orders"),
            "exception": "simulated",
        }
    )
    parsed = json.loads(alerts_path.read_text().strip())
    assert parsed["task_id"] == "load_fact_orders"
    assert parsed["exception"] == "simulated"


def test_sla_miss_callback_records_tasks(tmp_path, monkeypatch):
    alerts_path = tmp_path / "alerts.jsonl"
    monkeypatch.setattr(alerting, "ALERTS_PATH", alerts_path)

    alerting.sla_miss_callback(FakeDag(), [FakeTask("load_dim_customer")], [], [], [])
    parsed = json.loads(alerts_path.read_text().strip())
    assert parsed["type"] == "SLA_MISS"
    assert parsed["task_ids"] == ["load_dim_customer"]


def test_structured_logging_emits_json(capsys):
    from metrics_service.logging_config import configure_logging, run_id_var
    import logging

    configure_logging()
    token = run_id_var.set("run_abc")
    logging.getLogger("metrics_service.test").info(
        "hello", extra={"table_name": "fact_orders"}
    )
    run_id_var.reset(token)

    captured = capsys.readouterr().err
    payload = json.loads(captured.strip().splitlines()[-1])
    assert payload["message"] == "hello"
    assert payload["run_id"] == "run_abc"
    assert payload["table_name"] == "fact_orders"
