from __future__ import annotations

import pathlib

import pytest

DAG_FILE = pathlib.Path("airflow/dags/warehouse_incremental_dag.py")

EXPECTED_TASKS = [
    "seed_cdc_batch",
    "load_dim_customer",
    "load_fact_orders",
    "refresh_metrics_cache",
    "check_pipeline_health",
]


def test_dag_source_structure():
    """Structural assertions that do not require importing Airflow itself.

    Airflow's runtime import path can conflict with SQLAlchemy 2.x in some
    environments; the source-level checks below always run, while the DagBag
    checks run wherever Airflow imports cleanly (e.g. CI).
    """
    src = DAG_FILE.read_text()

    assert 'dag_id="warehouse_incremental"' in src
    assert 'schedule="*/15 * * * *"' in src
    assert "catchup=False" in src
    assert "max_active_runs=1" in src

    for task_id in EXPECTED_TASKS:
        assert f'task_id="{task_id}"' in src

    chain = (
        "seed_task >> load_dim_task >> load_fact_task "
        ">> refresh_cache_task >> health_task"
    )
    assert chain in src

    assert '"retries": 3' in src
    assert '"retry_exponential_backoff": True' in src
    assert "sla=timedelta(minutes=10)" in src


def test_dagbag_imports_cleanly():
    try:
        from airflow.models import DagBag
    except Exception as exc:  # noqa: BLE001 - environment/version conflicts
        pytest.skip(f"Airflow not importable in this environment: {exc}")

    try:
        dagbag = DagBag(dag_folder="airflow/dags", include_examples=False)
    except AttributeError as exc:  # airflow/sqlalchemy version conflict
        pytest.skip(f"Airflow incompatible with installed SQLAlchemy: {exc}")

    assert not dagbag.import_errors, dagbag.import_errors
    dag = dagbag.get_dag("warehouse_incremental")
    assert dag is not None

    task_ids = {t.task_id for t in dag.tasks}
    assert set(EXPECTED_TASKS).issubset(task_ids)

    load_dim = dag.get_task("load_dim_customer")
    assert load_dim.retries == 3
    assert load_dim.sla is not None
