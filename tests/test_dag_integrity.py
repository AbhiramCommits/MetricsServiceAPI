import pytest
from airflow.models import DagBag
from datetime import timedelta

def test_dag_loading():
    dagBag = DagBag(dag_folder="airflow/dags", include_examples=False)
    assert len(dagBag.import_errors) == 0, f"DAG import errors: {dagBag.import_errors}"
    
    dag = dagBag.get_dag("warehouse_incremental")
    assert dag is not None
    assert dag.schedule_interval == "*/15 * * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1

    expected_tasks = {
        "seed_cdc_batch",
        "load_dim_customer",
        "load_fact_orders",
        "refresh_metrics_cache",
        "check_pipeline_health"
    }
    actual_tasks = {t.task_id for t in dag.tasks}
    assert expected_tasks.issubset(actual_tasks)

    # Check retries and retry_delay
    for task in dag.tasks:
        if task.task_id in ["load_dim_customer", "load_fact_orders"]:
            assert task.retries == 3
            assert task.retry_delay == timedelta(minutes=1)
            assert task.sla == timedelta(minutes=10)
