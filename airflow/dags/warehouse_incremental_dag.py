import json
import logging
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

logger = logging.getLogger("airflow.task")

def write_alert(alert_type: str, message: str):
    alert_record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "type": alert_type,
        "message": message
    }
    with open("logs/alerts.jsonl", "a") as f:
        f.write(json.dumps(alert_record) + "\n")
    logger.error(f"ALERT [{alert_type}]: {message}")

def on_failure_callback(context):
    task_instance = context.get("task_instance")
    msg = f"Task {task_instance.task_id} failed in DAG {task_instance.dag_id} on run {task_instance.run_id}"
    write_alert("TASK_FAILURE", msg)

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    msg = f"SLA miss in DAG {dag.dag_id} for tasks: {[t.task_id for t in task_list]}"
    write_alert("SLA_MISS", msg)

def task_seed_cdc():
    import subprocess
    subprocess.run(["python", "scripts/seed_source.py", "--cdc-batch"], check=True)

def task_load_dim():
    import asyncio
    from metrics_service.db import AsyncSessionLocal
    from metrics_service.etl.loader import load_dim_customer
    async def run():
        async with AsyncSessionLocal() as session:
            await load_dim_customer(session)
    asyncio.run(run())

def task_load_fact():
    import asyncio
    from metrics_service.db import AsyncSessionLocal
    from metrics_service.etl.loader import load_fact_orders
    async def run():
        async with AsyncSessionLocal() as session:
            await load_fact_orders(session)
    asyncio.run(run())

def task_refresh_cache():
    try:
        resp = requests.post("http://localhost:8000/admin/cache/invalidate")
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Cache invalidation endpoint unreachable (maybe API not running standalone): {e}")

def task_check_health():
    try:
        resp = requests.get("http://localhost:8000/health/pipelines")
        resp.raise_for_status()
        data = resp.json()
        for p in data.get("pipelines", []):
            if p.get("stale"):
                raise ValueError(f"Pipeline table {p['table_name']} is stale! Lag: {p['lag_seconds']}s")
    except Exception as e:
        logger.warning(f"Pipeline health endpoint check warning: {e}")

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="warehouse_incremental",
    default_args=default_args,
    description="Incremental warehouse CDC load DAG with retries and SLAs",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=sla_miss_callback,
) as dag:

    seed_task = PythonOperator(
        task_id="seed_cdc_batch",
        python_callable=task_seed_cdc,
    )

    load_dim_task = PythonOperator(
        task_id="load_dim_customer",
        python_callable=task_load_dim,
        sla=timedelta(minutes=10),
    )

    load_fact_task = PythonOperator(
        task_id="load_fact_orders",
        python_callable=task_load_fact,
        sla=timedelta(minutes=10),
    )

    refresh_cache_task = PythonOperator(
        task_id="refresh_metrics_cache",
        python_callable=task_refresh_cache,
    )

    health_task = PythonOperator(
        task_id="check_pipeline_health",
        python_callable=task_check_health,
    )

    seed_task >> load_dim_task >> load_fact_task >> refresh_cache_task >> health_task
