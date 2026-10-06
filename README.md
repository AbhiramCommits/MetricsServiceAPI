# MetricsServiceAPI

A warehouse metrics service that answers analytical questions over a
PostgreSQL star schema through an async FastAPI API. Data lands in CDC-style
staging tables and is moved into `dim_customer` (SCD Type 2) and `fact_orders`
by an incremental, watermark-driven loader that only touches rows that changed
since the last successful run. Airflow orchestrates the loads, a pipeline-health
endpoint reads the run-history and watermark tables, and an in-process TTL cache
absorbs the read traffic. Every response field in the OpenAPI schema names the
SQL file and column it comes from, so the docs double as the metric dictionary.

## Architecture

```
  source / staging                     ETL (watermarks + MERGE)              warehouse
 +-------------------+    read > watermark     +--------------------+    +---------------------+
 | staging.stg_orders|  -------------------->  | etl/loader.py      |    | warehouse.dim_customer (SCD2)
 | staging.stg_customers|  INSERT..ON CONFLICT  | etl/watermark.py   | -> | warehouse.fact_orders
 | (op_type I/U/D)   |                         | etl/run_history.py |    | warehouse.dim_date
 +-------------------+                         +--------------------+    +---------------------+
          ^                                              |                          |
          | seed_source.py / CDC batch                   v                          v
          |                                     +----------------+          +------------------+
          |                                     | etl_watermark  |          | FastAPI + TTL    |
          |                                     | etl_run_history|          | cache (async)    |
          |                                     +----------------+          +------------------+
          |                                              ^                          |
          |                                              |                          v
   +----------------------+   schedule */15   +--------------------+        consumers / /docs
   | Airflow warehouse_   |  ---------------> | /health/pipelines  |
   | incremental DAG      |                   | /metrics/*         |
   +----------------------+                   +--------------------+
```

Modules:

- `metrics_service/config.py` — pydantic-settings (`DATABASE_URL`, `CACHE_TTL_SECONDS`, `APP_ENV`).
- `metrics_service/db.py` — async SQLAlchemy engine (`pool_size=10`, `max_overflow=5`) and session dependency.
- `metrics_service/cache.py` — `TTLCache` with `get/set/invalidate/stats` (monotonic clock).
- `metrics_service/etl/` — `watermark.py`, `run_history.py`, `loader.py`.
- `metrics_service/api/routes_metrics.py` — metric endpoints, cache-through reads, pydantic models.
- `metrics_service/api/routes_health.py` — health, pipeline health, cache admin.
- `metrics_service/sql/` — one `.sql` file per load and per metric (raw SQL, CTEs, window functions).
- `airflow/dags/` — `warehouse_incremental_dag.py` and `alerting.py`.
- `scripts/` — `apply_schema.py`, `seed_source.py`, `run_load.py`, `bench.py`, `demo_alerting.py`.

## Data model

- `warehouse.dim_customer` (SCD Type 2): `customer_sk` surrogate key, `customer_id`
  business key, `region`, `segment`, `valid_from`, `valid_to`, `is_current`.
- `warehouse.fact_orders`: grain = one row per `order_id`; `customer_sk` FK,
  `order_date`, `amount_cents`, `status`, `updated_at`.
- `warehouse.dim_date`: calendar dimension.
- `warehouse.etl_watermark(table_name PK, last_watermark, updated_at)`.
- `warehouse.etl_run_history(run_id PK, table_name, started_at, finished_at, status,
  rows_inserted/updated/deleted, watermark_before/after, error_message)`.

## How to run

```bash
# 1. Postgres
docker compose up -d

# 2. Schema + deterministic synthetic source (50k orders / 5k customers)
make schema
make seed

# 3. Full load then an incremental CDC run
PYTHONPATH=. python scripts/run_load.py --full-refresh
PYTHONPATH=. python scripts/seed_source.py --cdc-batch
PYTHONPATH=. python scripts/run_load.py

# 4. API (OpenAPI docs at /docs)
make api                      # uvicorn on :8000

# 5. Benchmark cold vs warm cache
PYTHONPATH=. python scripts/bench.py --base-url http://127.0.0.1:8000 --n 200

# 6. Tests + coverage
make test                     # pytest --cov=metrics_service
```

Airflow (optional, separate environment — see Limitations):

```bash
pip install -r requirements-airflow.txt
airflow standalone            # add airflow/dags to AIRFLOW_HOME/dags or dags_folder
airflow dags test warehouse_incremental 2026-01-01
```

## Metric dictionary

| Endpoint | Source SQL | Key columns | Technique |
|---|---|---|---|
| `GET /metrics/revenue/daily` | `metrics/revenue_daily.sql` | `order_date`, `total_revenue_cents`, `moving_avg_revenue_7d` | CTE-free aggregate + `AVG() OVER (... ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)` |
| `GET /metrics/revenue/by-region` | `metrics/revenue_by_region.sql` | `region`, `share_of_total_pct`, `revenue_rank` | CTEs + `RANK() OVER (ORDER BY revenue DESC)` |
| `GET /metrics/cohorts` | `metrics/customer_cohorts.sql` | `cohort_month`, `retention_rate_pct` | CTEs + `MIN() OVER (PARTITION BY customer_id)` |
| `GET /metrics/top-customers` | `metrics/top_customers.sql` | `segment`, `total_spent_cents`, `rank_in_segment` | CTE + `ROW_NUMBER() OVER (PARTITION BY segment ORDER BY spend DESC)` |
| `GET /health/pipelines` | `routes_health.py` | `lag_seconds`, `stale`, `last_run_status` | `ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY started_at DESC)` over run history |

Load SQL: `sql/load_dim_customer.sql` (CTE chain: expire changed current rows,
insert new versions, soft-expire on `op_type='D'`) and `sql/load_fact_orders.sql`
(`DISTINCT ON` dedupe, join to current dimension, `INSERT ... ON CONFLICT
(order_id) DO UPDATE`, delete on `op_type='D'`).

## Results

All figures below were measured on this machine against the seeded dataset
(PostgreSQL 16, Python 3.13). Raw console transcripts are in `logs/`.

### Seeded row counts

| Table | Rows |
|---|---|
| `staging.stg_orders` | 50,000 |
| `staging.stg_customers` | 5,000 |
| `warehouse.dim_customer` (after full load) | 5,000 current |
| `warehouse.fact_orders` (after full load) | 50,000 |

### Full refresh vs incremental CDC

| Run | Rows touched | ETL wall-clock |
|---|---|---|
| Full refresh | dim +5,000 inserted; fact +50,000 inserted | **0.7646 s** |
| Incremental CDC | dim +100 inserted / 1 updated; fact +499 inserted / 1 updated / 1 deleted | **0.0557 s** |

Incremental run is **~13.7x faster** than the full refresh and touches only the
CDC batch. A second incremental run with no new staging rows is a no-op
(0 rows, watermark unchanged).

### API latency (N=200/endpoint, `scripts/bench.py`)

| Endpoint | cold app-cache | warm p50 | warm p95 |
|---|---|---|---|
| `/metrics/revenue/daily?limit=100` | 18.16 ms | 0.69 ms | 1.18 ms |
| `/metrics/revenue/by-region` | 27.00 ms | 0.56 ms | 0.86 ms |
| `/metrics/cohorts` | 79.27 ms | 0.60 ms | 0.95 ms |
| `/metrics/top-customers?top_n=5` | 38.79 ms | 0.59 ms | 0.99 ms |

Cache hit rate over the run: **99.38%** (796 hits / 805 requests), 0 evictions.

### Tests

```
25 passed, 1 skipped
coverage: 86% on metrics_service/
```

The single skip is the `DagBag` import test, which is skipped when the installed
Airflow cannot import against the installed SQLAlchemy (see Limitations). The
source-structure assertions in `tests/test_dag_integrity.py` always run.

### Airflow

The DAG `warehouse_incremental` defines **5 tasks**
(`seed_cdc_batch -> load_dim_customer -> load_fact_orders -> refresh_metrics_cache
-> check_pipeline_health`) with `retries=3`, exponential backoff, and a 10-minute
SLA on the load tasks. `airflow dags test` could not be executed in this
environment (Limitations); the equivalent pipeline was run directly via
`scripts/run_load.py`, and the failure/SLA callback path was demonstrated with
`scripts/demo_alerting.py`, producing `logs/alerts.jsonl`:

```json
{"type": "TASK_FAILURE", "task_id": "load_fact_orders", ...}
{"type": "SLA_MISS", "task_ids": ["load_dim_customer"], ...}
```

## Limitations and next steps

- **Cache is in-process.** Each uvicorn worker has its own cache; move to Redis
  (or another shared store) for multi-worker deployments.
- **Airflow/SQLAlchemy split.** Airflow 3.x pins `sqlalchemy<2.0`, which conflicts
  with the async SQLAlchemy 2.x engine the API requires. Airflow lives in
  `requirements-airflow.txt` and should run in its own environment/image; that is
  why the `DagBag` test skips locally and `airflow dags test` was not run here.
- **SQL dialect.** Raw SQL targets PostgreSQL. Trino/Vertica support would need a
  dialect layer and per-dialect SQL variants.
- **CDC is simulation, not log-based.** The loader reads an `op_type` column from
  staging tables. Real log-based CDC (Debezium/WAL) would replace `seed_source.py`.
- **`overflow` pool stat** reported by `/health` follows SQLAlchemy's semantics
  (connections beyond `pool_size`) and can be negative.
- **Docker build** was not verifiable here because the local Docker Desktop
  containerd store is corrupted (`input/output error`); the `Dockerfile` itself is
  a standard `python:3.11-slim` build.
