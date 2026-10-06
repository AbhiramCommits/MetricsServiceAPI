"""Shared pytest fixtures.

Applies the warehouse schema, seeds a small deterministic dataset into the
staging tables, and runs a full load so the warehouse is populated before any
test runs. A single session-scoped event loop is used (configured in
pytest.ini) so the module-level async engine pool stays bound to one loop.
"""

from __future__ import annotations

import pathlib
from datetime import date, datetime, timezone

import asyncpg
import pytest_asyncio
from sqlalchemy import text

from metrics_service.config import settings
from metrics_service.db import AsyncSessionLocal
from metrics_service.etl.loader import run_all

BASE_TS = datetime(2024, 1, 1, tzinfo=timezone.utc)

CUSTOMERS = [
    ("CUST_1", "North", "Enterprise", BASE_TS, "I"),
    ("CUST_2", "South", "SMB", BASE_TS, "I"),
    ("CUST_3", "East", "Consumer", BASE_TS, "I"),
]

ORDERS = [
    ("ORD_1", "CUST_1", date(2024, 1, 1), 10000, "completed", BASE_TS, "I"),
    ("ORD_2", "CUST_1", date(2024, 1, 2), 20000, "completed", BASE_TS, "I"),
    ("ORD_3", "CUST_2", date(2024, 1, 1), 5000, "pending", BASE_TS, "I"),
    ("ORD_4", "CUST_2", date(2024, 1, 9), 15000, "completed", BASE_TS, "I"),
    ("ORD_5", "CUST_3", date(2024, 1, 1), 7000, "completed", BASE_TS, "I"),
    ("ORD_6", "CUST_3", date(2024, 1, 2), 3000, "completed", BASE_TS, "I"),
    ("ORD_7", "CUST_1", date(2024, 1, 15), 25000, "completed", BASE_TS, "I"),
]


async def _apply_schema() -> None:
    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        schema_sql = pathlib.Path("metrics_service/sql/schema.sql").read_text()
        await conn.execute(schema_sql)
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def prepared_warehouse():
    await _apply_schema()

    async with AsyncSessionLocal() as session:
        await session.execute(
            text(
                "TRUNCATE staging.stg_customers, staging.stg_orders, "
                "warehouse.fact_orders, warehouse.dim_customer, "
                "warehouse.etl_watermark, warehouse.etl_run_history "
                "RESTART IDENTITY CASCADE;"
            )
        )
        await session.execute(
            text(
                "INSERT INTO staging.stg_customers "
                "(customer_id, region, segment, source_updated_at, op_type) "
                "VALUES (:customer_id, :region, :segment, :source_updated_at, :op_type)"
            ),
            [
                {
                    "customer_id": c[0],
                    "region": c[1],
                    "segment": c[2],
                    "source_updated_at": c[3],
                    "op_type": c[4],
                }
                for c in CUSTOMERS
            ],
        )
        await session.execute(
            text(
                "INSERT INTO staging.stg_orders "
                "(order_id, customer_id, order_date, amount_cents, status, "
                "source_updated_at, op_type) "
                "VALUES (:order_id, :customer_id, :order_date, :amount_cents, "
                ":status, :source_updated_at, :op_type)"
            ),
            [
                {
                    "order_id": o[0],
                    "customer_id": o[1],
                    "order_date": o[2],
                    "amount_cents": o[3],
                    "status": o[4],
                    "source_updated_at": o[5],
                    "op_type": o[6],
                }
                for o in ORDERS
            ],
        )
        await session.commit()

        # Full initial load so downstream API/SQL tests see warehouse data.
        await run_all(session)

    yield
