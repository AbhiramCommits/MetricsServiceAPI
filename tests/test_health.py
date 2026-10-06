from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from metrics_service.db import AsyncSessionLocal
from metrics_service.main import app


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


@pytest.mark.asyncio
async def test_pipeline_health_reports_lag(client: AsyncClient):
    resp = await client.get("/health/pipelines")
    assert resp.status_code == 200
    pipelines = {p["table_name"]: p for p in resp.json()["pipelines"]}
    assert "dim_customer" in pipelines
    assert "fact_orders" in pipelines
    assert pipelines["dim_customer"]["lag_seconds"] >= 0
    assert "stale" in pipelines["dim_customer"]


@pytest.mark.asyncio
async def test_pipeline_flagged_stale_when_watermark_pushed_back(client: AsyncClient):
    old_ts = datetime.now(timezone.utc) - timedelta(days=10)
    async with AsyncSessionLocal() as session:
        await session.execute(
            text(
                "INSERT INTO warehouse.etl_watermark "
                "(table_name, last_watermark, updated_at) "
                "VALUES ('test_stale', :ts, NOW()) "
                "ON CONFLICT (table_name) DO UPDATE "
                "SET last_watermark = EXCLUDED.last_watermark"
            ),
            {"ts": old_ts},
        )
        await session.commit()

    try:
        resp = await client.get("/health/pipelines?stale_threshold_seconds=3600")
        assert resp.status_code == 200
        pipelines = {p["table_name"]: p for p in resp.json()["pipelines"]}
        assert pipelines["test_stale"]["stale"] is True
        assert pipelines["test_stale"]["lag_seconds"] > 3600
    finally:
        async with AsyncSessionLocal() as session:
            await session.execute(
                text(
                    "DELETE FROM warehouse.etl_watermark WHERE table_name = 'test_stale'"
                )
            )
            await session.commit()
