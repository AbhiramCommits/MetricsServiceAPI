from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from metrics_service.api.routes_metrics import cache
from metrics_service.main import app


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


@pytest.mark.asyncio
async def test_health_endpoint(client: AsyncClient):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"


@pytest.mark.asyncio
async def test_daily_revenue_endpoint(client: AsyncClient):
    cache.invalidate()
    resp = await client.get("/metrics/revenue/daily?limit=5")
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list)
    assert len(body) == 4
    assert {"order_date", "order_count", "total_revenue_cents"} <= set(body[0])


@pytest.mark.asyncio
async def test_revenue_by_region_endpoint(client: AsyncClient):
    resp = await client.get("/metrics/revenue/by-region")
    assert resp.status_code == 200
    assert len(resp.json()) == 3


@pytest.mark.asyncio
async def test_daily_revenue_date_filter(client: AsyncClient):
    cache.invalidate()
    resp = await client.get(
        "/metrics/revenue/daily?start_date=2024-01-02&end_date=2024-01-09"
    )
    assert resp.status_code == 200
    dates = [row["order_date"] for row in resp.json()]
    assert dates == ["2024-01-09", "2024-01-02"]


@pytest.mark.asyncio
async def test_cohorts_and_top_customers_endpoints(client: AsyncClient):
    cohorts = await client.get("/metrics/cohorts")
    assert cohorts.status_code == 200
    assert len(cohorts.json()) == 1

    top = await client.get("/metrics/top-customers?top_n=5")
    assert top.status_code == 200
    assert len(top.json()) == 3


@pytest.mark.asyncio
async def test_cache_hit_on_second_identical_call(client: AsyncClient):
    cache.invalidate()
    first = await client.get("/metrics/revenue/by-region")
    assert first.status_code == 200
    before = cache.stats()["hits"]

    second = await client.get("/metrics/revenue/by-region")
    assert second.status_code == 200
    assert second.json() == first.json()
    assert cache.stats()["hits"] == before + 1


@pytest.mark.asyncio
async def test_cache_invalidate_resets_stats(client: AsyncClient):
    stats_resp = await client.get("/admin/cache/stats")
    assert stats_resp.status_code == 200
    assert "hit_rate" in stats_resp.json()

    inv = await client.post("/admin/cache/invalidate")
    assert inv.status_code == 200
    body = inv.json()
    assert body["status"] == "success"
    assert body["invalidated_keys_count"] >= 0
    assert cache.stats()["size"] == 0


@pytest.mark.asyncio
async def test_bad_params_return_422(client: AsyncClient):
    resp = await client.get("/metrics/top-customers?top_n=9999")
    assert resp.status_code == 422
