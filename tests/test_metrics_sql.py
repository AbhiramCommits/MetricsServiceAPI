from __future__ import annotations

import pathlib
from datetime import date

import pytest
from sqlalchemy import text

from metrics_service.db import AsyncSessionLocal

SQL_DIR = pathlib.Path("metrics_service/sql/metrics")

EXPECTED_DAILY = {
    date(2024, 1, 1): (3, 22000, 22000.0),
    date(2024, 1, 2): (2, 23000, 22500.0),
    date(2024, 1, 9): (1, 15000, 20000.0),
    date(2024, 1, 15): (1, 25000, 21250.0),
}

EXPECTED_REGION = {
    "North": (3, 55000, 64.71, 1),
    "South": (2, 20000, 23.53, 2),
    "East": (2, 10000, 11.76, 3),
}


@pytest.mark.asyncio
async def test_revenue_daily_moving_average():
    async with AsyncSessionLocal() as session:
        sql = (SQL_DIR / "revenue_daily.sql").read_text()
        rows = (
            await session.execute(
                text(sql), {"start_date": None, "end_date": None, "limit": 100}
            )
        ).fetchall()
        assert len(rows) == 4

        for row in rows:
            order_count, total, moving_avg = EXPECTED_DAILY[row.order_date]
            assert row.order_count == order_count
            assert row.total_revenue_cents == total
            assert float(row.moving_avg_revenue_7d) == pytest.approx(moving_avg)


@pytest.mark.asyncio
async def test_revenue_by_region_rank_and_share():
    async with AsyncSessionLocal() as session:
        sql = (SQL_DIR / "revenue_by_region.sql").read_text()
        rows = (await session.execute(text(sql))).fetchall()
        assert len(rows) == 3

        for row in rows:
            order_count, total, share, rank = EXPECTED_REGION[row.region]
            assert row.order_count == order_count
            assert row.total_revenue_cents == total
            assert float(row.share_of_total_pct) == pytest.approx(share, abs=0.01)
            assert row.revenue_rank == rank


@pytest.mark.asyncio
async def test_top_customers_row_number():
    async with AsyncSessionLocal() as session:
        sql = (SQL_DIR / "top_customers.sql").read_text()
        rows = (await session.execute(text(sql), {"top_n": 5})).fetchall()
        assert len(rows) == 3

        by_segment = {r.segment: r for r in rows}
        assert by_segment["Enterprise"].customer_id == "CUST_1"
        assert by_segment["Enterprise"].total_spent_cents == 55000
        assert by_segment["Enterprise"].rank_in_segment == 1
        assert by_segment["SMB"].total_spent_cents == 20000
        assert by_segment["Consumer"].total_spent_cents == 10000
        assert all(r.rank_in_segment == 1 for r in rows)


@pytest.mark.asyncio
async def test_customer_cohorts_first_order_month():
    async with AsyncSessionLocal() as session:
        sql = (SQL_DIR / "customer_cohorts.sql").read_text()
        rows = (await session.execute(text(sql))).fetchall()
        assert len(rows) == 1

        row = rows[0]
        assert row.cohort_month == date(2024, 1, 1)
        assert row.order_month == date(2024, 1, 1)
        assert row.cohort_size == 3
        assert row.active_customers == 3
        assert float(row.retention_rate_pct) == pytest.approx(100.0)
