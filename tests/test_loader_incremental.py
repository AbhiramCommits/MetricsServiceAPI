from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from metrics_service.db import AsyncSessionLocal
from metrics_service.etl import loader
from metrics_service.etl.loader import load_dim_customer, load_fact_orders, run_all
from metrics_service.etl.watermark import get_watermark

CDC_TS_1 = datetime(2024, 2, 1, tzinfo=timezone.utc)
CDC_TS_2 = datetime(2024, 3, 1, tzinfo=timezone.utc)
CDC_TS_3 = datetime(2024, 4, 1, tzinfo=timezone.utc)


async def _insert_customer(
    session, customer_id: str, region: str, segment: str, ts: datetime, op: str
) -> None:
    await session.execute(
        text(
            "INSERT INTO staging.stg_customers "
            "(customer_id, region, segment, source_updated_at, op_type) "
            "VALUES (:cid, :region, :segment, :ts, :op)"
        ),
        {"cid": customer_id, "region": region, "segment": segment, "ts": ts, "op": op},
    )
    await session.commit()


async def _count(session, sql: str, **params) -> int:
    result = await session.execute(text(sql), params)
    return result.scalar_one()


@pytest.mark.asyncio
async def test_full_load_populates_warehouse():
    async with AsyncSessionLocal() as session:
        current_customers = await _count(
            session,
            "SELECT count(*) FROM warehouse.dim_customer WHERE is_current = true",
        )
        assert current_customers == 3

        orders = await _count(session, "SELECT count(*) FROM warehouse.fact_orders")
        assert orders == 7

        run_ok = await _count(
            session,
            "SELECT count(*) FROM warehouse.etl_run_history WHERE status = 'success'",
        )
        assert run_ok >= 2


@pytest.mark.asyncio
async def test_second_run_without_changes_is_noop():
    async with AsyncSessionLocal() as session:
        before = await get_watermark(session, "dim_customer")

        res_dim = await load_dim_customer(session)
        res_orders = await load_fact_orders(session)

        assert res_dim["inserted"] == 0
        assert res_dim["updated"] == 0
        assert res_orders["inserted"] == 0
        assert res_orders["updated"] == 0

        after = await get_watermark(session, "dim_customer")
        assert before == after


@pytest.mark.asyncio
async def test_cdc_update_creates_one_new_scd2_version_and_expires_old():
    async with AsyncSessionLocal() as session:
        await _insert_customer(
            session, "CUST_1", "North-Updated", "Enterprise", CDC_TS_1, "U"
        )
        res = await load_dim_customer(session)
        assert res["updated"] == 1
        assert res["inserted"] == 1

        result = await session.execute(
            text(
                "SELECT region, is_current, valid_to FROM warehouse.dim_customer "
                "WHERE customer_id = 'CUST_1' ORDER BY valid_from"
            )
        )
        rows = result.fetchall()
        assert len(rows) == 2
        assert rows[0].is_current is False
        assert rows[0].valid_to is not None
        assert rows[1].is_current is True
        assert rows[1].region == "North-Updated"


@pytest.mark.asyncio
async def test_cdc_delete_soft_expires_customer():
    async with AsyncSessionLocal() as session:
        await _insert_customer(session, "CUST_3", "East", "Consumer", CDC_TS_2, "D")
        res = await load_dim_customer(session)
        assert res["updated"] == 1
        assert res["inserted"] == 0

        current = await _count(
            session,
            "SELECT count(*) FROM warehouse.dim_customer "
            "WHERE customer_id = 'CUST_3' AND is_current = true",
        )
        assert current == 0


@pytest.mark.asyncio
async def test_failed_load_does_not_advance_watermark(monkeypatch):
    async with AsyncSessionLocal() as session:
        before = await get_watermark(session, "dim_customer")

        async def _boom(*args, **kwargs):
            raise RuntimeError("injected failure")

        monkeypatch.setattr(loader, "set_watermark", _boom)

        await _insert_customer(session, "CUST_2", "South-Updated", "SMB", CDC_TS_3, "U")
        with pytest.raises(RuntimeError, match="injected failure"):
            await load_dim_customer(session)

        after = await get_watermark(session, "dim_customer")
        assert before == after

        failed = await _count(
            session,
            "SELECT count(*) FROM warehouse.etl_run_history "
            "WHERE table_name = 'dim_customer' AND status = 'failed'",
        )
        assert failed >= 1


@pytest.mark.asyncio
async def test_run_all_returns_both_tables():
    async with AsyncSessionLocal() as session:
        res = await run_all(session)
        assert set(res.keys()) == {"dim_customer", "fact_orders"}
        assert "inserted" in res["dim_customer"]
        assert "inserted" in res["fact_orders"]
