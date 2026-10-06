import pathlib
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from metrics_service.etl.watermark import get_watermark, set_watermark
from metrics_service.etl.run_history import track_run


async def load_dim_customer(session: AsyncSession) -> dict:
    async with track_run(session, "dim_customer") as metrics:
        watermark = await get_watermark(session, "dim_customer")
        sql_path = pathlib.Path("metrics_service/sql/load_dim_customer.sql")
        query = text(sql_path.read_text())

        result = await session.execute(query, {"watermark": watermark})
        row = result.fetchone()

        if row and row[3]:
            inserted = row[1] or 0
            updated = row[2] or 0
            deleted = row[0] or 0
            max_ts = row[3]
            metrics["inserted"] = inserted
            metrics["updated"] = updated
            metrics["deleted"] = deleted
            metrics["watermark_after"] = max_ts
            await set_watermark(session, "dim_customer", max_ts)
            return metrics
        return metrics


async def load_fact_orders(session: AsyncSession) -> dict:
    async with track_run(session, "fact_orders") as metrics:
        watermark = await get_watermark(session, "fact_orders")
        sql_path = pathlib.Path("metrics_service/sql/load_fact_orders.sql")
        query = text(sql_path.read_text())

        result = await session.execute(query, {"watermark": watermark})
        row = result.fetchone()

        if row and row[3]:
            deleted = row[0] or 0
            inserted = row[1] or 0
            updated = row[2] or 0
            max_ts = row[3]
            metrics["inserted"] = inserted
            metrics["updated"] = updated
            metrics["deleted"] = deleted
            metrics["watermark_after"] = max_ts
            await set_watermark(session, "fact_orders", max_ts)
            return metrics
        return metrics


async def run_all(session: AsyncSession) -> dict:
    res_cust = await load_dim_customer(session)
    res_orders = await load_fact_orders(session)
    return {"dim_customer": res_cust, "fact_orders": res_orders}
