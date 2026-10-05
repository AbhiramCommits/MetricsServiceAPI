from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

async def get_watermark(session: AsyncSession, table_name: str) -> datetime:
    query = text("SELECT last_watermark FROM warehouse.etl_watermark WHERE table_name = :t")
    result = await session.execute(query, {"t": table_name})
    row = result.fetchone()
    if not row:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    return row[0]

async def set_watermark(session: AsyncSession, table_name: str, ts: datetime) -> None:
    query = text("""
        INSERT INTO warehouse.etl_watermark (table_name, last_watermark, updated_at)
        VALUES (:t, :ts, NOW())
        ON CONFLICT (table_name) DO UPDATE 
        SET last_watermark = EXCLUDED.last_watermark, updated_at = NOW()
    """)
    await session.execute(query, {"t": table_name, "ts": ts})
