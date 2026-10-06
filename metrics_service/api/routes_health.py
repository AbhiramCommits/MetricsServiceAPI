from typing import Any, cast

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metrics_service.api.routes_metrics import cache
from metrics_service.db import engine, get_session

router = APIRouter(tags=["health & admin"])


@router.get("/health")
async def health_check(session: AsyncSession = Depends(get_session)):
    try:
        await session.execute(text("SELECT 1"))
        pool = cast(Any, engine.pool)
        pool_stats = {
            "size": pool.size(),
            "checkedin": pool.checkedin(),
            "checkedout": pool.checkedout(),
            "overflow": pool.overflow(),
        }
        return {"status": "healthy", "database": "connected", "pool": pool_stats}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unhealthy: {str(e)}")


@router.get("/health/pipelines")
async def pipeline_health(
    stale_threshold_seconds: int = Query(
        86400, description="Seconds before a pipeline is considered stale"
    ),
    session: AsyncSession = Depends(get_session),
):
    query = text("""
        WITH latest_runs AS (
            SELECT 
                table_name,
                status,
                finished_at,
                rows_inserted,
                rows_updated,
                rows_deleted,
                watermark_after,
                ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY started_at DESC) as rn
            FROM warehouse.etl_run_history
        ),
        watermarks AS (
            SELECT table_name, last_watermark, updated_at
            FROM warehouse.etl_watermark
        )
        SELECT 
            w.table_name,
            w.last_watermark,
            lr.status as last_run_status,
            lr.finished_at as last_run_finished_at,
            lr.rows_inserted,
            lr.rows_updated,
            lr.rows_deleted,
            EXTRACT(EPOCH FROM (NOW() - w.last_watermark)) as lag_seconds
        FROM watermarks w
        LEFT JOIN latest_runs lr ON w.table_name = lr.table_name AND lr.rn = 1
    """)
    result = await session.execute(query)
    rows = result.fetchall()

    pipelines = []
    for row in rows:
        lag = row.lag_seconds or 0
        is_stale = lag > stale_threshold_seconds
        pipelines.append(
            {
                "table_name": row.table_name,
                "last_watermark": row.last_watermark,
                "last_run_status": row.last_run_status or "unknown",
                "last_run_finished_at": row.last_run_finished_at,
                "rows_inserted": row.rows_inserted or 0,
                "rows_updated": row.rows_updated or 0,
                "rows_deleted": row.rows_deleted or 0,
                "lag_seconds": round(lag, 2),
                "stale": is_stale,
            }
        )

    return {"pipelines": pipelines}


@router.get("/admin/cache/stats")
async def cache_stats():
    return cache.stats()


@router.post("/admin/cache/invalidate")
async def invalidate_cache(
    prefix: str = Query(None, description="Optional cache key prefix to invalidate"),
):
    count = cache.invalidate(prefix)
    return {"status": "success", "invalidated_keys_count": count}
