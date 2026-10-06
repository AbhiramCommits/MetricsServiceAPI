import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metrics_service.etl.watermark import get_watermark
from metrics_service.logging_config import run_id_var

logger = logging.getLogger("metrics_service.etl")


@asynccontextmanager
async def track_run(session: AsyncSession, table_name: str):
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    started_at = datetime.now(timezone.utc)
    watermark_before = await get_watermark(session, table_name)
    token = run_id_var.set(run_id)
    logger.info(
        "etl_run_started",
        extra={
            "run_id": run_id,
            "table_name": table_name,
            "watermark_before": str(watermark_before),
        },
    )

    metrics = {
        "inserted": 0,
        "updated": 0,
        "deleted": 0,
        "watermark_after": watermark_before,
    }

    try:
        # Record the running state in its own committed transaction so the
        # row survives a rollback of the actual load.
        await session.execute(
            text(
                """
                INSERT INTO warehouse.etl_run_history
                (run_id, table_name, started_at, status, watermark_before,
                 rows_inserted, rows_updated, rows_deleted)
                VALUES (:run_id, :table_name, :started_at, 'running',
                        :watermark_before, 0, 0, 0)
                """
            ),
            {
                "run_id": run_id,
                "table_name": table_name,
                "started_at": started_at,
                "watermark_before": watermark_before,
            },
        )
        await session.commit()
    except Exception:
        await session.rollback()
        run_id_var.reset(token)
        raise

    try:
        yield metrics
        finished_at = datetime.now(timezone.utc)
        await session.execute(
            text(
                """
                UPDATE warehouse.etl_run_history
                SET finished_at = :finished_at, status = 'success',
                    rows_inserted = :inserted, rows_updated = :updated,
                    rows_deleted = :deleted, watermark_after = :watermark_after
                WHERE run_id = :run_id
                """
            ),
            {
                "finished_at": finished_at,
                "inserted": metrics["inserted"],
                "updated": metrics["updated"],
                "deleted": metrics["deleted"],
                "watermark_after": metrics["watermark_after"],
                "run_id": run_id,
            },
        )
        await session.commit()
        logger.info(
            "etl_run_finished",
            extra={
                "run_id": run_id,
                "table_name": table_name,
                "status": "success",
                "rows_inserted": metrics["inserted"],
                "rows_updated": metrics["updated"],
                "rows_deleted": metrics["deleted"],
            },
        )
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        await session.rollback()
        try:
            await session.execute(
                text(
                    """
                    UPDATE warehouse.etl_run_history
                    SET finished_at = :finished_at, status = 'failed',
                        error_message = :err
                    WHERE run_id = :run_id
                    """
                ),
                {"finished_at": finished_at, "err": str(exc), "run_id": run_id},
            )
            await session.commit()
        except Exception:
            await session.rollback()
        logger.error(
            "etl_run_failed",
            extra={
                "run_id": run_id,
                "table_name": table_name,
                "status": "failed",
                "error": str(exc),
            },
        )
        raise
    finally:
        run_id_var.reset(token)
