from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Optional
from datetime import date
from pydantic import BaseModel, Field

from metrics_service.db import get_session
from metrics_service.cache import TTLCache
from metrics_service.config import settings
import pathlib

router = APIRouter(prefix="/metrics", tags=["metrics"])
cache = TTLCache(ttl_seconds=settings.CACHE_TTL_SECONDS)


class DailyRevenueResponse(BaseModel):
    order_date: date = Field(
        description="Source SQL: revenue_daily.sql, column: order_date"
    )
    order_count: int = Field(
        description="Source SQL: revenue_daily.sql, column: order_count"
    )
    total_revenue_cents: int = Field(
        description="Source SQL: revenue_daily.sql, column: total_revenue_cents"
    )
    moving_avg_revenue_7d: Optional[float] = Field(
        description="Source SQL: revenue_daily.sql, column: moving_avg_revenue_7d (7-day window AVG)"
    )


class RegionalRevenueResponse(BaseModel):
    region: str = Field(description="Source SQL: revenue_by_region.sql, column: region")
    order_count: int = Field(
        description="Source SQL: revenue_by_region.sql, column: order_count"
    )
    total_revenue_cents: int = Field(
        description="Source SQL: revenue_by_region.sql, column: total_revenue_cents"
    )
    share_of_total_pct: float = Field(
        description="Source SQL: revenue_by_region.sql, column: share_of_total_pct"
    )
    revenue_rank: int = Field(
        description="Source SQL: revenue_by_region.sql, column: revenue_rank (RANK window function)"
    )


class CohortResponse(BaseModel):
    cohort_month: date = Field(
        description="Source SQL: customer_cohorts.sql, column: cohort_month"
    )
    order_month: date = Field(
        description="Source SQL: customer_cohorts.sql, column: order_month"
    )
    cohort_size: int = Field(
        description="Source SQL: customer_cohorts.sql, column: cohort_size"
    )
    active_customers: int = Field(
        description="Source SQL: customer_cohorts.sql, column: active_customers"
    )
    retention_rate_pct: float = Field(
        description="Source SQL: customer_cohorts.sql, column: retention_rate_pct"
    )


class TopCustomerResponse(BaseModel):
    segment: str = Field(description="Source SQL: top_customers.sql, column: segment")
    customer_id: str = Field(
        description="Source SQL: top_customers.sql, column: customer_id"
    )
    region: str = Field(description="Source SQL: top_customers.sql, column: region")
    total_spent_cents: int = Field(
        description="Source SQL: top_customers.sql, column: total_spent_cents"
    )
    order_count: int = Field(
        description="Source SQL: top_customers.sql, column: order_count"
    )
    rank_in_segment: int = Field(
        description="Source SQL: top_customers.sql, column: rank_in_segment (ROW_NUMBER window function)"
    )


@router.get("/revenue/daily", response_model=List[DailyRevenueResponse])
async def get_daily_revenue(
    start_date: Optional[date] = Query(
        None, description="Inclusive lower bound on order_date"
    ),
    end_date: Optional[date] = Query(
        None, description="Inclusive upper bound on order_date"
    ),
    limit: int = Query(100, ge=1, le=1000),
    session: AsyncSession = Depends(get_session),
):
    cache_key = f"revenue:daily:{start_date}:{end_date}:{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    sql = pathlib.Path("metrics_service/sql/metrics/revenue_daily.sql").read_text()
    result = await session.execute(
        text(sql),
        {"start_date": start_date, "end_date": end_date, "limit": limit},
    )
    rows = [dict(row._mapping) for row in result.fetchall()]

    cache.set(cache_key, rows)
    return rows


@router.get("/revenue/by-region", response_model=List[RegionalRevenueResponse])
async def get_revenue_by_region(session: AsyncSession = Depends(get_session)):
    cache_key = "revenue:by_region"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    sql = pathlib.Path("metrics_service/sql/metrics/revenue_by_region.sql").read_text()
    result = await session.execute(text(sql))
    rows = [dict(row._mapping) for row in result.fetchall()]

    cache.set(cache_key, rows)
    return rows


@router.get("/cohorts", response_model=List[CohortResponse])
async def get_cohorts(session: AsyncSession = Depends(get_session)):
    cache_key = "cohorts"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    sql = pathlib.Path("metrics_service/sql/metrics/customer_cohorts.sql").read_text()
    result = await session.execute(text(sql))
    rows = [dict(row._mapping) for row in result.fetchall()]

    cache.set(cache_key, rows)
    return rows


@router.get("/top-customers", response_model=List[TopCustomerResponse])
async def get_top_customers(
    top_n: int = Query(5, ge=1, le=100), session: AsyncSession = Depends(get_session)
):
    cache_key = f"top_customers:{top_n}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    sql = pathlib.Path("metrics_service/sql/metrics/top_customers.sql").read_text()
    result = await session.execute(text(sql), {"top_n": top_n})
    rows = [dict(row._mapping) for row in result.fetchall()]

    cache.set(cache_key, rows)
    return rows
