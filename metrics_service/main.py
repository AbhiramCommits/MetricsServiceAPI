import time
import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from metrics_service.db import engine
from metrics_service.logging_config import (
    configure_logging,
    new_request_id,
    request_id_var,
)
from metrics_service.api.routes_metrics import router as metrics_router
from metrics_service.api.routes_health import router as health_router

configure_logging()
logger = logging.getLogger("metrics_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting MetricsServiceAPI and initializing DB engine pool")
    yield
    logger.info("Shutting down MetricsServiceAPI, disposing DB engine pool")
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(
        title="MetricsServiceAPI",
        description=(
            "Warehouse metrics service backed by PostgreSQL, an incremental "
            "CDC-style loader, async FastAPI, and an in-process TTL cache. "
            "Every response field is documented against the SQL that produces it."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def add_request_context(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or new_request_id()
        token = request_id_var.set(request_id)
        start_time = time.perf_counter()
        try:
            response = await call_next(request)
        finally:
            request_id_var.reset(token)
        duration_ms = (time.perf_counter() - start_time) * 1000
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Process-Time-Ms"] = f"{duration_ms:.2f}"
        logger.info(
            "request",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": round(duration_ms, 2),
            },
        )
        return response

    app.include_router(metrics_router)
    app.include_router(health_router)

    return app


app = create_app()
