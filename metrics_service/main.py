import time
import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from metrics_service.db import engine
from metrics_service.api.routes_metrics import router as metrics_router
from metrics_service.api.routes_health import router as health_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("metrics_service")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting MetricsServiceAPI and initializing DB engine pool...")
    yield
    # Shutdown
    logger.info("Shutting down MetricsServiceAPI, disposing DB engine pool...")
    await engine.dispose()

def create_app() -> FastAPI:
    app = FastAPI(
        title="MetricsServiceAPI",
        description="Warehouse metrics service backed by PostgreSQL, CDC loader, async FastAPI, and TTL cache.",
        version="1.0.0",
        lifespan=lifespan
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time-Ms"] = str(round(process_time * 1000, 2))
        logger.info(f"Path: {request.url.path} | Method: {request.method} | Status: {response.status_code} | Duration: {process_time:.4f}s")
        return response

    app.include_router(metrics_router)
    app.include_router(health_router)

    return app

app = create_app()
