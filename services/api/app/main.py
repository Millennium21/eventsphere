from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from services.api.app.api.v1.router import api_router
from services.api.app.core.config import settings
from services.api.app.core.logging import RequestContextMiddleware
from services.api.app.core.rate_limit import RedisRateLimitMiddleware
from services.api.app.db.session import engine
from services.api.app.grpc_client.inventory_client import InventoryClient
from services.shared.errors import (
    AuthenticationError,
    AuthorizationError,
    CapacityExceededError,
    ConflictError,
    DomainError,
    NotFoundError,
    ValidationError,
)
from services.shared.kafka.producer import KafkaEventProducer
from services.shared.logging import configure_logging

logger = structlog.get_logger(__name__)

_ERROR_STATUS_MAP: dict[type[DomainError], int] = {
    NotFoundError: 404,
    ValidationError: 422,
    AuthenticationError: 401,
    AuthorizationError: 403,
    CapacityExceededError: 409,
    ConflictError: 409,
}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(service_name="api", environment=settings.environment, level=settings.log_level)

    app.state.redis = Redis.from_url(settings.redis_url)

    app.state.kafka_producer = KafkaEventProducer(
        settings.kafka_bootstrap_servers,
        security_protocol=settings.kafka_security_protocol,
        sasl_mechanism=settings.kafka_sasl_mechanism,
        sasl_username=settings.kafka_sasl_username,
        sasl_password=settings.kafka_sasl_password,
    )
    await app.state.kafka_producer.start()

    app.state.inventory_client = InventoryClient(
        settings.inventory_grpc_target, timeout_seconds=settings.inventory_grpc_timeout_seconds
    )
    await app.state.inventory_client.start()

    logger.info("api_service_started", environment=settings.environment)
    try:
        yield
    finally:
        await app.state.inventory_client.stop()
        await app.state.kafka_producer.stop()
        await app.state.redis.aclose()
        await engine.dispose()
        logger.info("api_service_stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="EventSphere API",
        description="User-facing REST API for browsing events, authentication, and ticket booking.",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(RequestContextMiddleware)
    app.add_middleware(RedisRateLimitMiddleware, limit_per_minute=settings.rate_limit_per_minute)

    def _make_handler(code: int):
        async def _handler(request: Request, exc: DomainError) -> JSONResponse:
            return JSONResponse(status_code=code, content={"detail": str(exc)})

        return _handler

    for exc_type, status_code in _ERROR_STATUS_MAP.items():
        app.add_exception_handler(exc_type, _make_handler(status_code))

    app.include_router(api_router, prefix=settings.api_prefix)
    return app


app = create_app()
