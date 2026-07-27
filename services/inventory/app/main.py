from __future__ import annotations

import asyncio
import signal

import grpc
import structlog
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection
from redis.asyncio import Redis

from services.inventory.app.core.config import settings
from services.inventory.app.core.database import session_scope
from services.inventory.app.grpc_handlers.inventory_servicer import InventoryServicer
from services.inventory.app.services.inventory_service import InventoryService
from services.inventory.app.services.locking import RedisLock
from services.shared.generated import inventory_pb2, inventory_pb2_grpc
from services.shared.logging import configure_logging

logger = structlog.get_logger(__name__)


async def serve() -> None:
    configure_logging(service_name="inventory", environment=settings.environment, level=settings.log_level)

    redis = Redis.from_url(settings.redis_url)
    lock = RedisLock(
        redis,
        ttl_ms=settings.lock_ttl_ms,
        retry_attempts=settings.lock_retry_attempts,
        retry_delay_seconds=settings.lock_retry_delay_seconds,
    )
    inventory_service = InventoryService(
        session_scope=session_scope,
        lock=lock,
        reservation_ttl_seconds=settings.reservation_ttl_seconds,
        optimistic_retry_attempts=settings.optimistic_retry_attempts,
    )

    server = grpc.aio.server()
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryServicer(inventory_service), server)

    # Standard gRPC health-checking protocol so Kubernetes' native gRPC
    # liveness/readiness probes (`grpc: {port: ...}`, stable since k8s 1.24)
    # can check this service without a sidecar or an exec probe binary.
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    await health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    await health_servicer.set(
        inventory_pb2.DESCRIPTOR.services_by_name["InventoryService"].full_name,
        health_pb2.HealthCheckResponse.SERVING,
    )

    # Server reflection lets `grpcurl -plaintext localhost:50051 list` work
    # without needing the .proto file on hand — handy for local debugging.
    reflection.enable_server_reflection(
        (
            inventory_pb2.DESCRIPTOR.services_by_name["InventoryService"].full_name,
            health_pb2.DESCRIPTOR.services_by_name["Health"].full_name,
            reflection.SERVICE_NAME,
        ),
        server,
    )

    server.add_insecure_port(f"[::]:{settings.grpc_port}")
    await server.start()
    logger.info("inventory_grpc_server_started", port=settings.grpc_port)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()
    logger.info("inventory_grpc_server_stopping")
    await health_servicer.enter_graceful_shutdown()
    await server.stop(grace=10)
    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(serve())
