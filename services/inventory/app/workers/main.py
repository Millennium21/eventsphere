from __future__ import annotations

import asyncio

import structlog
from redis.asyncio import Redis

from services.inventory.app.core.config import settings
from services.inventory.app.core.database import session_scope
from services.inventory.app.services.inventory_service import InventoryService
from services.inventory.app.services.locking import RedisLock
from services.inventory.app.workers.event_sync_consumer import run_event_sync_consumer
from services.inventory.app.workers.reservation_reaper import run_reservation_reaper
from services.shared.logging import configure_logging

logger = structlog.get_logger(__name__)


async def main() -> None:
    configure_logging(service_name="inventory-worker", environment=settings.environment, level=settings.log_level)

    redis = Redis.from_url(settings.redis_url)
    lock = RedisLock(redis, ttl_ms=settings.lock_ttl_ms, retry_attempts=settings.lock_retry_attempts)
    service = InventoryService(
        session_scope=session_scope,
        lock=lock,
        reservation_ttl_seconds=settings.reservation_ttl_seconds,
        optimistic_retry_attempts=settings.optimistic_retry_attempts,
    )

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(run_event_sync_consumer(service, settings), name="event-sync-consumer")
            tg.create_task(run_reservation_reaper(service, settings), name="reservation-reaper")
    except* Exception as eg:
        for exc in eg.exceptions:
            logger.error("inventory_worker_task_failed", error=str(exc), error_type=type(exc).__name__)
        raise
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
