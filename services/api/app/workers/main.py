from __future__ import annotations

import asyncio

import structlog

from services.api.app.core.config import settings
from services.api.app.workers.notification_worker import run_notification_worker
from services.api.app.workers.order_status_worker import run_order_status_worker
from services.api.app.workers.payment_worker import run_payment_worker
from services.shared.logging import configure_logging

logger = structlog.get_logger(__name__)


async def main() -> None:
    configure_logging(service_name="api-worker", environment=settings.environment, level=settings.log_level)

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(run_payment_worker(settings), name="payment-worker")
            tg.create_task(run_order_status_worker(settings), name="order-status-worker")
            tg.create_task(run_notification_worker(settings), name="notification-worker")
    except* Exception as eg:
        for exc in eg.exceptions:
            logger.error("api_worker_task_failed", error=str(exc), error_type=type(exc).__name__)
        raise


if __name__ == "__main__":
    asyncio.run(main())
