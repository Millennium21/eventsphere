from __future__ import annotations

import uuid
from typing import Any

import structlog

from services.api.app.core.config import Settings
from services.api.app.db.session import SessionFactory
from services.api.app.models.order import Order
from services.api.app.models.user import User
from services.shared.kafka.consumer import BaseKafkaConsumer
from services.shared.kafka.topics import GROUP_API_NOTIFICATION_WORKER, EventType, Topic

logger = structlog.get_logger(__name__)


async def _send_mock_email(*, to: str, subject: str, order_id: uuid.UUID) -> None:
    """Stands in for a real transactional-email provider (SES, Postmark,
    SendGrid). Logging the "send" keeps the demo self-contained and
    still exercises the full consumer -> side-effect path end to end.
    """
    logger.info("mock_email_sent", to=to, subject=subject, order_id=str(order_id))


async def _handle_message(message: dict[str, Any]) -> None:
    event_type = message.get("event_type")
    payload = message.get("payload", {})
    order_id = uuid.UUID(payload["order_id"])

    async with SessionFactory() as session:
        order = await session.get(Order, order_id)
        if order is None:
            return
        user = await session.get(User, order.user_id)
        if user is None:
            return

    match event_type:
        case EventType.TICKET_ISSUED.value:
            await _send_mock_email(to=user.email, subject="Your tickets are confirmed!", order_id=order_id)
        case EventType.ORDER_CANCELLED.value:
            await _send_mock_email(to=user.email, subject="Your order was cancelled", order_id=order_id)
        case _:
            logger.warning("notification_worker_unknown_event_type", event_type=event_type)


async def run_notification_worker(settings: Settings) -> None:
    # TICKET_ISSUED and ORDER_CANCELLED now share one physical topic
    # (Topic.ORDERS_NOTIFICATIONS) - see topics.py.
    consumer = BaseKafkaConsumer(
        topics=[Topic.ORDERS_NOTIFICATIONS],
        group_id=GROUP_API_NOTIFICATION_WORKER,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        handler=_handle_message,
        security_protocol=settings.kafka_security_protocol,
        sasl_mechanism=settings.kafka_sasl_mechanism,
        sasl_username=settings.kafka_sasl_username,
        sasl_password=settings.kafka_sasl_password,
    )
    await consumer.run()
