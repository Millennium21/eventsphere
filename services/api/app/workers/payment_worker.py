from __future__ import annotations

import asyncio
import random
import uuid
from typing import Any

import structlog

from services.api.app.core.config import Settings
from services.shared.kafka.consumer import BaseKafkaConsumer
from services.shared.kafka.producer import KafkaEventProducer
from services.shared.kafka.schemas import PaymentProcessedPayload
from services.shared.kafka.topics import GROUP_API_PAYMENT_WORKER, EventType, Topic

logger = structlog.get_logger(__name__)


async def _mock_charge(order_id: uuid.UUID, amount_cents: int) -> tuple[bool, str]:
    """Stands in for a real payment gateway call (Stripe, Adyen, etc.).

    Simulates realistic gateway latency and a small failure rate so the
    downstream flow (and anyone reading this repo) has to deal with the
    unhappy path, not just the happy one.
    """
    await asyncio.sleep(random.uniform(0.05, 0.2))
    success = random.random() > 0.05  # ~95% success rate
    transaction_ref = f"mock_txn_{uuid.uuid4().hex[:12]}"
    logger.info("payment_gateway_call", order_id=str(order_id), amount_cents=amount_cents, success=success)
    return success, transaction_ref


async def _handle_message(producer: KafkaEventProducer, message: dict[str, Any]) -> None:
    payload = message.get("payload", {})
    order_id = uuid.UUID(payload["order_id"])
    success, transaction_ref = await _mock_charge(order_id, payload["total_price_cents"])

    await producer.publish(
        EventType.PAYMENT_PROCESSED,
        key=str(order_id),
        payload=PaymentProcessedPayload(order_id=order_id, success=success, transaction_ref=transaction_ref),
    )


async def run_payment_worker(settings: Settings) -> None:
    producer = KafkaEventProducer(
        settings.kafka_bootstrap_servers,
        security_protocol=settings.kafka_security_protocol,
        sasl_mechanism=settings.kafka_sasl_mechanism,
        sasl_username=settings.kafka_sasl_username,
        sasl_password=settings.kafka_sasl_password,
    )
    await producer.start()
    try:
        consumer = BaseKafkaConsumer(
            topics=[Topic.ORDERS_CREATED],
            group_id=GROUP_API_PAYMENT_WORKER,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            handler=lambda message: _handle_message(producer, message),
            security_protocol=settings.kafka_security_protocol,
            sasl_mechanism=settings.kafka_sasl_mechanism,
            sasl_username=settings.kafka_sasl_username,
            sasl_password=settings.kafka_sasl_password,
        )
        await consumer.run()
    finally:
        await producer.stop()
