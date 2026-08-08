from __future__ import annotations

import uuid
from typing import Any

import structlog

from services.api.app.core.config import Settings
from services.api.app.db.session import SessionFactory
from services.api.app.grpc_client.inventory_client import InventoryClient
from services.api.app.models.order import Order
from services.shared.enums import OrderStatus
from services.shared.kafka.consumer import BaseKafkaConsumer
from services.shared.kafka.producer import KafkaEventProducer
from services.shared.kafka.schemas import OrderCancelledPayload, TicketIssuedPayload
from services.shared.kafka.topics import GROUP_API_ORDER_STATUS_WORKER, EventType, Topic

logger = structlog.get_logger(__name__)


async def _handle_payment_processed(
    inventory: InventoryClient, producer: KafkaEventProducer, payload: dict[str, Any]
) -> None:
    order_id = uuid.UUID(payload["order_id"])
    async with SessionFactory() as session:
        order = await session.get(Order, order_id)
        if order is None or order.status != OrderStatus.PENDING:
            return  # unknown order, or already handled (idempotent replay)

        if payload["success"]:
            await inventory.confirm_reservation(reservation_id=order.reservation_id)
            order.status = OrderStatus.CONFIRMED
            await session.commit()
            await producer.publish(
                EventType.TICKET_ISSUED,
                key=str(order.id),
                payload=TicketIssuedPayload(order_id=order.id, user_id=order.user_id, event_id=order.event_id),
            )
            logger.info("order_confirmed", order_id=str(order.id))
        else:
            await inventory.release_reservation(reservation_id=order.reservation_id)
            order.status = OrderStatus.CANCELLED
            await session.commit()
            await producer.publish(
                EventType.ORDER_CANCELLED,
                key=str(order.id),
                payload=OrderCancelledPayload(order_id=order.id, reservation_id=order.reservation_id),
            )
            logger.warning("order_cancelled_payment_failed", order_id=str(order.id))


async def _handle_reservation_expired(payload: dict[str, Any]) -> None:
    order_id = uuid.UUID(payload["order_id"])
    async with SessionFactory() as session:
        order = await session.get(Order, order_id)
        if order is None or order.status != OrderStatus.PENDING:
            return
        order.status = OrderStatus.EXPIRED
        await session.commit()
        logger.info("order_expired", order_id=str(order.id))


async def _handle_message(
    inventory: InventoryClient, producer: KafkaEventProducer, message: dict[str, Any]
) -> None:
    event_type = message.get("event_type")
    payload = message.get("payload", {})

    match event_type:
        case EventType.PAYMENT_PROCESSED.value:
            await _handle_payment_processed(inventory, producer, payload)
        case EventType.RESERVATION_EXPIRED.value:
            await _handle_reservation_expired(payload)
        case _:
            logger.warning("order_status_worker_unknown_event_type", event_type=event_type)


async def run_order_status_worker(settings: Settings) -> None:
    inventory = InventoryClient(
        settings.inventory_grpc_target, timeout_seconds=settings.inventory_grpc_timeout_seconds
    )
    await inventory.start()
    producer = KafkaEventProducer(
        settings.kafka_bootstrap_servers,
        security_protocol=settings.kafka_security_protocol,
        sasl_mechanism=settings.kafka_sasl_mechanism,
        sasl_username=settings.kafka_sasl_username,
        sasl_password=settings.kafka_sasl_password,
    )
    await producer.start()
    try:
        # PAYMENT_PROCESSED and RESERVATION_EXPIRED now share one physical
        # topic (Topic.ORDERS_STATUS_CHANGED) - see topics.py - so this
        # worker subscribes to one topic instead of two, but the
        # match-on-event_type dispatch above is unchanged.
        consumer = BaseKafkaConsumer(
            topics=[Topic.ORDERS_STATUS_CHANGED],
            group_id=GROUP_API_ORDER_STATUS_WORKER,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            handler=lambda message: _handle_message(inventory, producer, message),
            security_protocol=settings.kafka_security_protocol,
            sasl_mechanism=settings.kafka_sasl_mechanism,
            sasl_username=settings.kafka_sasl_username,
            sasl_password=settings.kafka_sasl_password,
        )
        await consumer.run()
    finally:
        await producer.stop()
        await inventory.stop()
