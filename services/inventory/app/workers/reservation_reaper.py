from __future__ import annotations

import asyncio

import structlog

from services.inventory.app.core.config import Settings
from services.inventory.app.services.inventory_service import InventoryService
from services.shared.kafka.producer import KafkaEventProducer
from services.shared.kafka.schemas import ReservationExpiredPayload
from services.shared.kafka.topics import EventType

logger = structlog.get_logger(__name__)


async def run_reservation_reaper(service: InventoryService, settings: Settings) -> None:
    """Polls for PENDING reservations past their TTL and releases them.

    A booking that's reserved-but-never-paid (abandoned checkout, or the
    API crashed after reserving but before it finished writing the Order
    row) would otherwise hold seats forever. This is the system's
    eventual-consistency backstop for that case, favoured here over a
    distributed saga/2PC: it trades a few minutes of held-but-abandoned
    inventory for much simpler code, which is a reasonable trade for a
    ticketing system where reservations are already short-lived.
    """
    producer = KafkaEventProducer(
        settings.kafka_bootstrap_servers,
        security_protocol=settings.kafka_security_protocol,
        sasl_mechanism=settings.kafka_sasl_mechanism,
        sasl_username=settings.kafka_sasl_username,
        sasl_password=settings.kafka_sasl_password,
    )
    await producer.start()
    try:
        while True:
            released = await service.sweep_expired_reservations()
            for reservation in released:
                await producer.publish(
                    EventType.RESERVATION_EXPIRED,
                    key=str(reservation.event_id),
                    payload=ReservationExpiredPayload(
                        reservation_id=str(reservation.id),
                        order_id=reservation.order_id,
                        event_id=reservation.event_id,
                    ),
                )
            await asyncio.sleep(settings.reaper_interval_seconds)
    finally:
        await producer.stop()
