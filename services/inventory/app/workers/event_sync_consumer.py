from __future__ import annotations

import uuid
from typing import Any

import structlog

from services.inventory.app.core.config import Settings
from services.inventory.app.services.inventory_service import InventoryService
from services.shared.kafka.consumer import BaseKafkaConsumer
from services.shared.kafka.topics import GROUP_INVENTORY_EVENT_SYNC, Topic

logger = structlog.get_logger(__name__)


async def _handle_message(service: InventoryService, message: dict[str, Any]) -> None:
    event_type = message.get("event_type")
    payload = message.get("payload", {})

    match event_type:
        case Topic.EVENT_CREATED.value:
            event_id = uuid.UUID(payload["event_id"])
            await service.initialize_inventory(event_id, payload["total_seats"])
            logger.info("inventory_initialised_from_event", event_id=str(event_id))
        case Topic.EVENT_UPDATED.value:
            event_id = uuid.UUID(payload["event_id"])
            await service.adjust_capacity(event_id, payload["total_seats"])
            logger.info("inventory_capacity_synced", event_id=str(event_id))
        case _:
            logger.warning("event_sync_consumer_unknown_event_type", event_type=event_type)


async def run_event_sync_consumer(service: InventoryService, settings: Settings) -> None:
    consumer = BaseKafkaConsumer(
        topics=[Topic.EVENT_CREATED, Topic.EVENT_UPDATED],
        group_id=GROUP_INVENTORY_EVENT_SYNC,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        handler=lambda message: _handle_message(service, message),
    )
    await consumer.run()
