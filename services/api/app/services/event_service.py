from __future__ import annotations

import uuid

import structlog

from services.api.app.models.event import Event
from services.api.app.models.user import User
from services.api.app.repositories.event_repository import EventRepository
from services.api.app.schemas.event import EventCreate, EventSearchParams, EventUpdate
from services.shared.enums import UserRole
from services.shared.errors import AuthorizationError, NotFoundError
from services.shared.kafka.producer import KafkaEventProducer
from services.shared.kafka.schemas import EventCreatedPayload, EventUpdatedPayload
from services.shared.kafka.topics import Topic
from services.shared.pagination import Page, PaginationParams

logger = structlog.get_logger(__name__)


class EventService:
    def __init__(self, event_repository: EventRepository, kafka_producer: KafkaEventProducer) -> None:
        self._events = event_repository
        self._kafka = kafka_producer

    async def create(self, data: EventCreate, *, organizer: User) -> Event:
        event = Event(**data.model_dump(), organizer_id=organizer.id)
        await self._events.add(event)
        await self._events.session.commit()

        await self._kafka.publish(
            Topic.EVENT_CREATED,
            key=str(event.id),
            payload=EventCreatedPayload(event_id=event.id, total_seats=event.total_seats),
        )
        logger.info("event_created", event_id=str(event.id), organizer_id=str(organizer.id))
        return event

    async def get(self, event_id: uuid.UUID) -> Event:
        event = await self._events.get(event_id)
        if event is None:
            raise NotFoundError(f"No event {event_id}")
        return event

    async def search(self, params: EventSearchParams, pagination: PaginationParams) -> Page[Event]:
        items, total = await self._events.search(params, limit=pagination.limit, offset=pagination.offset)
        return Page.create(items, total, pagination)

    async def update(self, event_id: uuid.UUID, data: EventUpdate, *, current_user: User) -> Event:
        event = await self.get(event_id)
        self._require_owner_or_admin(event, current_user)

        seats_changed = data.total_seats is not None and data.total_seats != event.total_seats
        for field, value in data.model_dump(exclude_unset=True).items():
            setattr(event, field, value)
        await self._events.session.commit()

        if seats_changed:
            await self._kafka.publish(
                Topic.EVENT_UPDATED,
                key=str(event.id),
                payload=EventUpdatedPayload(event_id=event.id, total_seats=event.total_seats),
            )
        logger.info("event_updated", event_id=str(event.id))
        return event

    async def delete(self, event_id: uuid.UUID, *, current_user: User) -> None:
        event = await self.get(event_id)
        self._require_owner_or_admin(event, current_user)
        await self._events.delete(event)
        await self._events.session.commit()
        logger.info("event_deleted", event_id=str(event_id))

    def _require_owner_or_admin(self, event: Event, current_user: User) -> None:
        if current_user.role != UserRole.ADMIN and event.organizer_id != current_user.id:
            raise AuthorizationError("You do not have permission to modify this event")
