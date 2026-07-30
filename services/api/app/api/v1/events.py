from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, status

from services.api.app.api.deps import DbSession, KafkaProducerDep, Pagination, require_roles
from services.api.app.models.user import User
from services.api.app.repositories.event_repository import EventRepository
from services.api.app.schemas.event import EventCreate, EventRead, EventSearchParams, EventUpdate
from services.api.app.services.event_service import EventService
from services.shared.enums import UserRole
from services.shared.pagination import Page

router = APIRouter(prefix="/events", tags=["events"])

# Organizer-or-admin gate for the mutation endpoints below. `require_roles`
# returns a dependency callable; wrapping it in Depends() via this
# Annotated alias is what actually makes FastAPI resolve and enforce it
# (assigning the bare callable as a parameter default would not).
OrganizerUser = Annotated[User, Depends(require_roles(UserRole.ORGANIZER, UserRole.ADMIN))]


def _service(session: DbSession, kafka: KafkaProducerDep) -> EventService:
    return EventService(EventRepository(session), kafka)


@router.get("", response_model=Page[EventRead])
async def search_events(
    session: DbSession,
    kafka: KafkaProducerDep,
    pagination: Pagination,
    q: str | None = None,
    venue: str | None = None,
) -> Page[EventRead]:
    params = EventSearchParams(q=q, venue=venue)
    page = await _service(session, kafka).search(params, pagination)
    return Page[EventRead](
        items=[EventRead.model_validate(e) for e in page.items],
        total=page.total,
        page=page.page,
        page_size=page.page_size,
    )


@router.get("/{event_id}", response_model=EventRead)
async def get_event(event_id: uuid.UUID, session: DbSession, kafka: KafkaProducerDep) -> EventRead:
    event = await _service(session, kafka).get(event_id)
    return EventRead.model_validate(event)


@router.post("", response_model=EventRead, status_code=status.HTTP_201_CREATED)
async def create_event(
    data: EventCreate, session: DbSession, kafka: KafkaProducerDep, current_user: OrganizerUser
) -> EventRead:
    event = await _service(session, kafka).create(data, organizer=current_user)
    return EventRead.model_validate(event)


@router.patch("/{event_id}", response_model=EventRead)
async def update_event(
    event_id: uuid.UUID,
    data: EventUpdate,
    session: DbSession,
    kafka: KafkaProducerDep,
    current_user: OrganizerUser,
) -> EventRead:
    event = await _service(session, kafka).update(event_id, data, current_user=current_user)
    return EventRead.model_validate(event)


@router.delete("/{event_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_event(
    event_id: uuid.UUID, session: DbSession, kafka: KafkaProducerDep, current_user: OrganizerUser
) -> None:
    await _service(session, kafka).delete(event_id, current_user=current_user)
