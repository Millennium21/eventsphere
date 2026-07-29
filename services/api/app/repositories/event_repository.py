from __future__ import annotations

import uuid

from sqlalchemy import func, select

from services.api.app.models.event import Event
from services.api.app.schemas.event import EventSearchParams
from services.shared.db.repository import BaseRepository

class EventRepository(BaseRepository[Event]):
    model = Event

    def _apply_filters(self, stmt, params: EventSearchParams):
        if params.q:
            stmt = stmt.where(Event.title.ilike(f"%{params.q}%"))
        if params.venue:
            stmt = stmt.where(Event.venue.ilike(f"%{params.venue}%"))
        if params.starts_after:
            stmt = stmt.where(Event.starts_at >= params.starts_after)
        if params.starts_before:
            stmt = stmt.where(Event.starts_at <= params.starts_before)
        return stmt

    async def search(self, params: EventSearchParams, *, limit: int, offset: int) -> tuple[list[Event], int]:
        base_stmt = self._apply_filters(select(Event), params)

        count_stmt = self._apply_filters(select(func.count()).select_from(Event), params)
        total = (await self.session.execute(count_stmt)).scalar_one()

        page_stmt = base_stmt.order_by(Event.starts_at.asc()).limit(limit).offset(offset)
        items = (await self.session.execute(page_stmt)).scalars().all()

        return list(items), total

    async def get_by_organizer(self, organizer_id: uuid.UUID, event_id: uuid.UUID) -> Event | None:
        stmt = select(Event).where(Event.id == event_id, Event.organizer_id == organizer_id)
        return (await self.session.execute(stmt)).scalar_one_or_none()
