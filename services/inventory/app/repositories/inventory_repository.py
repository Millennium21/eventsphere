from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import StaleDataError

from services.inventory.app.models import EventInventory, Reservation
from services.shared.enums import ReservationStatus
from services.shared.errors import CapacityExceededError, ConflictError, NotFoundError


class InventoryRepository:
    """All persistence logic for EventInventory/Reservation.

    `reserve()` is the operation this whole project's concurrency story
    hinges on: it re-reads the current row, checks capacity, increments
    `reserved_count`, and flushes — relying on SQLAlchemy's version-column
    optimistic locking to raise StaleDataError if another transaction
    updated the same row first. On that conflict we roll back and retry
    from a fresh read, up to `max_retries` times.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_inventory(self, event_id: uuid.UUID) -> EventInventory | None:
        return await self.session.get(EventInventory, event_id)

    async def initialize_inventory(self, event_id: uuid.UUID, total_capacity: int) -> EventInventory:
        existing = await self.get_inventory(event_id)
        if existing is not None:
            return existing
        inventory = EventInventory(event_id=event_id, total_capacity=total_capacity)
        self.session.add(inventory)
        await self.session.flush()
        return inventory

    async def adjust_capacity(self, event_id: uuid.UUID, new_total_capacity: int) -> EventInventory:
        inventory = await self.get_inventory(event_id)
        if inventory is None:
            raise NotFoundError(f"No inventory record for event {event_id}")
        inventory.total_capacity = new_total_capacity
        await self.session.flush()
        return inventory

    async def reserve(
        self,
        *,
        event_id: uuid.UUID,
        order_id: uuid.UUID,
        quantity: int,
        reservation_ttl_seconds: int,
        max_retries: int = 5,
    ) -> Reservation:
        if quantity <= 0:
            raise ValueError("quantity must be positive")

        for _attempt in range(max_retries):
            inventory = await self.get_inventory(event_id)
            if inventory is None:
                raise NotFoundError(f"No inventory record for event {event_id}")

            if inventory.available < quantity:
                raise CapacityExceededError(
                    f"Only {inventory.available} seat(s) left for event {event_id}, requested {quantity}"
                )

            inventory.reserved_count += quantity
            reservation = Reservation(
                event_id=event_id,
                order_id=order_id,
                quantity=quantity,
                status=ReservationStatus.PENDING,
                expires_at=datetime.now(UTC) + timedelta(seconds=reservation_ttl_seconds),
            )
            self.session.add(reservation)

            try:
                await self.session.flush()
                return reservation
            except StaleDataError:
                # Someone else's concurrent reserve/release won the race on
                # this row's version. Roll back this attempt's uncommitted
                # changes and retry against a fresh read.
                await self.session.rollback()
                continue

        raise ConflictError(
            f"Could not reserve {quantity} seat(s) for event {event_id} after {max_retries} attempts "
            "due to contention"
        )

    async def confirm(self, reservation_id: uuid.UUID) -> Reservation:
        reservation = await self.session.get(Reservation, reservation_id)
        if reservation is None:
            raise NotFoundError(f"No reservation {reservation_id}")
        if reservation.status != ReservationStatus.PENDING:
            return reservation

        inventory = await self.get_inventory(reservation.event_id)
        if inventory is not None:
            inventory.reserved_count -= reservation.quantity
            inventory.confirmed_count += reservation.quantity
        reservation.status = ReservationStatus.CONFIRMED
        await self.session.flush()
        return reservation

    async def release(self, reservation_id: uuid.UUID, *, expired: bool = False) -> Reservation:
        reservation = await self.session.get(Reservation, reservation_id)
        if reservation is None:
            raise NotFoundError(f"No reservation {reservation_id}")
        if reservation.status not in (ReservationStatus.PENDING, ReservationStatus.CONFIRMED):
            return reservation  # already released/expired: idempotent no-op

        inventory = await self.get_inventory(reservation.event_id)
        if inventory is not None:
            if reservation.status == ReservationStatus.PENDING:
                inventory.reserved_count -= reservation.quantity
            else:
                inventory.confirmed_count -= reservation.quantity

        reservation.status = ReservationStatus.EXPIRED if expired else ReservationStatus.RELEASED
        await self.session.flush()
        return reservation

    async def find_expired_pending(self, *, limit: int = 100) -> list[Reservation]:
        now = datetime.now(UTC)
        stmt = (
            select(Reservation)
            .where(Reservation.status == ReservationStatus.PENDING)
            .where(Reservation.expires_at < now)
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())
