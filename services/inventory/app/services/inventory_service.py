from __future__ import annotations

import uuid
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from services.inventory.app.models import EventInventory, Reservation
from services.inventory.app.repositories.inventory_repository import InventoryRepository
from services.inventory.app.services.locking import RedisLock
from services.shared.errors import NotFoundError

logger = structlog.get_logger(__name__)

SessionScopeFactory = Callable[[], AbstractAsyncContextManager[AsyncSession]]


class InventoryService:
    """Business-logic layer sitting between the gRPC servicer and the
    repository. Owns the "acquire lock, then run a DB transaction" shape
    for every write operation.
    """

    def __init__(
        self,
        *,
        session_scope: SessionScopeFactory,
        lock: RedisLock,
        reservation_ttl_seconds: int,
        optimistic_retry_attempts: int = 5,
    ) -> None:
        self._session_scope = session_scope
        self._lock = lock
        self._reservation_ttl_seconds = reservation_ttl_seconds
        self._optimistic_retry_attempts = optimistic_retry_attempts

    async def initialize_inventory(self, event_id: uuid.UUID, total_capacity: int) -> EventInventory:
        async with self._session_scope() as session:
            repo = InventoryRepository(session)
            return await repo.initialize_inventory(event_id, total_capacity)

    async def adjust_capacity(self, event_id: uuid.UUID, new_total_capacity: int) -> EventInventory:
        async with self._lock.acquire(f"event:{event_id}"):
            async with self._session_scope() as session:
                repo = InventoryRepository(session)
                return await repo.adjust_capacity(event_id, new_total_capacity)

    async def get_availability(self, event_id: uuid.UUID) -> EventInventory:
        async with self._session_scope() as session:
            repo = InventoryRepository(session)
            inventory = await repo.get_inventory(event_id)
            if inventory is None:
                raise NotFoundError(f"No inventory record for event {event_id}")
            return inventory

    async def reserve_seats(self, *, event_id: uuid.UUID, order_id: uuid.UUID, quantity: int) -> Reservation:
        # The Redis lock serialises access to one event's inventory so
        # that, under normal conditions, concurrent requests for the same
        # hot event queue briefly instead of all racing the DB's version
        # check at once. If the lock expires mid-operation (slow query,
        # GC pause, etc.) the version column still prevents an overbook —
        # reserve() below will retry-or-fail correctly either way.
        async with self._lock.acquire(f"event:{event_id}"):
            async with self._session_scope() as session:
                repo = InventoryRepository(session)
                reservation = await repo.reserve(
                    event_id=event_id,
                    order_id=order_id,
                    quantity=quantity,
                    reservation_ttl_seconds=self._reservation_ttl_seconds,
                    max_retries=self._optimistic_retry_attempts,
                )
                logger.info(
                    "seats_reserved",
                    event_id=str(event_id),
                    order_id=str(order_id),
                    quantity=quantity,
                    reservation_id=str(reservation.id),
                )
                return reservation

    async def confirm_reservation(self, reservation_id: uuid.UUID) -> Reservation:
        async with self._session_scope() as session:
            repo = InventoryRepository(session)
            reservation = await repo.confirm(reservation_id)
            logger.info("reservation_confirmed", reservation_id=str(reservation_id))
            return reservation

    async def release_reservation(self, reservation_id: uuid.UUID, *, expired: bool = False) -> Reservation:
        async with self._session_scope() as session:
            repo = InventoryRepository(session)
            reservation = await repo.release(reservation_id, expired=expired)
            logger.info("reservation_released", reservation_id=str(reservation_id), expired=expired)
            return reservation

    async def sweep_expired_reservations(self) -> list[Reservation]:
        """Finds PENDING reservations past their TTL and releases each one.

        Runs periodically from the inventory-worker process (see
        workers/reservation_reaper.py), which publishes a
        `reservations.expired` event per released reservation so the API
        service can mark the corresponding Order as EXPIRED.
        """
        async with self._session_scope() as session:
            repo = InventoryRepository(session)
            expired = await repo.find_expired_pending()
            released: list[Reservation] = []
            for reservation in expired:
                released.append(await repo.release(reservation.id, expired=True))
            if released:
                logger.info("expired_reservations_swept", count=len(released))
            return released
