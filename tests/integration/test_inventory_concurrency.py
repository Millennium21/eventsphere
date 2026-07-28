"""These tests hit a real Postgres + Redis (see tests/conftest.py). They
are the empirical backbone of this project's core claim: concurrent
booking requests for the same event cannot oversell its capacity.
"""

import asyncio
import uuid

import pytest

from services.inventory.app.services.inventory_service import InventoryService
from services.shared.errors import CapacityExceededError, NotFoundError

pytestmark = pytest.mark.integration


@pytest.fixture
def inventory_service(inventory_grpc_server) -> InventoryService:
    _target, service = inventory_grpc_server
    return service


async def test_reserve_confirm_release_lifecycle(inventory_service: InventoryService):
    event_id = uuid.uuid4()
    await inventory_service.initialize_inventory(event_id, total_capacity=5)

    reservation = await inventory_service.reserve_seats(event_id=event_id, order_id=uuid.uuid4(), quantity=2)
    inventory = await inventory_service.get_availability(event_id)
    assert inventory.available == 3

    await inventory_service.confirm_reservation(reservation.id)
    inventory = await inventory_service.get_availability(event_id)
    assert inventory.reserved_count == 0
    assert inventory.confirmed_count == 2
    assert inventory.available == 3

    await inventory_service.release_reservation(reservation.id)
    inventory = await inventory_service.get_availability(event_id)
    assert inventory.available == 5


async def test_reservation_beyond_capacity_is_rejected(inventory_service: InventoryService):
    event_id = uuid.uuid4()
    await inventory_service.initialize_inventory(event_id, total_capacity=3)

    with pytest.raises(CapacityExceededError):
        await inventory_service.reserve_seats(event_id=event_id, order_id=uuid.uuid4(), quantity=4)


async def test_unknown_event_raises_not_found(inventory_service: InventoryService):
    with pytest.raises(NotFoundError):
        await inventory_service.get_availability(uuid.uuid4())


async def test_fifty_concurrent_reservations_never_oversell_ten_seats(inventory_service: InventoryService):
    """The headline test: 50 simultaneous 1-seat reservation attempts
    against an event with 10 seats must yield *exactly* 10 successes —
    not 9 (lost update), not 11 (overbooking) — proving the
    optimistic-lock version column holds under real concurrent load.
    """
    event_id = uuid.uuid4()
    await inventory_service.initialize_inventory(event_id, total_capacity=10)

    async def attempt() -> str:
        try:
            await inventory_service.reserve_seats(event_id=event_id, order_id=uuid.uuid4(), quantity=1)
            return "success"
        except CapacityExceededError:
            return "rejected"

    results = await asyncio.gather(*(attempt() for _ in range(50)))

    assert results.count("success") == 10
    assert results.count("rejected") == 40

    final = await inventory_service.get_availability(event_id)
    assert final.reserved_count == 10
    assert final.available == 0
