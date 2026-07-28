"""Verifies the gRPC wire contract itself (message shapes, status codes)
against a real in-process server — distinct from the business-logic
correctness already covered in tests/integration/test_inventory_concurrency.py.
"""

import uuid

import grpc
import pytest

from services.api.app.grpc_client.inventory_client import InventoryClient
from services.shared.errors import CapacityExceededError, NotFoundError
from services.shared.generated import inventory_pb2

pytestmark = pytest.mark.contract


async def test_reserve_seats_response_matches_the_proto_contract(inventory_client: InventoryClient):
    event_id = uuid.uuid4()
    await inventory_client.initialize_inventory(event_id=event_id, total_capacity=10)

    result = await inventory_client.reserve_seats(event_id=event_id, order_id=uuid.uuid4(), quantity=3)

    assert isinstance(result.reservation_id, str) and len(result.reservation_id) > 0
    assert isinstance(result.expires_at_unix, int) and result.expires_at_unix > 0


async def test_reserve_seats_for_unknown_event_maps_to_not_found(inventory_client: InventoryClient):
    with pytest.raises(NotFoundError):
        await inventory_client.reserve_seats(event_id=uuid.uuid4(), order_id=uuid.uuid4(), quantity=1)


async def test_reserve_seats_beyond_capacity_maps_to_capacity_exceeded(inventory_client: InventoryClient):
    event_id = uuid.uuid4()
    await inventory_client.initialize_inventory(event_id=event_id, total_capacity=2)

    with pytest.raises(CapacityExceededError):
        await inventory_client.reserve_seats(event_id=event_id, order_id=uuid.uuid4(), quantity=3)


async def test_raw_stub_exposes_the_full_service_surface(inventory_client: InventoryClient):
    """Guards against accidentally dropping an RPC from the .proto file
    without noticing — every method declared on the service should be
    callable on the generated stub."""
    expected_methods = {
        "InitializeInventory",
        "AdjustCapacity",
        "ReserveSeats",
        "ConfirmReservation",
        "ReleaseReservation",
        "GetAvailability",
    }
    actual_methods = {m.name for m in inventory_pb2.DESCRIPTOR.services_by_name["InventoryService"].methods}
    assert expected_methods == actual_methods
    for name in expected_methods:
        assert hasattr(inventory_client.stub, name)


async def test_get_availability_for_unknown_event_raises_grpc_not_found(inventory_client: InventoryClient):
    with pytest.raises(NotFoundError):
        await inventory_client.reserve_seats(event_id=uuid.uuid4(), order_id=uuid.uuid4(), quantity=1)

    # Also confirm the underlying grpc status code directly, one level
    # below the client's translated domain error.
    request = inventory_pb2.EventIdRequest(event_id=str(uuid.uuid4()))
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await inventory_client.stub.GetAvailability(request, timeout=5.0)
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND
