from __future__ import annotations

import uuid

import grpc
import structlog

from services.inventory.app.models import EventInventory, Reservation
from services.inventory.app.services.inventory_service import InventoryService
from services.inventory.app.services.locking import LockAcquisitionError
from services.shared.enums import ReservationStatus
from services.shared.errors import CapacityExceededError, ConflictError, NotFoundError
from services.shared.generated import inventory_pb2, inventory_pb2_grpc

logger = structlog.get_logger(__name__)


def _status_to_proto(status: ReservationStatus) -> int:
    match status:
        case ReservationStatus.PENDING:
            return inventory_pb2.PENDING
        case ReservationStatus.CONFIRMED:
            return inventory_pb2.CONFIRMED
        case ReservationStatus.RELEASED:
            return inventory_pb2.RELEASED
        case ReservationStatus.EXPIRED:
            return inventory_pb2.EXPIRED
        case _:
            return inventory_pb2.RESERVATION_STATUS_UNSPECIFIED


def _reservation_to_proto(reservation: Reservation, *, message: str = "OK") -> inventory_pb2.ReservationResponse:
    return inventory_pb2.ReservationResponse(
        success=True,
        message=message,
        reservation_id=str(reservation.id),
        event_id=str(reservation.event_id),
        order_id=str(reservation.order_id),
        quantity=reservation.quantity,
        status=_status_to_proto(reservation.status),
        expires_at_unix=int(reservation.expires_at.timestamp()),
    )


def _inventory_to_proto(inventory: EventInventory, *, message: str = "OK") -> inventory_pb2.InventoryResponse:
    return inventory_pb2.InventoryResponse(
        success=True,
        message=message,
        event_id=str(inventory.event_id),
        total_capacity=inventory.total_capacity,
        reserved_count=inventory.reserved_count,
        confirmed_count=inventory.confirmed_count,
        available=inventory.available,
        version=inventory.version,
    )


class InventoryServicer(inventory_pb2_grpc.InventoryServiceServicer):
    def __init__(self, service: InventoryService) -> None:
        self._service = service

    async def InitializeInventory(self, request, context):  # noqa: N802 (grpc naming convention)
        inventory = await self._service.initialize_inventory(uuid.UUID(request.event_id), request.total_capacity)
        return _inventory_to_proto(inventory, message="Inventory initialised")

    async def AdjustCapacity(self, request, context):  # noqa: N802
        try:
            inventory = await self._service.adjust_capacity(
                uuid.UUID(request.event_id), request.new_total_capacity
            )
        except NotFoundError as exc:
            await context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        return _inventory_to_proto(inventory, message="Capacity updated")

    async def ReserveSeats(self, request, context):  # noqa: N802
        try:
            reservation = await self._service.reserve_seats(
                event_id=uuid.UUID(request.event_id),
                order_id=uuid.UUID(request.order_id),
                quantity=request.quantity,
            )
        except NotFoundError as exc:
            await context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        except CapacityExceededError as exc:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(exc))
        except ConflictError as exc:
            await context.abort(grpc.StatusCode.ABORTED, str(exc))
        except LockAcquisitionError as exc:
            # The event is under such heavy simultaneous contention that
            # this request couldn't even get a turn within its wait
            # budget. RESOURCE_EXHAUSTED signals "the server is fine,
            # this specific request should retry" — distinct from
            # ABORTED (lost a race, safe to retry immediately) and from
            # FAILED_PRECONDITION (genuinely sold out, retrying won't help).
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, str(exc))
        return _reservation_to_proto(reservation, message="Reservation created")

    async def ConfirmReservation(self, request, context):  # noqa: N802
        try:
            reservation = await self._service.confirm_reservation(uuid.UUID(request.reservation_id))
        except NotFoundError as exc:
            await context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        return _reservation_to_proto(reservation, message="Reservation confirmed")

    async def ReleaseReservation(self, request, context):  # noqa: N802
        try:
            reservation = await self._service.release_reservation(uuid.UUID(request.reservation_id))
        except NotFoundError as exc:
            await context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        return _reservation_to_proto(reservation, message="Reservation released")

    async def GetAvailability(self, request, context):  # noqa: N802
        try:
            inventory = await self._service.get_availability(uuid.UUID(request.event_id))
        except NotFoundError as exc:
            await context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        return _inventory_to_proto(inventory)
