from __future__ import annotations

import uuid
from dataclasses import dataclass

import grpc

from services.shared.errors import CapacityExceededError, ConflictError, DomainError, NotFoundError
from services.shared.generated import inventory_pb2, inventory_pb2_grpc


@dataclass(frozen=True, slots=True)
class ReservationResult:
    reservation_id: str
    expires_at_unix: int


def _raise_domain_error(exc: grpc.aio.AioRpcError) -> None:
    match exc.code():
        case grpc.StatusCode.NOT_FOUND:
            raise NotFoundError(exc.details()) from exc
        case grpc.StatusCode.FAILED_PRECONDITION:
            raise CapacityExceededError(exc.details()) from exc
        case grpc.StatusCode.ABORTED | grpc.StatusCode.RESOURCE_EXHAUSTED:
            # Both are retryable-by-the-caller conditions (lost an
            # optimistic-lock race, or the per-event lock was saturated);
            # surfaced identically to API clients as a 409 (see
            # api/deps.py's exception handler).
            raise ConflictError(exc.details()) from exc
        case _:
            raise DomainError(f"Inventory service error: {exc.code()} {exc.details()}") from exc


class InventoryClient:
    def __init__(self, target: str, *, timeout_seconds: float = 5.0) -> None:
        self._target = target
        self._timeout_seconds = timeout_seconds
        self._channel: grpc.aio.Channel | None = None
        self._stub: inventory_pb2_grpc.InventoryServiceStub | None = None

    async def start(self) -> None:
        self._channel = grpc.aio.insecure_channel(self._target)
        self._stub = inventory_pb2_grpc.InventoryServiceStub(self._channel)

    async def stop(self) -> None:
        if self._channel is not None:
            await self._channel.close()

    @property
    def stub(self) -> inventory_pb2_grpc.InventoryServiceStub:
        if self._stub is None:
            raise RuntimeError("InventoryClient.start() must be called first")
        return self._stub

    async def initialize_inventory(self, *, event_id: uuid.UUID, total_capacity: int) -> None:
        request = inventory_pb2.InitializeInventoryRequest(event_id=str(event_id), total_capacity=total_capacity)
        try:
            await self.stub.InitializeInventory(request, timeout=self._timeout_seconds)
        except grpc.aio.AioRpcError as exc:
            _raise_domain_error(exc)

    async def adjust_capacity(self, *, event_id: uuid.UUID, new_total_capacity: int) -> None:
        request = inventory_pb2.AdjustCapacityRequest(
            event_id=str(event_id), new_total_capacity=new_total_capacity
        )
        try:
            await self.stub.AdjustCapacity(request, timeout=self._timeout_seconds)
        except grpc.aio.AioRpcError as exc:
            _raise_domain_error(exc)

    async def reserve_seats(self, *, event_id: uuid.UUID, order_id: uuid.UUID, quantity: int) -> ReservationResult:
        request = inventory_pb2.ReserveSeatsRequest(
            event_id=str(event_id), order_id=str(order_id), quantity=quantity
        )
        try:
            response = await self.stub.ReserveSeats(request, timeout=self._timeout_seconds)
        except grpc.aio.AioRpcError as exc:
            _raise_domain_error(exc)
        return ReservationResult(response.reservation_id, response.expires_at_unix)

    async def confirm_reservation(self, *, reservation_id: str) -> None:
        request = inventory_pb2.ReservationIdRequest(reservation_id=reservation_id)
        try:
            await self.stub.ConfirmReservation(request, timeout=self._timeout_seconds)
        except grpc.aio.AioRpcError as exc:
            _raise_domain_error(exc)

    async def release_reservation(self, *, reservation_id: str) -> None:
        request = inventory_pb2.ReservationIdRequest(reservation_id=reservation_id)
        try:
            await self.stub.ReleaseReservation(request, timeout=self._timeout_seconds)
        except grpc.aio.AioRpcError as exc:
            _raise_domain_error(exc)
