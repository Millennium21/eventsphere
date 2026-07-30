from __future__ import annotations

import uuid

from fastapi import APIRouter, Header, status

from services.api.app.api.deps import CurrentUser, DbSession, InventoryClientDep, KafkaProducerDep, Pagination
from services.api.app.repositories.event_repository import EventRepository
from services.api.app.repositories.order_repository import OrderRepository
from services.api.app.schemas.order import OrderCreateRequest, OrderRead
from services.api.app.services.booking_service import BookingService
from services.shared.pagination import Page

router = APIRouter(prefix="/orders", tags=["orders"])


def _service(session: DbSession, inventory: InventoryClientDep, kafka: KafkaProducerDep) -> BookingService:
    return BookingService(OrderRepository(session), EventRepository(session), inventory, kafka)


@router.post("", response_model=OrderRead, status_code=status.HTTP_201_CREATED)
async def book_tickets(
    data: OrderCreateRequest,
    session: DbSession,
    inventory: InventoryClientDep,
    kafka: KafkaProducerDep,
    current_user: CurrentUser,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> OrderRead:
    order = await _service(session, inventory, kafka).book_tickets(
        user=current_user,
        event_id=data.event_id,
        quantity=data.quantity,
        idempotency_key=idempotency_key,
    )
    return OrderRead.model_validate(order)


@router.get("", response_model=Page[OrderRead])
async def list_orders(
    session: DbSession,
    inventory: InventoryClientDep,
    kafka: KafkaProducerDep,
    current_user: CurrentUser,
    pagination: Pagination,
) -> Page[OrderRead]:
    page = await _service(session, inventory, kafka).list_orders(user=current_user, pagination=pagination)
    return Page[OrderRead](
        items=[OrderRead.model_validate(o) for o in page.items],
        total=page.total,
        page=page.page,
        page_size=page.page_size,
    )


@router.get("/{order_id}", response_model=OrderRead)
async def get_order(
    order_id: uuid.UUID,
    session: DbSession,
    inventory: InventoryClientDep,
    kafka: KafkaProducerDep,
    current_user: CurrentUser,
) -> OrderRead:
    order = await _service(session, inventory, kafka).get_order(order_id, user=current_user)
    return OrderRead.model_validate(order)


@router.post("/{order_id}/cancel", response_model=OrderRead)
async def cancel_order(
    order_id: uuid.UUID,
    session: DbSession,
    inventory: InventoryClientDep,
    kafka: KafkaProducerDep,
    current_user: CurrentUser,
) -> OrderRead:
    order = await _service(session, inventory, kafka).cancel_order(order_id, user=current_user)
    return OrderRead.model_validate(order)
