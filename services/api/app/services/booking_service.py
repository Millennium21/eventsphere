from __future__ import annotations

import uuid

import structlog

from services.api.app.grpc_client.inventory_client import InventoryClient
from services.api.app.models.order import Order
from services.api.app.models.user import User
from services.api.app.repositories.event_repository import EventRepository
from services.api.app.repositories.order_repository import OrderRepository
from services.shared.enums import OrderStatus
from services.shared.errors import AuthorizationError, NotFoundError, ValidationError
from services.shared.kafka.producer import KafkaEventProducer
from services.shared.kafka.schemas import OrderCancelledPayload, OrderCreatedPayload
from services.shared.kafka.topics import Topic
from services.shared.pagination import Page, PaginationParams

logger = structlog.get_logger(__name__)


class BookingService:
    """The booking flow: gRPC reservation, Order persistence, and the
    OrderCreated event that kicks off the async payment/notification
    pipeline (see services/api/app/workers/).

    Ordering matters here: the gRPC reserve call happens before the
    Order row is written, so a failed/rejected reservation never creates
    an orphan Order. The reverse race — inventory reserved successfully
    but the Order insert then fails for an unrelated reason (DB blip) —
    is handled by the reservation's own TTL: an unconfirmed reservation
    with no matching Order simply expires and gets swept by the
    Inventory service's reaper (see reservation_reaper.py), the same
    eventual-consistency mechanism used for abandoned checkouts. A full
    saga/2PC would close that small window entirely at the cost of much
    more moving parts — not a trade this system needs to make.
    """

    def __init__(
        self,
        order_repository: OrderRepository,
        event_repository: EventRepository,
        inventory_client: InventoryClient,
        kafka_producer: KafkaEventProducer,
    ) -> None:
        self._orders = order_repository
        self._events = event_repository
        self._inventory = inventory_client
        self._kafka = kafka_producer

    async def book_tickets(
        self,
        *,
        user: User,
        event_id: uuid.UUID,
        quantity: int,
        idempotency_key: str | None = None,
    ) -> Order:
        if idempotency_key:
            existing = await self._orders.get_by_idempotency_key(user.id, idempotency_key)
            if existing is not None:
                logger.info("order_idempotent_replay", order_id=str(existing.id))
                return existing

        event = await self._events.get(event_id)
        if event is None:
            raise NotFoundError(f"No event {event_id}")

        order_id = uuid.uuid4()
        reservation = await self._inventory.reserve_seats(event_id=event_id, order_id=order_id, quantity=quantity)

        order = Order(
            id=order_id,
            user_id=user.id,
            event_id=event_id,
            reservation_id=reservation.reservation_id,
            quantity=quantity,
            unit_price_cents=event.price_cents,
            total_price_cents=event.price_cents * quantity,
            status=OrderStatus.PENDING,
            idempotency_key=idempotency_key,
        )
        await self._orders.add(order)
        await self._orders.session.commit()

        await self._kafka.publish(
            Topic.ORDER_CREATED,
            key=str(order.id),
            payload=OrderCreatedPayload(
                order_id=order.id,
                event_id=order.event_id,
                user_id=order.user_id,
                quantity=order.quantity,
                total_price_cents=order.total_price_cents,
                reservation_id=order.reservation_id,
            ),
        )
        logger.info("order_created", order_id=str(order.id), event_id=str(event_id), quantity=quantity)
        return order

    async def get_order(self, order_id: uuid.UUID, *, user: User) -> Order:
        order = await self._orders.get_for_user(order_id, user.id)
        if order is None:
            raise NotFoundError(f"No order {order_id}")
        return order

    async def list_orders(self, *, user: User, pagination: PaginationParams) -> Page[Order]:
        items, total = await self._orders.list_for_user(user.id, limit=pagination.limit, offset=pagination.offset)
        return Page.create(items, total, pagination)

    async def cancel_order(self, order_id: uuid.UUID, *, user: User) -> Order:
        order = await self._orders.get_for_user(order_id, user.id)
        if order is None:
            raise NotFoundError(f"No order {order_id}")
        if order.user_id != user.id:
            raise AuthorizationError("You do not have permission to cancel this order")
        if order.status in (OrderStatus.CANCELLED, OrderStatus.EXPIRED):
            raise ValidationError(f"Order is already {order.status.value}")

        await self._inventory.release_reservation(reservation_id=order.reservation_id)
        order.status = OrderStatus.CANCELLED
        await self._orders.session.commit()

        await self._kafka.publish(
            Topic.ORDER_CANCELLED,
            key=str(order.id),
            payload=OrderCancelledPayload(order_id=order.id, reservation_id=order.reservation_id),
        )
        logger.info("order_cancelled", order_id=str(order.id))
        return order
