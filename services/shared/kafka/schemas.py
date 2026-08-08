"""Event envelope + payload schemas for every message on the bus.

Every message is wrapped in `EventEnvelope` (event_id, event_type,
occurred_at, payload) so consumers can log/trace/deduplicate on
`event_id` regardless of which topic it came from, and so the payload
shape is versioned independently of the transport wrapper.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from services.shared.kafka.topics import EventType


class EventEnvelope(BaseModel):
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    event_type: str
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    payload: dict[str, Any]

    @classmethod
    def wrap(cls, event_type: EventType, payload: BaseModel) -> EventEnvelope:
        # event_type is stamped from the logical EventType, not from
        # whichever physical Topic it happens to be published on - see
        # topics.py's docstring for why that separation matters.
        return cls(event_type=event_type.value, payload=payload.model_dump(mode="json"))


class EventCreatedPayload(BaseModel):
    event_id: uuid.UUID
    total_seats: int


class EventUpdatedPayload(BaseModel):
    event_id: uuid.UUID
    total_seats: int


class OrderCreatedPayload(BaseModel):
    order_id: uuid.UUID
    event_id: uuid.UUID
    user_id: uuid.UUID
    quantity: int
    total_price_cents: int
    reservation_id: str


class PaymentProcessedPayload(BaseModel):
    order_id: uuid.UUID
    success: bool
    transaction_ref: str


class TicketIssuedPayload(BaseModel):
    order_id: uuid.UUID
    user_id: uuid.UUID
    event_id: uuid.UUID


class OrderCancelledPayload(BaseModel):
    order_id: uuid.UUID
    reservation_id: str


class ReservationExpiredPayload(BaseModel):
    reservation_id: str
    order_id: uuid.UUID
    event_id: uuid.UUID
