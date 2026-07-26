"""Shared enums used by both services and by Kafka event payloads.

Using StrEnum (3.11+) means these members serialize as plain strings in
Pydantic/JSON without a custom encoder, while still comparing equal to
plain str values coming back from the database (see the models in each
service for how they're persisted).
"""

from enum import StrEnum, auto


class UserRole(StrEnum):
    ATTENDEE = auto()
    ORGANIZER = auto()
    ADMIN = auto()


class OrderStatus(StrEnum):
    PENDING = auto()
    CONFIRMED = auto()
    CANCELLED = auto()
    EXPIRED = auto()


class ReservationStatus(StrEnum):
    PENDING = auto()
    CONFIRMED = auto()
    RELEASED = auto()
    EXPIRED = auto()
