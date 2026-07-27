from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from services.shared.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from services.shared.enums import ReservationStatus

SCHEMA = "inventory"


class EventInventory(Base):
    """One row per event, owned exclusively by the Inventory service.

    `version` backs SQLAlchemy's built-in optimistic concurrency check:
    every UPDATE includes `WHERE version = :expected` and SQLAlchemy
    raises StaleDataError if another transaction won the race and moved
    the version first. See InventoryRepository.reserve for the retry loop
    that handles that. A short-lived Redis lock (services/locking.py) sits
    in front of this to cut down on how often that race actually happens
    for a hot event, but the version column is what actually *guarantees*
    correctness — the lock is a performance optimisation, not the safety
    mechanism.
    """

    __tablename__ = "event_inventory"
    __table_args__ = {"schema": SCHEMA}

    event_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    total_capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    reserved_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    confirmed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __mapper_args__ = {"version_id_col": version, "eager_defaults": True}

    @property
    def available(self) -> int:
        return self.total_capacity - self.reserved_count - self.confirmed_count


class Reservation(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """A hold against an event's inventory, correlated back to an Order in
    the API service purely by `order_id` (a plain UUID column, not a
    foreign key — the two services own separate schemas and never share
    a cross-schema FK, by design).
    """

    __tablename__ = "reservations"
    __table_args__ = {"schema": SCHEMA}
    __mapper_args__ = {"eager_defaults": True}

    event_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.event_inventory.event_id"), nullable=False, index=True
    )
    order_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[ReservationStatus] = mapped_column(
        SqlEnum(
            ReservationStatus,
            name="reservation_status",
            native_enum=False,
            length=20,
            validate_strings=True,
            # Persist the StrEnum's lowercase .value ("pending") rather
            # than SQLAlchemy's default of the member .name ("PENDING"),
            # so raw DB rows match what the API/gRPC layer serialises.
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
        ),
        nullable=False,
        default=ReservationStatus.PENDING,
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
