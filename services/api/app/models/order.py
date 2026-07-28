from __future__ import annotations

import uuid

from sqlalchemy import CheckConstraint, ForeignKey, Index, Integer, String
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from services.shared.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from services.shared.enums import OrderStatus

SCHEMA = "api"


class Order(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    __tablename__ = "orders"
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_orders_quantity_positive"),
        # One order per (user, idempotency_key): lets POST /orders be
        # safely retried (client timeout, network blip) without risking a
        # duplicate booking. NULL keys are unconstrained — a partial
        # index only enforces uniqueness where a key was actually
        # supplied
        Index(
            "ix_orders_user_idempotency_key",
            "user_id",
            "idempotency_key",
            unique=True,
            postgresql_where="idempotency_key IS NOT NULL",
        ),
        {"schema": SCHEMA},
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey(f"{SCHEMA}.users.id"), nullable=False, index=True
    )
    event_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey(f"{SCHEMA}.events.id"), nullable=False, index=True
    )
    # Cross-service reference by id only — the Inventory service owns
    # this reservation in its own schema; deliberately not a foreign key.
    reservation_id: Mapped[str] = mapped_column(String(64), nullable=False)

    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    total_price_cents: Mapped[int] = mapped_column(Integer, nullable=False)

    status: Mapped[OrderStatus] = mapped_column(
        SqlEnum(
            OrderStatus,
            name="order_status",
            native_enum=False,
            length=20,
            validate_strings=True,
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
        ),
        nullable=False,
        default=OrderStatus.PENDING,
    )

    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
