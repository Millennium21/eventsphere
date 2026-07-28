from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from services.shared.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

SCHEMA = "api"


class Event(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    __tablename__ = "events"
    __table_args__ = {"schema": SCHEMA}
    __mapper_args__ = {"eager_defaults": True}

    title: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    venue: Mapped[str] = mapped_column(String(255), nullable=False)
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    total_seats: Mapped[int] = mapped_column(Integer, nullable=False)

    # Money is stored as integer pennies (never float/Decimal-from-JSON)
    price_cents: Mapped[int] = mapped_column(Integer, nullable=False)

    organizer_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey(f"{SCHEMA}.users.id"), nullable=False, index=True
    )
