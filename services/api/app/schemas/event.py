from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class EventCreate(BaseModel):
    title: str = Field(min_length=1, max_length=255)
    description: str | None = None
    venue: str = Field(min_length=1, max_length=255)
    starts_at: datetime
    total_seats: int = Field(gt=0)
    price_cents: int = Field(ge=0)


class EventUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = None
    venue: str | None = Field(default=None, min_length=1, max_length=255)
    starts_at: datetime | None = None
    total_seats: int | None = Field(default=None, gt=0)
    price_cents: int | None = Field(default=None, ge=0)


class EventRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    title: str
    description: str | None
    venue: str
    starts_at: datetime
    total_seats: int
    price_cents: int
    organizer_id: uuid.UUID
    created_at: datetime
    updated_at: datetime


class EventSearchParams(BaseModel):
    q: str | None = Field(default=None, description="Free-text search over title")
    venue: str | None = None
    starts_after: datetime | None = None
    starts_before: datetime | None = None
