from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from services.shared.enums import OrderStatus


class OrderCreateRequest(BaseModel):
    event_id: uuid.UUID
    quantity: int = Field(gt=0, le=20)


class OrderRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_id: uuid.UUID
    quantity: int
    unit_price_cents: int
    total_price_cents: int
    status: OrderStatus
    reservation_id: str
    created_at: datetime
    updated_at: datetime
