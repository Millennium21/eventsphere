from __future__ import annotations

import uuid

from sqlalchemy import func, select

from services.api.app.models.order import Order
from services.shared.db.repository import BaseRepository

class OrderRepository(BaseRepository[Order]):
    model = Order

    async def get_for_user(self, order_id: uuid.UUID, user_id: uuid.UUID) -> Order | None:
        stmt = select(Order).where(Order.id == order_id, Order.user_id == user_id)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_by_idempotency_key(self, user_id: uuid.UUID, idempotency_key: str) -> Order | None:
        stmt = select(Order).where(Order.user_id == user_id, Order.idempotency_key == idempotency_key)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_for_user(self, user_id: uuid.UUID, *, limit: int, offset: int) -> tuple[list[Order], int]:
        count_stmt = select(func.count()).select_from(Order).where(Order.user_id == user_id)
        total = (await self.session.execute(count_stmt)).scalar_one()

        page_stmt = (
            select(Order)
            .where(Order.user_id == user_id)
            .order_by(Order.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        items = (await self.session.execute(page_stmt)).scalars().all()
        return list(items), total
