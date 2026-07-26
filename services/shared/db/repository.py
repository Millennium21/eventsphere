from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from services.shared.db.base import Base


class BaseRepository[ModelType: Base]:
    """Generic async repository providing common CRUD operations.

    Uses PEP 695 (Python 3.12) generic class syntax. Service-specific
    repositories subclass this with their SQLAlchemy model to inherit
    get/list/add/delete, and add their own domain-specific queries
    (searching, filtering, locked reads, etc.) on top.
    """

    model: type[ModelType]

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, id_: Any) -> ModelType | None:
        return await self.session.get(self.model, id_)

    async def list(self, *, limit: int = 20, offset: int = 0) -> Sequence[ModelType]:
        stmt = select(self.model).limit(limit).offset(offset)
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def add(self, instance: ModelType) -> ModelType:
        self.session.add(instance)
        await self.session.flush()
        return instance

    async def delete(self, instance: ModelType) -> None:
        await self.session.delete(instance)
        await self.session.flush()
