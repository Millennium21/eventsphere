from __future__ import annotations

from sqlalchemy import select

from services.api.app.models.user import User
from services.shared.db.repository import BaseRepository

class UserRepository(BaseRepository[User]):
    model = User

    async def get_by_email(self, email: str) -> User | None:
        stmt = select(User).where(User.email == email)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
