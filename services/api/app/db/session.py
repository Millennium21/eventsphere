from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from services.api.app.core.config import settings

engine = create_async_engine(settings.database_url, pool_pre_ping=True, pool_size=10, max_overflow=10)
SessionFactory = async_sessionmaker(engine, expire_on_commit=False)


async def get_db() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding one session per request.

    Routers commit explicitly via the service layer (or right after
    calling it) rather than this dependency auto-committing, so a
    handler that raises after a partial write rolls back cleanly here.
    """
    async with SessionFactory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
