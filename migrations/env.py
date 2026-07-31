from __future__ import annotations

import asyncio
import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Import every model from every service so Base.metadata is complete —
# this is the one place in the repo that's allowed to know about both
# services at once, since a single migration history is what the
# `migrations/` folder in this project's layout implies (see the
# README's "why one migrations folder" note for the schema-per-service
# alternative this trades off against).
from services.api.app.models import Event, Order, User  # noqa: E402, F401
from services.inventory.app.models import EventInventory, Reservation  # noqa: E402, F401
from services.shared.db.base import Base  # noqa: E402

config = context.config

database_url = os.environ.get("ALEMBIC_DATABASE_URL") or os.environ.get(
    "DATABASE_URL", "postgresql+asyncpg://eventsphere:eventsphere_pw@localhost:5432/eventsphere"
)
config.set_main_option("sqlalchemy.url", database_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        version_table_schema="public",
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        version_table_schema="public",
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
