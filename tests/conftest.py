"""Shared test fixtures.

Postgres and Redis are real (pointed at localhost by default, matching
docker-compose's service names when run in CI — see the `*_URL` env vars
below). Kafka is faked with an in-memory recorder: exercising the actual
booking/business logic doesn't require a live broker, and the Kafka
wiring itself (producer/consumer, envelope schema) has its own focused
unit tests instead.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import grpc
import httpx
import pytest
import pytest_asyncio
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from services.api.app.api.deps import get_db, get_inventory_client, get_kafka_producer, get_redis
from services.api.app.core.security import hash_password
from services.api.app.grpc_client.inventory_client import InventoryClient
from services.api.app.main import create_app
from services.api.app.models.user import User
from services.inventory.app.grpc_handlers.inventory_servicer import InventoryServicer
from services.inventory.app.services.inventory_service import InventoryService
from services.inventory.app.services.locking import RedisLock
from services.shared.db.base import Base
from services.shared.enums import UserRole
from services.shared.generated import inventory_pb2_grpc
from services.shared.kafka.schemas import EventEnvelope
from services.shared.kafka.topics import EventType

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/eventsphere_test"
)
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://127.0.0.1:6379/1")
TEST_GRPC_PORT = int(os.environ.get("TEST_GRPC_PORT", "50099"))


class FakeKafkaProducer:
    """Records published events instead of sending them anywhere, so
    tests can assert on what *would* have been published without
    needing a live broker.

    Also optionally relays events.created/events.updated straight to an
    InventoryClient, standing in for the real event_sync_consumer that
    would normally receive these via Kafka in production. Without this,
    the Inventory service would never learn an event exists in tests
    (there's no broker to deliver the message), and every booking
    attempt would 404 with "no inventory record" even though the app
    code is correct — this fake exists so tests exercise that same
    events.created -> inventory-initialised contract, just synchronously.
    """

    def __init__(self, inventory_client: InventoryClient | None = None) -> None:
        self.published: list[EventEnvelope] = []
        self._inventory_client = inventory_client

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def publish(self, event_type: EventType, key: str, payload) -> None:  # noqa: ANN001
        envelope = EventEnvelope.wrap(event_type, payload)
        self.published.append(envelope)

        if self._inventory_client is None:
            return
        match event_type:
            case EventType.EVENT_CREATED:
                await self._inventory_client.initialize_inventory(
                    event_id=payload.event_id, total_capacity=payload.total_seats
                )
            case EventType.EVENT_UPDATED:
                await self._inventory_client.adjust_capacity(
                    event_id=payload.event_id, new_total_capacity=payload.total_seats
                )

    def events_of_type(self, event_type: EventType) -> list[EventEnvelope]:
        return [e for e in self.published if e.event_type == event_type.value]


@pytest_asyncio.fixture(scope="session")
async def db_engine():
    engine = create_async_engine(TEST_DATABASE_URL, pool_pre_ping=True)
    async with engine.begin() as conn:
        await conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS api")
        await conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS inventory")
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(db_engine) -> AsyncIterator[AsyncSession]:
    session_factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    # Truncate everything between tests. Simpler and more robust here
    # than nested-transaction rollback, since the service layer commits
    # explicitly mid-flow (see BookingService, EventService).
    async with db_engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[Redis]:
    client = Redis.from_url(TEST_REDIS_URL)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


@pytest_asyncio.fixture
async def inventory_grpc_server(db_engine, redis_client):
    session_factory = async_sessionmaker(db_engine, expire_on_commit=False)

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def session_scope():
        async with session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    lock = RedisLock(redis_client, ttl_ms=3000, retry_attempts=200, retry_delay_seconds=0.03)
    service = InventoryService(session_scope=session_scope, lock=lock, reservation_ttl_seconds=600)

    server = grpc.aio.server()
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryServicer(service), server)
    port = server.add_insecure_port(f"127.0.0.1:{TEST_GRPC_PORT}")
    await server.start()
    try:
        yield f"127.0.0.1:{port}", service
    finally:
        await server.stop(grace=None)


@pytest_asyncio.fixture
async def inventory_client(inventory_grpc_server) -> AsyncIterator[InventoryClient]:
    target, _service = inventory_grpc_server
    client = InventoryClient(target, timeout_seconds=5.0)
    await client.start()
    yield client
    await client.stop()


@pytest.fixture
def fake_kafka_producer(inventory_client: InventoryClient) -> FakeKafkaProducer:
    return FakeKafkaProducer(inventory_client)


@pytest_asyncio.fixture
async def test_user(db_session) -> User:
    user = User(
        email=f"user-{uuid.uuid4().hex[:8]}@example.com",
        hashed_password=hash_password("correct-horse-battery-staple"),
        full_name="Test User",
        role=UserRole.ATTENDEE,
    )
    db_session.add(user)
    await db_session.commit()
    return user


@pytest_asyncio.fixture
async def organizer_user(db_session) -> User:
    user = User(
        email=f"organizer-{uuid.uuid4().hex[:8]}@example.com",
        hashed_password=hash_password("correct-horse-battery-staple"),
        full_name="Test Organizer",
        role=UserRole.ORGANIZER,
    )
    db_session.add(user)
    await db_session.commit()
    return user


@pytest.fixture
def app(db_session, redis_client, inventory_client, fake_kafka_producer):
    application = create_app()

    application.dependency_overrides[get_db] = lambda: db_session
    application.dependency_overrides[get_redis] = lambda: redis_client
    application.dependency_overrides[get_inventory_client] = lambda: inventory_client
    application.dependency_overrides[get_kafka_producer] = lambda: fake_kafka_producer

    # The rate-limit middleware reads request.app.state.redis directly
    # (it's cross-cutting, not tied to a single route's Depends chain),
    # so dependency_overrides alone doesn't reach it — the lifespan that
    # would normally set this never runs under TestClient here, so it's
    # seeded directly instead.
    application.state.redis = redis_client

    return application


@pytest_asyncio.fixture
async def client(app) -> AsyncIterator[httpx.AsyncClient]:
    # An async-native client instead of Starlette's sync TestClient:
    # TestClient bridges to the ASGI app via a background thread with
    # its own event loop, which can't share the DB pool / Redis client
    # created on the pytest-asyncio session loop above. ASGITransport
    # runs the app directly on the current (session) loop, avoiding
    # that entirely.
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as async_client:
        yield async_client


def auth_headers_for(client: httpx.AsyncClient, user: User) -> dict[str, str]:
    """Bypasses login (the test users' plaintext passwords aren't
    tracked here) by minting a token directly with the real signing
    helper — equivalent to a successful login for the purposes of
    exercising authenticated endpoints."""
    from services.api.app.core.security import create_access_token

    token = create_access_token(user.id)
    return {"Authorization": f"Bearer {token}"}
