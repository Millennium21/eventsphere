from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import Depends, Request
from fastapi.security import OAuth2PasswordBearer
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from services.api.app.core.security import decode_token
from services.api.app.db.session import get_db
from services.api.app.grpc_client.inventory_client import InventoryClient
from services.api.app.models.user import User
from services.api.app.repositories.user_repository import UserRepository
from services.shared.enums import UserRole
from services.shared.errors import AuthenticationError, AuthorizationError
from services.shared.kafka.producer import KafkaEventProducer
from services.shared.pagination import PaginationParams

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")

DbSession = Annotated[AsyncSession, Depends(get_db)]


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)], session: DbSession) -> User:
    payload = decode_token(token, expected_type="access")
    user = await UserRepository(session).get(uuid.UUID(payload["sub"]))
    if user is None or not user.is_active:
        raise AuthenticationError("User not found or inactive")
    return user


CurrentUser = Annotated[User, Depends(get_current_user)]


def require_roles(*roles: UserRole):
    async def _check(current_user: CurrentUser) -> User:
        if current_user.role not in roles:
            raise AuthorizationError(f"Requires one of roles: {', '.join(r.value for r in roles)}")
        return current_user

    return _check


def get_pagination(page: int = 1, page_size: int = 20) -> PaginationParams:
    return PaginationParams(page=page, page_size=page_size)


Pagination = Annotated[PaginationParams, Depends(get_pagination)]


def get_inventory_client(request: Request) -> InventoryClient:
    return request.app.state.inventory_client


def get_kafka_producer(request: Request) -> KafkaEventProducer:
    return request.app.state.kafka_producer


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


InventoryClientDep = Annotated[InventoryClient, Depends(get_inventory_client)]
KafkaProducerDep = Annotated[KafkaEventProducer, Depends(get_kafka_producer)]
RedisDep = Annotated[Redis, Depends(get_redis)]
