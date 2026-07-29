from __future__ import annotations

import uuid
from datetime import timedelta

import structlog
from redis.asyncio import Redis

from services.api.app.core.config import settings
from services.api.app.core.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)
from services.api.app.models.user import User
from services.api.app.repositories.user_repository import UserRepository
from services.api.app.schemas.auth import TokenResponse
from services.shared.errors import AuthenticationError, ValidationError

logger = structlog.get_logger(__name__)


class AuthService:
    def __init__(self, user_repository: UserRepository, redis: Redis) -> None:
        self._users = user_repository
        self._redis = redis

    async def register(self, *, email: str, password: str, full_name: str) -> User:
        existing = await self._users.get_by_email(email)
        if existing is not None:
            raise ValidationError("An account with this email already exists")

        user = User(email=email, hashed_password=hash_password(password), full_name=full_name)
        await self._users.add(user)
        await self._users.session.commit()
        logger.info("user_registered", user_id=str(user.id))
        return user

    async def login(self, *, email: str, password: str) -> TokenResponse:
        user = await self._users.get_by_email(email)
        if user is None or not verify_password(password, user.hashed_password):
            raise AuthenticationError("Incorrect email or password")
        if not user.is_active:
            raise AuthenticationError("This account has been deactivated")

        return await self._issue_tokens(user.id)

    async def refresh(self, refresh_token: str) -> TokenResponse:
        payload = decode_token(refresh_token, expected_type="refresh")
        jti = payload["jti"]
        user_id = payload["sub"]

        stored_user_id = await self._redis.get(f"refresh:{jti}")
        if stored_user_id is None:
            raise AuthenticationError("Refresh token has been revoked or has expired")

        # Rotate: the old token is single-use.
        await self._redis.delete(f"refresh:{jti}")
        return await self._issue_tokens(uuid.UUID(user_id))

    async def logout(self, refresh_token: str) -> None:
        try:
            payload = decode_token(refresh_token, expected_type="refresh")
        except AuthenticationError:
            return  # already invalid/expired: logout is a no-op, not an error
        await self._redis.delete(f"refresh:{payload['jti']}")

    async def _issue_tokens(self, user_id: uuid.UUID) -> TokenResponse:
        access_token = create_access_token(user_id)
        refresh_token = create_refresh_token(user_id)

        payload = decode_token(refresh_token, expected_type="refresh")
        await self._redis.set(
            f"refresh:{payload['jti']}",
            str(user_id),
            ex=timedelta(days=settings.refresh_token_expire_days),
        )
        return TokenResponse(access_token=access_token, refresh_token=refresh_token)
