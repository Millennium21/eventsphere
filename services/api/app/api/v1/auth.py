from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.security import OAuth2PasswordRequestForm

from services.api.app.api.deps import DbSession, RedisDep
from services.api.app.repositories.user_repository import UserRepository
from services.api.app.schemas.auth import RefreshRequest, RegisterRequest, TokenResponse
from services.api.app.schemas.user import UserRead
from services.api.app.services.auth_service import AuthService

router = APIRouter(prefix="/auth", tags=["auth"])


def _service(session: DbSession, redis: RedisDep) -> AuthService:
    return AuthService(UserRepository(session), redis)


@router.post("/register", response_model=UserRead, status_code=status.HTTP_201_CREATED)
async def register(data: RegisterRequest, session: DbSession, redis: RedisDep) -> UserRead:
    service = _service(session, redis)
    user = await service.register(email=data.email, password=data.password, full_name=data.full_name)
    return UserRead.model_validate(user)


@router.post("/login", response_model=TokenResponse)
async def login(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()], session: DbSession, redis: RedisDep
) -> TokenResponse:
    # `username` is the OAuth2 form's conventional field name; this API
    # treats it as the user's email so Swagger UI's built-in "Authorize"
    # button works out of the box alongside the JSON /register endpoint.
    return await _service(session, redis).login(email=form_data.username, password=form_data.password)


@router.post("/refresh", response_model=TokenResponse)
async def refresh(data: RefreshRequest, session: DbSession, redis: RedisDep) -> TokenResponse:
    return await _service(session, redis).refresh(data.refresh_token)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(data: RefreshRequest, session: DbSession, redis: RedisDep) -> None:
    await _service(session, redis).logout(data.refresh_token)
