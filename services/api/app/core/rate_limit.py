from __future__ import annotations

import time

import structlog
from redis.asyncio import Redis
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from services.api.app.core.config import settings

logger = structlog.get_logger(__name__)

_EXEMPT_PATHS = {
    f"{settings.api_prefix}/health",
    f"{settings.api_prefix}/health/ready",
    "/docs",
    "/openapi.json",
    "/redoc",
}


class RedisRateLimitMiddleware(BaseHTTPMiddleware):
    """Fixed-window rate limiter keyed by client IP.

    One INCR + one EXPIRE per request, windows aligned to the wall clock
    minute. Simple and cheap, at the cost of allowing up to ~2x the
    stated limit right at a window boundary (a burst at :59 followed by
    another at :00). A sliding-window-log or token-bucket algorithm
    avoids that edge effect at the cost of more Redis state per key —
    a reasonable upgrade path called out in the README rather than
    implemented here, since fixed-window is enough to demonstrate (and
    actually enforce) the mechanism end-to-end.
    """

    def __init__(self, app, *, limit_per_minute: int) -> None:
        super().__init__(app)
        self._limit = limit_per_minute

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path in _EXEMPT_PATHS:
            return await call_next(request)

        redis: Redis = request.app.state.redis
        identifier = request.client.host if request.client else "unknown"
        window = int(time.time() // 60)
        key = f"ratelimit:{identifier}:{window}"

        current = await redis.incr(key)
        if current == 1:
            await redis.expire(key, 60)

        if current > self._limit:
            logger.warning("rate_limit_exceeded", identifier=identifier)
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded. Try again shortly."},
                headers={"Retry-After": "60"},
            )

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(self._limit)
        response.headers["X-RateLimit-Remaining"] = str(max(0, self._limit - current))
        return response
