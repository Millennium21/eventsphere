from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from redis.asyncio import Redis

logger = structlog.get_logger(__name__)

# Lua script for safe unlock: only delete the key if it still holds the
# token we set, so we never release a lock we no longer own (e.g. one
# that already expired and was re-acquired by someone else).
_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class LockAcquisitionError(Exception):
    pass


class RedisLock:
    """A single-node SET-NX/PX distributed lock.

    This is enough to cut down on wasted optimistic-lock retries when many
    requests target the same popular event concurrently. It is not used
    as the correctness guarantee against overbooking — that's the
    database's version column (see EventInventory). A stricter multi-node
    deployment wanting the lock itself to be the safety net (rather than
    a contention-reduction optimisation) would run Redlock across an odd
    number of independent Redis nodes instead of trusting a single one.
    """

    def __init__(
        self,
        redis: Redis,
        *,
        ttl_ms: int = 5000,
        retry_attempts: int = 10,
        retry_delay_seconds: float = 0.1,
    ) -> None:
        self._redis = redis
        self._ttl_ms = ttl_ms
        self._retry_attempts = retry_attempts
        self._retry_delay_seconds = retry_delay_seconds

    @asynccontextmanager
    async def acquire(self, resource: str) -> AsyncIterator[None]:
        token = str(uuid.uuid4())
        key = f"lock:{resource}"
        acquired = False

        for _ in range(self._retry_attempts):
            acquired = bool(await self._redis.set(key, token, nx=True, px=self._ttl_ms))
            if acquired:
                break
            await asyncio.sleep(self._retry_delay_seconds)

        if not acquired:
            raise LockAcquisitionError(f"Could not acquire lock for resource={resource!r}")

        try:
            yield
        finally:
            try:
                await self._redis.eval(_RELEASE_SCRIPT, 1, key, token)
            except Exception:
                logger.warning("lock_release_failed", resource=resource)
