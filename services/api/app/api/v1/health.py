from __future__ import annotations

import structlog
from fastapi import APIRouter, Response, status
from sqlalchemy import text

from services.api.app.api.deps import DbSession, InventoryClientDep, RedisDep

logger = structlog.get_logger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health")
async def liveness() -> dict[str, str]:
    """Liveness: the process is up and serving. Kubernetes restarts the
    pod if this stops responding — it deliberately does NOT check
    downstream dependencies (a slow Postgres shouldn't cause a restart
    loop, only a readiness failure)."""
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(
    session: DbSession, redis: RedisDep, inventory: InventoryClientDep, response: Response
) -> dict:
    """Readiness: safe to receive traffic. Kubernetes removes the pod
    from the Service's endpoints (but does not restart it) while this
    reports unhealthy."""
    checks: dict[str, str] = {}
    healthy = True

    try:
        await session.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as exc:
        checks["database"] = f"error: {exc}"
        healthy = False

    try:
        await redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"
        healthy = False

    try:
        state = inventory.stub  # raises RuntimeError if channel never started
        checks["inventory_grpc"] = "ok" if state is not None else "not started"
    except Exception as exc:
        checks["inventory_grpc"] = f"error: {exc}"
        healthy = False

    response.status_code = status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ok" if healthy else "unhealthy", "checks": checks}
