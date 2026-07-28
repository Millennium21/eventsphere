from __future__ import annotations

import time
import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = structlog.get_logger(__name__)


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Binds a request id + basic request metadata to structlog's
    contextvars for the duration of the request, so every log line
    emitted anywhere while handling it (routers, services, repositories)
    is automatically tagged — no need to thread a logger/request id
    through every function signature.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        structlog.contextvars.bind_contextvars(request_id=request_id, path=request.url.path, method=request.method)

        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            logger.exception("unhandled_exception")
            raise
        duration_ms = round((time.perf_counter() - start) * 1000, 2)

        logger.info("request_completed", status_code=response.status_code, duration_ms=duration_ms)
        response.headers["X-Request-ID"] = request_id
        structlog.contextvars.unbind_contextvars("request_id", "path", "method")
        return response
