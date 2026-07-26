"""Domain-level exceptions shared by both services.

Each service translates these into its own transport errors at the edge:
the API service maps them to HTTP status codes (see app/api/deps.py's
exception handlers), and the Inventory service maps them to gRPC status
codes (see grpc_handlers/inventory_servicer.py). Keeping the exceptions
themselves transport-agnostic means the business logic in between never
has to think about HTTP or gRPC.
"""


class DomainError(Exception):
    """Base class for all domain-level errors across services."""


class NotFoundError(DomainError):
    """The requested resource does not exist."""


class ConflictError(DomainError):
    """An optimistic-lock version conflict or other write conflict."""


class CapacityExceededError(DomainError):
    """A reservation would overbook an event's remaining capacity."""


class ValidationError(DomainError):
    """A business-rule validation failure (distinct from request schema
    validation, which FastAPI/Pydantic already handles at the edge)."""


class AuthenticationError(DomainError):
    """Invalid credentials or an invalid/expired token."""


class AuthorizationError(DomainError):
    """The authenticated principal is not allowed to do this."""
