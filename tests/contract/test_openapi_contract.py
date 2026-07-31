"""Guards against accidentally breaking the public API contract (routes
disappearing, response models losing fields) without needing a live
FastAPI process — just the schema FastAPI already generates.
"""

import httpx
import pytest

pytestmark = pytest.mark.contract

EXPECTED_PATHS = {
    "/api/v1/health",
    "/api/v1/health/ready",
    "/api/v1/auth/register",
    "/api/v1/auth/login",
    "/api/v1/auth/refresh",
    "/api/v1/auth/logout",
    "/api/v1/events",
    "/api/v1/events/{event_id}",
    "/api/v1/orders",
    "/api/v1/orders/{order_id}",
    "/api/v1/orders/{order_id}/cancel",
}


async def test_openapi_exposes_every_expected_path(client: httpx.AsyncClient):
    schema = (await client.get("/openapi.json")).json()
    assert EXPECTED_PATHS.issubset(set(schema["paths"].keys()))


async def test_login_uses_oauth2_password_flow_so_swagger_ui_authorize_button_works(client: httpx.AsyncClient):
    schema = (await client.get("/openapi.json")).json()
    security_schemes = schema["components"]["securitySchemes"]
    assert "OAuth2PasswordBearer" in security_schemes
    assert security_schemes["OAuth2PasswordBearer"]["flows"]["password"]["tokenUrl"] == "/api/v1/auth/login"


async def test_order_read_schema_has_the_fields_a_client_would_depend_on(client: httpx.AsyncClient):
    schema = (await client.get("/openapi.json")).json()
    order_schema = schema["components"]["schemas"]["OrderRead"]
    expected_fields = {
        "id",
        "event_id",
        "quantity",
        "unit_price_cents",
        "total_price_cents",
        "status",
        "reservation_id",
        "created_at",
        "updated_at",
    }
    assert expected_fields.issubset(set(order_schema["properties"].keys()))


async def test_book_tickets_endpoint_requires_authentication(client: httpx.AsyncClient):
    schema = (await client.get("/openapi.json")).json()
    book_operation = schema["paths"]["/api/v1/orders"]["post"]
    assert book_operation["security"], "POST /orders must declare a security requirement"
