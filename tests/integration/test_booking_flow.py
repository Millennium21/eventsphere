"""End-to-end API tests against a real Postgres + Redis + an in-process
gRPC Inventory server, with Kafka faked (see tests/conftest.py). These
exercise the same code paths a live docker-compose stack would, minus
the broker itself.
"""

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from services.shared.kafka.topics import EventType
from tests.conftest import FakeKafkaProducer, auth_headers_for

pytestmark = pytest.mark.integration


def _future_event_payload(**overrides):
    payload = {
        "title": "PyCon Keynote Night",
        "description": "An evening of talks.",
        "venue": "Barbican Centre",
        "starts_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
        "total_seats": 5,
        "price_cents": 2500,
    }
    payload.update(overrides)
    return payload


async def test_register_and_login_returns_working_tokens(client: httpx.AsyncClient):
    register_resp = await client.post(
        "/api/v1/auth/register",
        json={
            "email": "new.user@example.com",
            "password": "correct-horse-battery-staple",
            "full_name": "New User",
        },
    )
    assert register_resp.status_code == 201
    assert register_resp.json()["email"] == "new.user@example.com"

    login_resp = await client.post(
        "/api/v1/auth/login",
        data={"username": "new.user@example.com", "password": "correct-horse-battery-staple"},
    )
    assert login_resp.status_code == 200
    body = login_resp.json()
    assert body["token_type"] == "bearer"
    assert "access_token" in body and "refresh_token" in body


async def test_registering_the_same_email_twice_is_rejected(client: httpx.AsyncClient):
    payload = {"email": "dupe@example.com", "password": "correct-horse-battery-staple", "full_name": "Dupe"}
    first = await client.post("/api/v1/auth/register", json=payload)
    second = await client.post("/api/v1/auth/register", json=payload)
    assert first.status_code == 201
    assert second.status_code == 422


async def test_attendee_cannot_create_event_but_organizer_can(
    client: httpx.AsyncClient, test_user, organizer_user
):
    attendee_resp = await client.post(
        "/api/v1/events", json=_future_event_payload(), headers=auth_headers_for(client, test_user)
    )
    assert attendee_resp.status_code == 403

    organizer_resp = await client.post(
        "/api/v1/events", json=_future_event_payload(), headers=auth_headers_for(client, organizer_user)
    )
    assert organizer_resp.status_code == 201
    assert organizer_resp.json()["total_seats"] == 5


async def test_search_and_get_event_are_public(client: httpx.AsyncClient, organizer_user):
    create_resp = await client.post(
        "/api/v1/events",
        json=_future_event_payload(title="Public Test Gig"),
        headers=auth_headers_for(client, organizer_user),
    )
    event_id = create_resp.json()["id"]

    search_resp = await client.get("/api/v1/events", params={"q": "Public Test"})
    assert search_resp.status_code == 200
    assert search_resp.json()["total"] >= 1

    get_resp = await client.get(f"/api/v1/events/{event_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == event_id


async def test_booking_reduces_capacity_and_publishes_order_created(
    client: httpx.AsyncClient, test_user, organizer_user, fake_kafka_producer: FakeKafkaProducer
):
    event = (
        await client.post(
            "/api/v1/events",
            json=_future_event_payload(total_seats=5),
            headers=auth_headers_for(client, organizer_user),
        )
    ).json()

    order_resp = await client.post(
        "/api/v1/orders",
        json={"event_id": event["id"], "quantity": 2},
        headers=auth_headers_for(client, test_user),
    )
    assert order_resp.status_code == 201
    order = order_resp.json()
    assert order["quantity"] == 2
    assert order["total_price_cents"] == 5000
    assert order["status"] == "pending"

    order_created_events = fake_kafka_producer.events_of_type(EventType.ORDER_CREATED)
    assert len(order_created_events) == 1
    assert order_created_events[0].payload["quantity"] == 2

    # A third booking for 4 more seats should be rejected: 5 - 2 = 3 left.
    over_resp = await client.post(
        "/api/v1/orders",
        json={"event_id": event["id"], "quantity": 4},
        headers=auth_headers_for(client, test_user),
    )
    assert over_resp.status_code == 409


async def test_booking_with_same_idempotency_key_returns_the_same_order(
    client: httpx.AsyncClient, test_user, organizer_user, fake_kafka_producer: FakeKafkaProducer
):
    event = (
        await client.post(
            "/api/v1/events",
            json=_future_event_payload(total_seats=5),
            headers=auth_headers_for(client, organizer_user),
        )
    ).json()

    headers = {**auth_headers_for(client, test_user), "Idempotency-Key": "retry-key-123"}
    first = await client.post("/api/v1/orders", json={"event_id": event["id"], "quantity": 1}, headers=headers)
    second = await client.post("/api/v1/orders", json={"event_id": event["id"], "quantity": 1}, headers=headers)

    assert first.status_code == 201
    assert second.status_code == 201
    assert first.json()["id"] == second.json()["id"]
    # Only one reservation/order should actually have been created.
    assert len(fake_kafka_producer.events_of_type(EventType.ORDER_CREATED)) == 1


async def test_cancel_order_releases_seats_and_publishes_cancelled(
    client: httpx.AsyncClient, test_user, organizer_user, fake_kafka_producer: FakeKafkaProducer
):
    event = (
        await client.post(
            "/api/v1/events",
            json=_future_event_payload(total_seats=2),
            headers=auth_headers_for(client, organizer_user),
        )
    ).json()
    headers = auth_headers_for(client, test_user)

    order = (
        await client.post("/api/v1/orders", json={"event_id": event["id"], "quantity": 2}, headers=headers)
    ).json()

    # No seats left now.
    blocked = await client.post("/api/v1/orders", json={"event_id": event["id"], "quantity": 1}, headers=headers)
    assert blocked.status_code == 409

    cancel_resp = await client.post(f"/api/v1/orders/{order['id']}/cancel", headers=headers)
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["status"] == "cancelled"
    assert len(fake_kafka_producer.events_of_type(EventType.ORDER_CANCELLED)) == 1

    # Seats are back: booking 2 again should now succeed.
    rebooked = await client.post("/api/v1/orders", json={"event_id": event["id"], "quantity": 2}, headers=headers)
    assert rebooked.status_code == 201


async def test_list_orders_only_returns_the_current_users_orders(
    client: httpx.AsyncClient, test_user, organizer_user
):
    event = (
        await client.post(
            "/api/v1/events",
            json=_future_event_payload(total_seats=5),
            headers=auth_headers_for(client, organizer_user),
        )
    ).json()
    await client.post(
        "/api/v1/orders",
        json={"event_id": event["id"], "quantity": 1},
        headers=auth_headers_for(client, test_user),
    )

    my_orders = (await client.get("/api/v1/orders", headers=auth_headers_for(client, test_user))).json()
    other_users_orders = (
        await client.get("/api/v1/orders", headers=auth_headers_for(client, organizer_user))
    ).json()

    assert my_orders["total"] == 1
    assert other_users_orders["total"] == 0


async def test_unauthenticated_booking_is_rejected(client: httpx.AsyncClient, organizer_user):
    event = (
        await client.post(
            "/api/v1/events", json=_future_event_payload(), headers=auth_headers_for(client, organizer_user)
        )
    ).json()
    resp = await client.post("/api/v1/orders", json={"event_id": event["id"], "quantity": 1})
    assert resp.status_code == 401
