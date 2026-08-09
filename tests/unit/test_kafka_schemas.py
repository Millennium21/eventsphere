import json
import uuid

from services.shared.kafka.schemas import EventEnvelope, OrderCreatedPayload
from services.shared.kafka.topics import EventType


def test_event_envelope_wraps_payload_and_stamps_event_type():
    payload = OrderCreatedPayload(
        order_id=uuid.uuid4(),
        event_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        quantity=3,
        total_price_cents=4500,
        reservation_id="abc-123",
    )
    envelope = EventEnvelope.wrap(EventType.ORDER_CREATED, payload)

    assert envelope.event_type == EventType.ORDER_CREATED.value
    assert envelope.payload["quantity"] == 3
    assert envelope.payload["total_price_cents"] == 4500


def test_envelope_round_trips_through_json_like_a_real_kafka_message():
    payload = OrderCreatedPayload(
        order_id=uuid.uuid4(),
        event_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        quantity=1,
        total_price_cents=1000,
        reservation_id="res-1",
    )
    envelope = EventEnvelope.wrap(EventType.ORDER_CREATED, payload)

    wire_bytes = envelope.model_dump_json().encode("utf-8")
    decoded = json.loads(wire_bytes.decode("utf-8"))

    assert decoded["event_type"] == EventType.ORDER_CREATED.value
    assert decoded["payload"]["reservation_id"] == "res-1"
    assert "occurred_at" in decoded
