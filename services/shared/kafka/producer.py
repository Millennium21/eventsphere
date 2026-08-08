from __future__ import annotations

import ssl

import structlog
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel

from services.shared.kafka.schemas import EventEnvelope
from services.shared.kafka.topics import TOPIC_FOR_EVENT_TYPE, EventType

logger = structlog.get_logger(__name__)


class KafkaEventProducer:
    """Thin async wrapper around aiokafka's producer.

    Idempotence is enabled (`enable_idempotence=True` + `acks="all"`) so a
    retried send can never duplicate a message on the broker - that's the
    producer half of "exactly-once basics". The consumer half is handling
    every message idempotently (see BaseKafkaConsumer): together these give
    "effectively-once" processing without needing a transactional/EOS
    pipeline, which would be considerable extra operational complexity for
    what this system needs.

    security_protocol defaults to PLAINTEXT, matching the local
    docker-compose Kafka broker (no auth needed inside a private Docker
    network). Pass security_protocol="SASL_SSL" plus the sasl_* args to
    talk to a managed broker that requires auth (e.g. Aiven's free Kafka
    tier).
    """

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._security_protocol = security_protocol
        self._sasl_mechanism = sasl_mechanism
        self._sasl_username = sasl_username
        self._sasl_password = sasl_password
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        # A default SSL context (trusting the system's normal CA bundle)
        # is enough for a broker with a publicly-trusted certificate --
        # Aiven's free tier uses Let's Encrypt certs by default, so no
        # custom CA file is needed here.
        ssl_context = ssl.create_default_context() if self._security_protocol == "SASL_SSL" else None

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            security_protocol=self._security_protocol,
            sasl_mechanism=self._sasl_mechanism,
            sasl_plain_username=self._sasl_username,
            sasl_plain_password=self._sasl_password,
            ssl_context=ssl_context,
            enable_idempotence=True,
            acks="all",
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        )
        await self._producer.start()
        logger.info(
            "kafka_producer_started",
            bootstrap_servers=self._bootstrap_servers,
            security_protocol=self._security_protocol,
        )

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            logger.info("kafka_producer_stopped")

    async def publish(self, event_type: EventType, key: str, payload: BaseModel) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaEventProducer.start() must be called before publish()")
        topic = TOPIC_FOR_EVENT_TYPE[event_type]
        envelope = EventEnvelope.wrap(event_type, payload)
        await self._producer.send_and_wait(topic.value, value=envelope.model_dump_json(), key=key)
        logger.info(
            "event_published",
            topic=topic.value,
            event_type=event_type.value,
            event_id=str(envelope.event_id),
            key=key,
        )
