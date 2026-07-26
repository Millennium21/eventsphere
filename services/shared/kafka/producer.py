from __future__ import annotations

import structlog
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel

from services.shared.kafka.schemas import EventEnvelope
from services.shared.kafka.topics import Topic

logger = structlog.get_logger(__name__)


class KafkaEventProducer:
    """Thin async wrapper around aiokafka's producer.

    Idempotence is enabled (`enable_idempotence=True` + `acks="all"`) so a
    retried send can never duplicate a message on the broker — that's the
    producer half of "exactly-once basics". The consumer half is handling
    every message idempotently (see BaseKafkaConsumer): together these give
    "effectively-once" processing without needing a transactional/EOS
    pipeline, which would be considerable extra operational complexity for
    what this system needs.
    """

    def __init__(self, bootstrap_servers: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            enable_idempotence=True,
            acks="all",
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        )
        await self._producer.start()
        logger.info("kafka_producer_started", bootstrap_servers=self._bootstrap_servers)

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            logger.info("kafka_producer_stopped")

    async def publish(self, topic: Topic, key: str, payload: BaseModel) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaEventProducer.start() must be called before publish()")
        envelope = EventEnvelope.wrap(topic, payload)
        await self._producer.send_and_wait(topic.value, value=envelope.model_dump_json(), key=key)
        logger.info("event_published", topic=topic.value, event_id=str(envelope.event_id), key=key)
