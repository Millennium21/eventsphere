from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer

from services.shared.kafka.topics import Topic

logger = structlog.get_logger(__name__)

Handler = Callable[[dict[str, Any]], Awaitable[None]]


class BaseKafkaConsumer:
    """Wraps AIOKafkaConsumer with manual offset commit.

    Offsets are only committed after the handler returns successfully, so
    a crash mid-processing simply redelivers the message on restart
    (at-least-once delivery). Handlers are written to be safe to run
    twice (checking current state before applying a change) rather than
    assuming exactly-once delivery, which is the standard, much simpler
    alternative to a fully transactional consume-process-produce pipeline.

    A handler that raises is logged and the message is skipped without
    committing (non-fatal partial failure — one bad message doesn't take
    the whole consumer down); a production system would route repeat
    failures to a dead-letter topic after N retries instead of retrying
    forever on every restart.
    """

    def __init__(
        self,
        *,
        topics: list[Topic],
        group_id: str,
        bootstrap_servers: str,
        handler: Handler,
    ) -> None:
        self._topics = [t.value for t in topics]
        self._group_id = group_id
        self._bootstrap_servers = bootstrap_servers
        self._handler = handler
        self._consumer: AIOKafkaConsumer | None = None
        self._stopping = asyncio.Event()

    async def run(self) -> None:
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await self._consumer.start()
        logger.info("kafka_consumer_started", topics=self._topics, group_id=self._group_id)
        try:
            async for message in self._consumer:
                if self._stopping.is_set():
                    break
                await self._process(message)
        finally:
            await self._consumer.stop()
            logger.info("kafka_consumer_stopped", group_id=self._group_id)

    async def _process(self, message: Any) -> None:
        try:
            await self._handler(message.value)
            await self._consumer.commit()
        except Exception:
            logger.exception(
                "kafka_message_handler_failed",
                topic=message.topic,
                offset=message.offset,
                group_id=self._group_id,
            )

    def stop(self) -> None:
        self._stopping.set()
