from enum import StrEnum


class EventType(StrEnum):
    """Every distinct kind of fact in the system. Consumers match on
    EventType (see each worker's `match event_type:` block), never on
    the physical Kafka topic -- see Topic and TOPIC_FOR_EVENT_TYPE below.
    That indirection is what let this go from 7 physical topics down to
    4 (to fit inside a 5-topic free-tier ceiling) by re-homing several
    EventTypes onto a shared Topic, without touching any consumer's dispatch
    logic: every consumer that read events off two separate topics already handled
    both event kinds via the same handler and the same match statement,
    so only the subscription list changes, not the handling code.
    """

    EVENT_CREATED = "eventsphere.events.created"
    EVENT_UPDATED = "eventsphere.events.updated"
    ORDER_CREATED = "eventsphere.orders.created"
    PAYMENT_PROCESSED = "eventsphere.payments.processed"
    TICKET_ISSUED = "eventsphere.tickets.issued"
    ORDER_CANCELLED = "eventsphere.orders.cancelled"
    RESERVATION_EXPIRED = "eventsphere.reservations.expired"


class Topic(StrEnum):
    """Physical Kafka topics. Several EventTypes can share one Topic --
    see TOPIC_FOR_EVENT_TYPE. Four topics total: comfortably under a
    5-topic free-tier ceiling (e.g. Aiven's free Kafka tier) with one
    to spare, rather than exactly at the limit.
    """

    EVENTS_LIFECYCLE = "eventsphere.events.lifecycle"
    ORDERS_CREATED = "eventsphere.orders.created"
    ORDERS_STATUS_CHANGED = "eventsphere.orders.status-changed"
    ORDERS_NOTIFICATIONS = "eventsphere.orders.notifications"


# Which physical topic each EventType is actually published to. Grouped
# by existing consumer: everything a single worker already handled via
# one match statement across multiple topics now lives on one topic --
# see event_sync_consumer.py, order_status_worker.py, and
# notification_worker.py, none of which needed a dispatch-logic change
# for this, only a shorter `topics=[...]` subscription list.
TOPIC_FOR_EVENT_TYPE: dict[EventType, Topic] = {
    EventType.EVENT_CREATED: Topic.EVENTS_LIFECYCLE,
    EventType.EVENT_UPDATED: Topic.EVENTS_LIFECYCLE,
    EventType.ORDER_CREATED: Topic.ORDERS_CREATED,
    EventType.PAYMENT_PROCESSED: Topic.ORDERS_STATUS_CHANGED,
    EventType.RESERVATION_EXPIRED: Topic.ORDERS_STATUS_CHANGED,
    EventType.TICKET_ISSUED: Topic.ORDERS_NOTIFICATIONS,
    EventType.ORDER_CANCELLED: Topic.ORDERS_NOTIFICATIONS,
}

ALL_TOPICS: tuple[Topic, ...] = tuple(Topic)

# Used by the local kafka-init container (infra/kafka/create_topics.sh) when
# creating topics for docker-compose, and worth setting explicitly (rather
# than relying on auto-create) against any managed Kafka too. On AWS MSK,
# partitions would be sized per-topic based on target throughput and
# consumer parallelism, and replication factor would typically be 3
# (spread across AZs) rather than 1.
DEFAULT_PARTITIONS = 3
LOCAL_REPLICATION_FACTOR = 1

# Consumer group ids, centralised here so a topic's set of consumer groups
# is discoverable in one place rather than scattered across worker files.
GROUP_INVENTORY_EVENT_SYNC = "inventory-event-sync"
GROUP_API_PAYMENT_WORKER = "api-payment-worker"
GROUP_API_ORDER_STATUS_WORKER = "api-order-status-worker"
GROUP_API_NOTIFICATION_WORKER = "api-notification-worker"
