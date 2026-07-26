from enum import StrEnum


class Topic(StrEnum):
    """Every Kafka topic in the system, namespaced with `eventsphere.` so
    they're unambiguous in a shared cluster alongside other projects/teams.
    """

    EVENT_CREATED = "eventsphere.events.created"
    EVENT_UPDATED = "eventsphere.events.updated"
    ORDER_CREATED = "eventsphere.orders.created"
    PAYMENT_PROCESSED = "eventsphere.payments.processed"
    TICKET_ISSUED = "eventsphere.tickets.issued"
    ORDER_CANCELLED = "eventsphere.orders.cancelled"
    RESERVATION_EXPIRED = "eventsphere.reservations.expired"


ALL_TOPICS: tuple[Topic, ...] = tuple(Topic)

# Used by the local kafka-init container (infra/kafka/create_topics.sh) when
# creating topics for docker-compose. On AWS MSK, partitions would be sized
# per-topic based on target throughput and consumer parallelism, and
# replication factor would typically be 3 (spread across AZs) rather than 1.
DEFAULT_PARTITIONS = 3
LOCAL_REPLICATION_FACTOR = 1

# Consumer group ids, centralised here so a topic's set of consumer groups
# is discoverable in one place rather than scattered across worker files.
GROUP_INVENTORY_EVENT_SYNC = "inventory-event-sync"
GROUP_API_PAYMENT_WORKER = "api-payment-worker"
GROUP_API_ORDER_STATUS_WORKER = "api-order-status-worker"
GROUP_API_NOTIFICATION_WORKER = "api-notification-worker"
