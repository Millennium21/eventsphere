#!/usr/bin/env bash
# Run once by the short-lived `kafka-init` compose service. Waits for the
# broker, then creates every topic with the partition/replication
# defaults from services/shared/kafka/topics.py (kept here as plain
# constants since this script runs in the Kafka image, not the Python one).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
PARTITIONS="${DEFAULT_PARTITIONS:-3}"
REPLICATION="${LOCAL_REPLICATION_FACTOR:-1}"

TOPICS=(
  "eventsphere.events.created"
  "eventsphere.events.updated"
  "eventsphere.orders.created"
  "eventsphere.payments.processed"
  "eventsphere.tickets.issued"
  "eventsphere.orders.cancelled"
  "eventsphere.reservations.expired"
)

echo "Waiting for Kafka at ${BOOTSTRAP}..."
until kafka-topics --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; do
  sleep 2
done

for topic in "${TOPICS[@]}"; do
  echo "Creating topic: ${topic} (partitions=${PARTITIONS}, replication=${REPLICATION})"
  kafka-topics --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
    --topic "${topic}" --partitions "${PARTITIONS}" --replication-factor "${REPLICATION}"
done

echo "All topics created:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list
