#!/usr/bin/env bash
set -euo pipefail

# External bootstrap for host-side verification; internal bootstrap for in-container commands
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
INTERNAL_BOOTSTRAP="workshop-kafka:9093"
PARTITIONS=4
RF=1

echo "Creating Kafka topics on $BOOTSTRAP ..."

TOPICS=(
  # Existing scenarios 01-06
  "topic.in"
  "topic.mid"
  "topic.out"
  "trades.scenario06"
  # Scenarios 07-09 (joins)
  "topic.quotes"
  "topic.fxrates:1"           # single partition: preserve global FX ordering for temporal join
  "topic.orders"
  "topic.fills"
  "topic.enriched.s07"
  "topic.enriched.s08"
  "topic.enriched.s09"
)

for ENTRY in "${TOPICS[@]}"; do
  TOPIC="${ENTRY%%:*}"
  OVERRIDE_PARTS=""
  if [[ "$ENTRY" == *:* ]]; then
    OVERRIDE_PARTS="${ENTRY##*:}"
  fi
  PARTS="${OVERRIDE_PARTS:-$PARTITIONS}"
  if podman exec workshop-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$INTERNAL_BOOTSTRAP" \
      --list 2>/dev/null | grep -qx "$TOPIC"; then
    echo "  $TOPIC already exists — skipping"
  else
    podman exec workshop-kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "$INTERNAL_BOOTSTRAP" \
        --create \
        --topic "$TOPIC" \
        --partitions "$PARTS" \
        --replication-factor "$RF" \
        --config min.insync.replicas=1
    echo "  Created $TOPIC ($PARTS partitions, RF=$RF)"
  fi
done

echo "Done. Topics:"
podman exec workshop-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$INTERNAL_BOOTSTRAP" \
    --list
