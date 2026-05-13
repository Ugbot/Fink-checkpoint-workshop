#!/usr/bin/env bash
set -euo pipefail

# External bootstrap for host-side verification; internal bootstrap for in-container commands
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
INTERNAL_BOOTSTRAP="workshop-kafka:9093"
PARTITIONS=4
RF=1

echo "Creating Kafka topics on $BOOTSTRAP ..."

for TOPIC in "topic.in" "topic.mid" "topic.out" "trades.scenario06"; do
  if podman exec workshop-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$INTERNAL_BOOTSTRAP" \
      --list 2>/dev/null | grep -qx "$TOPIC"; then
    echo "  $TOPIC already exists — skipping"
  else
    podman exec workshop-kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "$INTERNAL_BOOTSTRAP" \
        --create \
        --topic "$TOPIC" \
        --partitions "$PARTITIONS" \
        --replication-factor "$RF" \
        --config min.insync.replicas=1
    echo "  Created $TOPIC ($PARTITIONS partitions, RF=$RF)"
  fi
done

echo "Done. Topics:"
podman exec workshop-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$INTERNAL_BOOTSTRAP" \
    --list
