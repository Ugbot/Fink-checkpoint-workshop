#!/usr/bin/env bash
# Scenario 06 — end-to-end sanity check.
#
# 1. Counts records currently visible on Kafka topic `trades.scenario06`
#    (read_committed, exactly-once, so this equals what the ingest job has
#    durably written).
# 2. Counts rows in the Paimon table via the SQL Gateway.
# 3. Asserts the Paimon count is close to the Kafka count (allowing for
#    the in-flight checkpoint window — Paimon only publishes snapshots
#    at checkpoint boundaries, which by default is every 10s).

set -euo pipefail
cd "$(dirname "$0")/.."

INTERNAL_BOOTSTRAP="workshop-kafka:9093"
TOPIC="trades.scenario06"
GATEWAY="${GATEWAY:-http://localhost:18083}"

echo "=== Scenario 06: Kafka -> Paimon ingest verification ==="
echo

echo "Kafka committed message count ($TOPIC):"
TMP=$(mktemp)
podman exec workshop-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$INTERNAL_BOOTSTRAP" \
    --topic "$TOPIC" \
    --from-beginning \
    --consumer-property isolation.level=read_committed \
    --timeout-ms 8000 2>/dev/null > "$TMP" || true
KAFKA_COUNT=$(wc -l < "$TMP" | tr -d ' ')
rm -f "$TMP"
echo "  $KAFKA_COUNT"
echo

echo "Paimon row count (via SQL Gateway $GATEWAY):"
PAIMON_OUT=$(./scripts/query-rest.sh \
  "SELECT COUNT(*) FROM paimon.workshop.trades" 2>&1 | tail -2 | tail -1)
echo "  $PAIMON_OUT"
PAIMON_COUNT=$(echo "$PAIMON_OUT" | awk '{print $1}')
echo

if ! [[ "$PAIMON_COUNT" =~ ^[0-9]+$ ]]; then
  echo "RESULT: could not parse Paimon count — check the gateway is up and the table exists"
  exit 1
fi

if [[ "$KAFKA_COUNT" -eq 0 ]]; then
  echo "RESULT: no Kafka messages yet — start the datagen + ingest jobs first"
  exit 1
fi

# Allow Paimon to be at most 'checkpoint interval worth' behind Kafka.
LAG=$(( KAFKA_COUNT - PAIMON_COUNT ))
ABS_LAG=${LAG#-}
echo "Kafka: $KAFKA_COUNT  Paimon: $PAIMON_COUNT  lag: $LAG"

if [[ "$ABS_LAG" -le $(( KAFKA_COUNT / 10 + 50 )) ]]; then
  echo "RESULT: PASS  (Paimon caught up within one checkpoint window)"
else
  echo "RESULT: lag larger than expected — is the PaimonIngestJob running and checkpointing?"
  exit 2
fi
