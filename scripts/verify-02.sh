#!/usr/bin/env bash
# Scenario 02 — confirm zero duplicate eventIds in topic.mid with EXACTLY_ONCE

set -euo pipefail
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
TIMEOUT_MS=10000

echo "=== Scenario 02: EXACTLY_ONCE Kafka — duplicate check in topic.mid ==="
echo ""

TMPFILE=$(mktemp)

podman exec workshop-kafka kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --topic topic.mid \
    --from-beginning \
    --property print.key=true \
    --property key.separator="=" \
    --consumer-property isolation.level=read_committed \
    --timeout-ms "$TIMEOUT_MS" 2>/dev/null > "$TMPFILE" || true

TOTAL=$(wc -l < "$TMPFILE" | tr -d ' ')
DUPLICATES=$(awk -F'=' '{print $1}' "$TMPFILE" | sort | uniq -d | wc -l | tr -d ' ')

echo "Total committed messages in topic.mid : $TOTAL"
echo "Duplicate eventIds                    : $DUPLICATES"

if [ "$DUPLICATES" -eq 0 ]; then
  echo ""
  echo "RESULT: NO DUPLICATES (exactly-once is working correctly)"
else
  echo ""
  echo "RESULT: DUPLICATES FOUND — check transactional sink configuration"
fi

rm -f "$TMPFILE"
