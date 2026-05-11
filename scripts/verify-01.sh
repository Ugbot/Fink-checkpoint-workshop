#!/usr/bin/env bash
# Scenario 01 — count duplicate eventIds in topic.mid
# Expected: > 0 duplicates after App1 crash+restart with AT_LEAST_ONCE

set -euo pipefail
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
TIMEOUT_MS=10000

echo "=== Scenario 01: Baseline AT_LEAST_ONCE — duplicate check in topic.mid ==="
echo ""

TMPFILE=$(mktemp)

podman exec workshop-kafka kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --topic topic.mid \
    --from-beginning \
    --property print.key=true \
    --property key.separator="=" \
    --timeout-ms "$TIMEOUT_MS" 2>/dev/null > "$TMPFILE" || true

TOTAL=$(wc -l < "$TMPFILE" | tr -d ' ')
DUPLICATES=$(awk -F'=' '{print $1}' "$TMPFILE" | sort | uniq -d | wc -l | tr -d ' ')

echo "Total messages in topic.mid : $TOTAL"
echo "Duplicate eventIds          : $DUPLICATES"

if [ "$DUPLICATES" -gt 0 ]; then
  echo ""
  echo "RESULT: DUPLICATES PRESENT (expected for AT_LEAST_ONCE after crash)"
  echo "Sample duplicate keys:"
  awk -F'=' '{print $1}' "$TMPFILE" | sort | uniq -d | head -5
else
  echo ""
  echo "RESULT: No duplicates found (crash may not have fired yet, or topic was empty)"
fi

rm -f "$TMPFILE"
