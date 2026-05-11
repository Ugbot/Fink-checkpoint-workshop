#!/usr/bin/env bash
# Scenario 04 — compare topic.out message count between naive and dedup variants
# Run this script after each App2 variant to compare totals.

set -euo pipefail
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
TIMEOUT_MS=8000

echo "=== Scenario 04: Upstream True Duplicates — topic.out message count ==="
echo ""

TMPFILE=$(mktemp)

podman exec workshop-kafka kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --topic topic.out \
    --from-beginning \
    --timeout-ms "$TIMEOUT_MS" 2>/dev/null > "$TMPFILE" || true

TOTAL=$(wc -l < "$TMPFILE" | tr -d ' ')
echo "Total messages in topic.out: $TOTAL"
echo ""
echo "Compare:"
echo "  Naive (App2PipelineNaive)       — inflated by ~TOTAL_EVENTS/DUPLICATE_EVERY extra messages"
echo "  With dedup (App2PipelineWithDedup) — count equals TOTAL_EVENTS"
echo ""
echo "Check task manager logs for dropped duplicates:"
echo "  podman logs workshop-taskmanager 2>&1 | grep 'Dropping duplicate'"

rm -f "$TMPFILE"
