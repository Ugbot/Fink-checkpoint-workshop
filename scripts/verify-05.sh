#!/usr/bin/env bash
# Scenario 05 — show consumer group lag to demonstrate replay after rescale/savepoint restore

set -euo pipefail
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"

echo "=== Scenario 05: Rescaling / Replay Pressure ==="
echo ""

echo "--- Consumer group offsets for s05-app1 (reads topic.in) ---"
podman exec workshop-kafka kafka-consumer-groups.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --describe \
    --group s05-app1 2>/dev/null || echo "(group not found — start App1 first)"

echo ""
echo "--- Consumer group offsets for s05-app2 (reads topic.mid) ---"
podman exec workshop-kafka kafka-consumer-groups.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --describe \
    --group s05-app2 2>/dev/null || echo "(group not found — start App2 first)"

echo ""
echo "After restoring from a lagging savepoint, the LAG column above shows"
echo "how many records will be replayed by each consumer group."
echo ""
echo "For Postgres duplicate check after replay (if scenario-03 external sink was used):"
echo "  ./scripts/verify-03.sh"
