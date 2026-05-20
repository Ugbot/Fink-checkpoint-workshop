#!/usr/bin/env bash
# Verify scenario 08 — lookup and temporal join outputs.
set -euo pipefail

INTERNAL_BOOTSTRAP="workshop-kafka:9093"
TOPIC="topic.enriched.s08"
TIMEOUT_MS="${TIMEOUT_MS:-30000}"

echo "=== Scenario 08 — Stream-Table Joins verification ==="
echo "Reading $TOPIC (timeout ${TIMEOUT_MS}ms) ..."

SAMPLE="$(podman exec workshop-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$INTERNAL_BOOTSTRAP" \
    --topic "$TOPIC" \
    --from-beginning \
    --timeout-ms "$TIMEOUT_MS" \
    --consumer-property isolation.level=read_committed 2>/dev/null || true)"

if [[ -z "$SAMPLE" ]]; then
    echo "WARN: no records on $TOPIC. Are App08LookupJoin / App08TemporalJoin running?"
    exit 2
fi

TOTAL="$(printf '%s\n' "$SAMPLE" | grep -c .)"
LOOKUP_HIT="$(printf '%s\n' "$SAMPLE" | grep -c '"cacheHit":' || true)"
LOOKUP_OK="$(printf '%s\n' "$SAMPLE"  | grep -c '"account":{' || true)"
LOOKUP_NULL="$(printf '%s\n' "$SAMPLE" | grep -c '"account":null' || true)"
TEMP_OK="$(printf '%s\n' "$SAMPLE"   | grep -c '"fxRate":[0-9]' || true)"
TEMP_NULL="$(printf '%s\n' "$SAMPLE" | grep -c '"fxRate":null' || true)"

echo
echo "Total records on $TOPIC: $TOTAL"
echo
echo "── Lookup join (App08LookupJoin) ─────────────────────────"
echo "  with account     : $LOOKUP_OK"
echo "  null account     : $LOOKUP_NULL"
echo "  total lookups    : $LOOKUP_HIT"
echo
echo "── Temporal join (App08TemporalJoin) ─────────────────────"
echo "  with FX rate     : $TEMP_OK"
echo "  null FX rate     : $TEMP_NULL"
echo

PASS=1
[[ $LOOKUP_HIT -gt 0 ]] || { echo "INFO: no lookup-join records observed yet"; }
[[ $TEMP_OK    -gt 0 ]] || { echo "INFO: no temporal-join records observed yet (need FX rates flowing first)"; }
[[ $TOTAL      -gt 0 ]] || PASS=0

if [[ $PASS -eq 1 ]]; then
    echo "PASS: scenario 08 has join output."
else
    echo "FAIL: no join output observed."
    exit 1
fi
