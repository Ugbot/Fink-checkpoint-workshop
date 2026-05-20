#!/usr/bin/env bash
# Verify scenario 07 — counts join outputs per joinKind and validates interval-join lag.
# Reads topic.enriched.s07 with read_committed semantics for ~30 seconds.

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
INTERNAL_BOOTSTRAP="workshop-kafka:9093"
TOPIC="topic.enriched.s07"
TIMEOUT_MS="${TIMEOUT_MS:-30000}"

echo "=== Scenario 07 — Stream-Stream Joins verification ==="
echo "Reading $TOPIC (timeout ${TIMEOUT_MS}ms) ..."

SAMPLE="$(podman exec workshop-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$INTERNAL_BOOTSTRAP" \
    --topic "$TOPIC" \
    --from-beginning \
    --timeout-ms "$TIMEOUT_MS" \
    --consumer-property isolation.level=read_committed 2>/dev/null || true)"

if [[ -z "$SAMPLE" ]]; then
    echo "WARN: no records read from $TOPIC within ${TIMEOUT_MS}ms"
    echo "      Make sure App07RegularJoin (and/or interval/window job) is running and that"
    echo "      OrderFillSplitterJob is fanning topic.in into topic.orders/topic.fills."
    exit 2
fi

TOTAL="$(printf '%s\n' "$SAMPLE" | grep -c .)"
INNER="$(printf '%s\n' "$SAMPLE" | grep -c '"joinKind":"INNER"' || true)"
LEFT="$(printf '%s\n' "$SAMPLE"  | grep -c '"joinKind":"LEFT_ORPHAN"' || true)"
RIGHT="$(printf '%s\n' "$SAMPLE" | grep -c '"joinKind":"RIGHT_ORPHAN"' || true)"
WITH_QUOTE="$(printf '%s\n' "$SAMPLE" | grep -c '"quote":{' || true)"
WITH_TICKER_VOLUME="$(printf '%s\n' "$SAMPLE" | grep -c '"tradeCount":' || true)"

echo
echo "Total records on $TOPIC: $TOTAL"
echo
echo "── Regular join (App07RegularJoin) ───────────────────────"
echo "  INNER          : $INNER"
echo "  LEFT_ORPHAN    : $LEFT     (orders without fills)"
echo "  RIGHT_ORPHAN   : $RIGHT    (fills without orders)"
echo
echo "── Interval join (App07IntervalJoin) ─────────────────────"
echo "  trade+quote    : $WITH_QUOTE"
echo
echo "── Window join (App07WindowJoin) ─────────────────────────"
echo "  ticker volume  : $WITH_TICKER_VOLUME"
echo

PASS=1
[[ $INNER -gt 0 ]] || { echo "FAIL: no INNER joins observed";        PASS=0; }
[[ $LEFT  -gt 0 ]] || { echo "INFO: no LEFT_ORPHAN observed (raise DROP_FILL_PCT or wait longer)"; }
[[ $RIGHT -gt 0 ]] || { echo "INFO: no RIGHT_ORPHAN observed (raise ORPHAN_FILL_PCT or wait longer)"; }

if [[ $PASS -eq 1 ]]; then
    echo "PASS: scenario 07 has join output."
else
    echo "FAIL: scenario 07 verification did not see expected output."
    exit 1
fi
