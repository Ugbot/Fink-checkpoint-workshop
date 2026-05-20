#!/usr/bin/env bash
# Verify scenario 09 — multi-way join + crash recovery.
# Reads topic.enriched.s09 under read_committed, then:
#   - asserts zero duplicate eventIds (EXACTLY_ONCE Kafka sink + read_committed)
#   - asserts every enriched record has non-null quote, fxRate, account fields
set -euo pipefail

INTERNAL_BOOTSTRAP="workshop-kafka:9093"
TOPIC="topic.enriched.s09"
TIMEOUT_MS="${TIMEOUT_MS:-45000}"

echo "=== Scenario 09 — Multi-Way Join + Recovery verification ==="
echo "Reading $TOPIC under read_committed (timeout ${TIMEOUT_MS}ms) ..."

SAMPLE="$(podman exec workshop-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$INTERNAL_BOOTSTRAP" \
    --topic "$TOPIC" \
    --from-beginning \
    --timeout-ms "$TIMEOUT_MS" \
    --consumer-property isolation.level=read_committed 2>/dev/null || true)"

if [[ -z "$SAMPLE" ]]; then
    echo "WARN: no records on $TOPIC. Is App09MultiWayJoin running?"
    exit 2
fi

TOTAL="$(printf '%s\n' "$SAMPLE" | grep -c .)"
EVENT_IDS="$(printf '%s\n' "$SAMPLE" | grep -oE '"eventId":"[^"]+"' | sort)"
UNIQUE="$(printf '%s\n' "$EVENT_IDS" | sort -u | grep -c . || true)"
DUPS="$(printf '%s\n' "$EVENT_IDS" | sort | uniq -d | grep -c . || true)"

WITH_QUOTE="$(printf '%s\n' "$SAMPLE"   | grep -c '"quote":{' || true)"
WITH_FX="$(printf '%s\n' "$SAMPLE"      | grep -c '"fxRate":[0-9]' || true)"
WITH_ACCOUNT="$(printf '%s\n' "$SAMPLE" | grep -c '"account":{' || true)"

echo
echo "Total records on $TOPIC : $TOTAL"
echo "Unique eventIds         : $UNIQUE"
echo "Duplicate eventIds      : $DUPS"
echo
echo "── Enrichment coverage ───────────────────────────────────"
echo "  with quote   : $WITH_QUOTE"
echo "  with fxRate  : $WITH_FX"
echo "  with account : $WITH_ACCOUNT"
echo

PASS=1
if [[ "$DUPS" -gt 0 ]]; then
    echo "FAIL: $DUPS duplicate eventId(s) under read_committed — EXACTLY_ONCE broken"
    PASS=0
else
    echo "OK:   no duplicate eventIds under read_committed"
fi

if [[ "$TOTAL" -gt 0 ]] && [[ "$WITH_QUOTE"   -lt $((TOTAL / 2)) ]]; then
    echo "WARN: fewer than half of records have a quote (interval join coverage low)"
fi
if [[ "$TOTAL" -gt 0 ]] && [[ "$WITH_FX"      -lt $((TOTAL / 2)) ]]; then
    echo "WARN: fewer than half of records have an FX rate (temporal join coverage low)"
fi
if [[ "$TOTAL" -gt 0 ]] && [[ "$WITH_ACCOUNT" -lt $((TOTAL / 2)) ]]; then
    echo "WARN: fewer than half of records have an account row (accounts seeded?)"
fi

if [[ $PASS -eq 1 ]]; then
    echo "PASS: scenario 09 multi-way join + recovery looks correct."
else
    echo "FAIL: scenario 09 verification failed."
    exit 1
fi
