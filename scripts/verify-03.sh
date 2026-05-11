#!/usr/bin/env bash
# Scenario 03 — check for duplicate rows in PostgreSQL processed_trades table

set -euo pipefail

echo "=== Scenario 03: External Sink Duplicates — PostgreSQL duplicate check ==="
echo ""

DUPLICATE_COUNT=$(podman exec workshop-postgres psql \
    -U workshop -d workshop -t -c \
    "SELECT COUNT(*) FROM duplicate_trades;" 2>/dev/null | tr -d ' ')

TOTAL_COUNT=$(podman exec workshop-postgres psql \
    -U workshop -d workshop -t -c \
    "SELECT COUNT(*) FROM processed_trades;" 2>/dev/null | tr -d ' ')

echo "Total rows in processed_trades : $TOTAL_COUNT"
echo "Rows with duplicate event_id   : $DUPLICATE_COUNT"

if [ "$DUPLICATE_COUNT" -gt 0 ]; then
  echo ""
  echo "RESULT: DUPLICATES PRESENT in Postgres (expected in SINK_MODE=buggy after crash)"
  echo "Sample duplicates:"
  podman exec workshop-postgres psql \
      -U workshop -d workshop -c \
      "SELECT event_id, cnt FROM duplicate_trades LIMIT 5;"
else
  echo ""
  echo "RESULT: No duplicates (either SINK_MODE=fixed / upsert is working, or no crash yet)"
fi
