#!/usr/bin/env bash
# Scenario 06 — run an ad-hoc SQL statement against the Flink SQL Gateway
# using nothing but curl + jq. Mirrors what docker/query-ui/server.js does.
#
# Usage:
#   ./scripts/query-rest.sh "SELECT COUNT(*) FROM paimon.workshop.trades"
#   ./scripts/query-rest.sh   # uses a default sample query
#
# Env:
#   GATEWAY (default http://localhost:18083)

set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:18083}"
SQL="${1:-SELECT COUNT(*) AS total_trades FROM paimon.workshop.trades}"

command -v jq >/dev/null || { echo "jq is required (brew install jq)"; exit 1; }

echo "Gateway:  $GATEWAY"
echo "SQL:      $SQL"
echo

# 1. open session
SESSION_BODY='{"properties":{"execution.runtime-mode":"batch"}}'
SESSION=$(curl -fsS -X POST -H "Content-Type: application/json" \
  -d "$SESSION_BODY" "$GATEWAY/v1/sessions" | jq -r .sessionHandle)
echo "session:  $SESSION"

cleanup() { curl -fsS -X DELETE "$GATEWAY/v1/sessions/$SESSION" >/dev/null || true; }
trap cleanup EXIT

# 2. submit statement
STMT_BODY=$(jq -nc --arg sql "$SQL" '{statement:$sql}')
OPER=$(curl -fsS -X POST -H "Content-Type: application/json" \
  -d "$STMT_BODY" "$GATEWAY/v1/sessions/$SESSION/statements" | jq -r .operationHandle)
echo "operation: $OPER"
echo

# 3. drain result pages until EOS
URL="$GATEWAY/v1/sessions/$SESSION/operations/$OPER/result/0"
COLS_PRINTED=0
DEADLINE=$(( $(date +%s) + 60 ))

while [[ -n "$URL" ]]; do
  if (( $(date +%s) > DEADLINE )); then
    echo "timeout after 60s"; exit 1
  fi
  PAGE=$(curl -fsS "$URL")
  TYPE=$(echo "$PAGE" | jq -r .resultType)

  if [[ "$TYPE" == "NOT_READY" ]]; then
    sleep 0.25
    continue
  fi

  if [[ $COLS_PRINTED -eq 0 ]]; then
    echo "$PAGE" | jq -r '.results.columns // [] | map(.name) | @tsv'
    COLS_PRINTED=1
  fi
  echo "$PAGE" | jq -r '.results.data // [] | map(select(.kind=="INSERT") | .fields | @tsv) | .[]'

  if [[ "$TYPE" == "EOS" ]]; then break; fi

  NEXT=$(echo "$PAGE" | jq -r '.nextResultUri // empty')
  if [[ -z "$NEXT" ]]; then
    sleep 0.25
    continue
  fi
  if [[ "$NEXT" == http* ]]; then URL="$NEXT"; else URL="$GATEWAY$NEXT"; fi
done
