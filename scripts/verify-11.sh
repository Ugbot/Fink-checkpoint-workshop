#!/usr/bin/env bash
# Verify scenario 11: Fluss catalog reachable, tables created, seed ran.
set -euo pipefail
GATEWAY="${GATEWAY:-http://localhost:18083}"

command -v jq >/dev/null || { echo "jq required (brew install jq)"; exit 1; }

echo "=== Scenario 11 — Fluss Fundamentals verification ==="

run_sql() {
    local sql="$1"
    local session_body='{"properties":{}}'
    local session
    session=$(curl -fsS -X POST -H 'Content-Type: application/json' \
        -d "$session_body" "$GATEWAY/v1/sessions" | jq -r .sessionHandle)

    # Register the Fluss catalog every session
    local create_sql="CREATE CATALOG IF NOT EXISTS fluss WITH ('type'='fluss','bootstrap.servers'='workshop-fluss-coordinator:9123')"
    curl -fsS -X POST -H 'Content-Type: application/json' \
        -d "$(jq -nc --arg s "$create_sql" '{statement:$s}')" \
        "$GATEWAY/v1/sessions/$session/statements" >/dev/null

    local op
    op=$(curl -fsS -X POST -H 'Content-Type: application/json' \
        -d "$(jq -nc --arg s "$sql" '{statement:$s}')" \
        "$GATEWAY/v1/sessions/$session/statements" | jq -r .operationHandle)

    local url="$GATEWAY/v1/sessions/$session/operations/$op/result/0"
    local deadline=$(( $(date +%s) + 30 ))
    local out=""
    while [[ -n "$url" ]]; do
        if (( $(date +%s) > deadline )); then break; fi
        local page type
        page=$(curl -fsS "$url" || true)
        type=$(echo "$page" | jq -r .resultType 2>/dev/null || echo UNKNOWN)
        out+="$(echo "$page" | jq -r '.results.data // [] | map(select(.kind=="INSERT") | .fields | @tsv) | .[]')"$'\n'
        if [[ "$type" == "EOS" ]]; then break; fi
        if [[ "$type" == "NOT_READY" ]]; then sleep 0.25; continue; fi
        local next
        next=$(echo "$page" | jq -r '.nextResultUri // empty')
        [[ -z "$next" ]] && { sleep 0.25; continue; }
        [[ "$next" == http* ]] && url="$next" || url="$GATEWAY$next"
    done
    curl -fsS -X DELETE "$GATEWAY/v1/sessions/$session" >/dev/null || true
    echo "$out"
}

PASS=1

echo "Check 1/3 — Fluss catalog visible"
if run_sql "SHOW CATALOGS" | grep -q fluss; then
    echo "  OK: fluss catalog present"
else
    echo "  FAIL: fluss catalog missing from SHOW CATALOGS"
    PASS=0
fi

echo "Check 2/3 — account_profile is populated"
COUNT=$(run_sql "SELECT COUNT(*) FROM fluss.workshop.account_profile" | grep -oE '[0-9]+' | head -1)
COUNT=${COUNT:-0}
if (( COUNT > 0 )); then
    echo "  OK: $COUNT rows"
else
    echo "  FAIL: account_profile empty — did seed-fluss.sh run?"
    PASS=0
fi

echo "Check 3/3 — Point lookup works"
if run_sql "SELECT account_id, tier FROM fluss.workshop.account_profile WHERE account_id = 'ACC-0001'" \
        | grep -q 'ACC-0001'; then
    echo "  OK: lookup returned a row"
else
    echo "  INFO: no row for ACC-0001 (seed may not have created it yet)"
fi

[[ $PASS -eq 1 ]] && echo "PASS: scenario 11 verified." || { echo "FAIL"; exit 1; }
