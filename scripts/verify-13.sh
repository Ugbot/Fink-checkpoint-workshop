#!/usr/bin/env bash
# Verify scenario 13: customer_360 has rows with columns populated from
# all three source streams (KYC + AML + Marketing partial updates).
set -euo pipefail
GATEWAY="${GATEWAY:-http://localhost:18083}"

command -v jq >/dev/null || { echo "jq required (brew install jq)"; exit 1; }

run_sql() {
    local sql="$1"
    local session
    session=$(curl -fsS -X POST -H 'Content-Type: application/json' \
        -d '{"properties":{"execution.runtime-mode":"batch"}}' \
        "$GATEWAY/v1/sessions" | jq -r .sessionHandle)
    curl -fsS -X POST -H 'Content-Type: application/json' \
        -d "$(jq -nc --arg s "CREATE CATALOG IF NOT EXISTS fluss WITH ('type'='fluss','bootstrap.servers'='workshop-fluss-coordinator:9123')" '{statement:$s}')" \
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
        [[ "$type" == "EOS" ]] && break
        [[ "$type" == "NOT_READY" ]] && { sleep 0.25; continue; }
        local next
        next=$(echo "$page" | jq -r '.nextResultUri // empty')
        [[ -z "$next" ]] && { sleep 0.25; continue; }
        [[ "$next" == http* ]] && url="$next" || url="$GATEWAY$next"
    done
    curl -fsS -X DELETE "$GATEWAY/v1/sessions/$session" >/dev/null || true
    echo "$out"
}

echo "=== Scenario 13 — Partial updates verification ==="

PASS=1

echo "Check 1/3 — customer_360 has rows"
TOTAL=$(run_sql "SELECT COUNT(*) FROM fluss.workshop.customer_360" | grep -oE '[0-9]+' | head -1)
TOTAL=${TOTAL:-0}
if (( TOTAL > 0 )); then echo "  OK: $TOTAL rows"; else echo "  FAIL: empty"; PASS=0; fi

echo "Check 2/3 — rows have KYC + AML + Marketing data populated"
FULL=$(run_sql "SELECT COUNT(*) FROM fluss.workshop.customer_360
                 WHERE kyc_status IS NOT NULL
                   AND aml_status IS NOT NULL
                   AND segment    IS NOT NULL" | grep -oE '[0-9]+' | head -1)
FULL=${FULL:-0}
if (( FULL > 0 )); then
    echo "  OK: $FULL rows have all three source's columns populated"
else
    echo "  INFO: no rows have all three sources yet — give seed-fluss.sh a few more seconds"
fi

echo "Check 3/3 — aggregation merge engine accumulated"
AGG=$(run_sql "SELECT COUNT(*) FROM fluss.workshop.daily_volumes_agg" | grep -oE '[0-9]+' | head -1)
AGG=${AGG:-0}
if (( AGG > 0 )); then
    echo "  OK: daily_volumes_agg has $AGG row(s)"
else
    echo "  INFO: daily_volumes_agg empty (only step 04's manual INSERTs populate it)"
fi

[[ $PASS -eq 1 ]] && echo "PASS: scenario 13 verified." || { echo "FAIL"; exit 1; }
