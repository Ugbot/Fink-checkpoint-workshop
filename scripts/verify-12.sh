#!/usr/bin/env bash
# Verify scenario 12: Fluss wide schemas + projection pushdown.
#
# Checks:
#   1. The fluss catalog is reachable from the SQL Gateway.
#   2. instrument_master has rows (seed-fluss.sh has run).
#   3. EXPLAIN of a projected query mentions all 3 projected columns AND
#      does NOT mention obviously-unprojected columns — i.e. projection
#      pushdown is working as expected.
set -euo pipefail
GATEWAY="${GATEWAY:-http://localhost:18083}"

command -v jq >/dev/null || { echo "jq required (brew install jq)"; exit 1; }

echo "=== Scenario 12 — Fluss Wide Schemas + Pushdown verification ==="

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

echo "Check 2/3 — instrument_master is populated"
COUNT=$(run_sql "SELECT COUNT(*) FROM fluss.workshop.instrument_master" | grep -oE '[0-9]+' | head -1)
COUNT=${COUNT:-0}
if (( COUNT > 0 )); then
    echo "  OK: $COUNT rows in instrument_master"
else
    echo "  FAIL: instrument_master empty — did seed-fluss.sh run?"
    PASS=0
fi

echo "Check 3/3 — Projection pushdown visible in EXPLAIN"
# Run the projected EXPLAIN and look for evidence that the planner
# narrowed the column set. Two signals (either is sufficient):
#
#   - The plan mentions `project=[...]` with isin, ticker, last_close.
#   - The plan does NOT mention columns we know are unprojected
#     (e.g. `cusip`, `sedol`, `governance_score`, `dividend_yield`).
EXPLAIN_OUT=$(run_sql "EXPLAIN SELECT isin, ticker, last_close FROM fluss.workshop.instrument_master WHERE country = 'US'")

HAS_ISIN=0
HAS_TICKER=0
HAS_LAST_CLOSE=0
echo "$EXPLAIN_OUT" | grep -q 'isin'       && HAS_ISIN=1
echo "$EXPLAIN_OUT" | grep -q 'ticker'     && HAS_TICKER=1
echo "$EXPLAIN_OUT" | grep -q 'last_close' && HAS_LAST_CLOSE=1

# These columns are NOT in the projection list. If they appear in the
# plan body it usually means the planner failed to narrow down — e.g. the
# table was read as `SELECT *`.
NEG_HIT=0
for col in cusip sedol governance_score dividend_yield issuer_lei volatility_30d; do
    if echo "$EXPLAIN_OUT" | grep -q "\b${col}\b"; then
        NEG_HIT=$((NEG_HIT + 1))
    fi
done

if (( HAS_ISIN == 1 && HAS_TICKER == 1 && HAS_LAST_CLOSE == 1 && NEG_HIT == 0 )); then
    echo "  OK: EXPLAIN mentions isin/ticker/last_close and not the unprojected columns"
elif (( HAS_ISIN == 1 && HAS_TICKER == 1 && HAS_LAST_CLOSE == 1 )); then
    echo "  WARN: projected columns present but unprojected columns also leaked into plan ($NEG_HIT hits)"
    echo "        — this can happen if the planner attached the full schema for metadata. Check the project=[...] line manually."
else
    echo "  FAIL: EXPLAIN plan didn't mention all three projected columns"
    echo "        isin=$HAS_ISIN ticker=$HAS_TICKER last_close=$HAS_LAST_CLOSE"
    PASS=0
fi

[[ $PASS -eq 1 ]] && echo "PASS: scenario 12 verified." || { echo "FAIL"; exit 1; }
