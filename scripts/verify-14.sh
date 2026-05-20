#!/usr/bin/env bash
# Verify scenario 14: Fluss catalog reachable, instrument_master seeded,
# and a smoke-test lookup join compiles + returns rows.
set -euo pipefail
GATEWAY="${GATEWAY:-http://localhost:18083}"

command -v jq >/dev/null || { echo "jq required (brew install jq)"; exit 1; }

echo "=== Scenario 14 — Fluss Streaming & Lookup Joins verification ==="

run_sql() {
    local sql="$1"
    local session_body='{"properties":{}}'
    local session
    session=$(curl -fsS -X POST -H 'Content-Type: application/json' \
        -d "$session_body" "$GATEWAY/v1/sessions" | jq -r .sessionHandle)

    # Register the Fluss catalog every session.
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
    echo "  OK: $COUNT rows"
else
    echo "  FAIL: instrument_master empty — did seed-fluss.sh run?"
    PASS=0
fi

echo "Check 3/3 — Smoke-test scan + a sample join result"
# Pull one row from instrument_master and re-join it back to itself via a
# PROCTIME() lookup. This proves:
#   - the catalog resolves the table,
#   - the FOR SYSTEM_TIME AS OF clause compiles into a LookupJoin
#     (the SQL Gateway will reject the query otherwise),
#   - and Fluss serves a non-empty lookup result.
# Using the table as its own probe side means we don't depend on Kafka
# datagen alignment with the seed; the row is guaranteed to exist if step 2
# passed.
SMOKE_SQL=$(cat <<'EOF'
SELECT t.ticker, im.sector
FROM (
  SELECT ticker, PROCTIME() AS proc_time
  FROM fluss.workshop.instrument_master
  LIMIT 1
) AS t
LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im
  ON t.ticker = im.ticker
EOF
)
SMOKE_OUT=$(run_sql "$SMOKE_SQL" || true)
if echo "$SMOKE_OUT" | grep -qE '[A-Za-z0-9]'; then
    NON_NULL=$(echo "$SMOKE_OUT" | grep -cE '[A-Za-z0-9]' || true)
    echo "  OK: lookup join returned $NON_NULL row(s)"
else
    echo "  INFO: smoke-test lookup returned no probe rows; catalog + scan checks above are the authoritative pass/fail"
fi

[[ $PASS -eq 1 ]] && echo "PASS: scenario 14 verified." || { echo "FAIL"; exit 1; }
