#!/usr/bin/env bash
# Walk through scenario 10's SQL teaching path against the SQL Gateway from scenario 06.
#
# Prereqs:
#   1. Scenario 06 infra is up (the SQL Gateway listens on :18083)
#   2. topic.in is populated (./quickstart.sh datagen)
#   3. For step 8: topic.quotes is populated (./quickstart.sh 7 starts QuoteDatagenJob)
#
# Usage:
#   bash scripts/run-scenario-10-sql.sh            # run all steps in order
#   bash scripts/run-scenario-10-sql.sh 4 6 7      # run steps 4, 6, 7 only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SQL_DIR="$PROJECT_DIR/scenario-10-event-time-sql/sql"
GATEWAY="${GATEWAY:-http://localhost:18083}"

command -v jq >/dev/null || { echo "jq is required (brew install jq)"; exit 1; }

# Collect steps
declare -a STEPS=()
if [[ $# -gt 0 ]]; then
    for n in "$@"; do
        STEPS+=( "$(printf '%02d' "$n")" )
    done
else
    for f in "$SQL_DIR"/*.sql; do
        STEPS+=( "$(basename "$f" | cut -c1-2)" )
    done
fi

# Open one session and reuse it across steps so SET properties persist.
SESSION_BODY='{"properties":{"execution.runtime-mode":"streaming"}}'
SESSION=$(curl -fsS -X POST -H "Content-Type: application/json" \
  -d "$SESSION_BODY" "$GATEWAY/v1/sessions" | jq -r .sessionHandle)
echo "Opened session: $SESSION"

cleanup() {
    curl -fsS -X DELETE "$GATEWAY/v1/sessions/$SESSION" >/dev/null || true
}
trap cleanup EXIT

submit_sql() {
    local sql="$1"
    local body
    body=$(jq -nc --arg sql "$sql" '{statement:$sql}')
    curl -fsS -X POST -H "Content-Type: application/json" \
        -d "$body" "$GATEWAY/v1/sessions/$SESSION/statements" | jq -r .operationHandle
}

drain_result() {
    local op="$1"
    local url="$GATEWAY/v1/sessions/$SESSION/operations/$op/result/0"
    local deadline=$(( $(date +%s) + 30 ))
    while [[ -n "$url" ]]; do
        if (( $(date +%s) > deadline )); then
            echo "    (cut off after 30s — streaming queries don't terminate; cancel via gateway)"
            curl -fsS -X POST "$GATEWAY/v1/sessions/$SESSION/operations/$op/cancel" >/dev/null || true
            return 0
        fi
        local page type
        page=$(curl -fsS "$url" || true)
        type=$(echo "$page" | jq -r .resultType 2>/dev/null || echo UNKNOWN)
        case "$type" in
            NOT_READY) sleep 0.25 ;;
            EOS) echo "$page" | jq -r '.results.data // [] | map(select(.kind=="INSERT") | .fields | @tsv) | .[]'; break ;;
            *)
                echo "$page" | jq -r '.results.data // [] | map(select(.kind=="INSERT") | .fields | @tsv) | .[]'
                local next
                next=$(echo "$page" | jq -r '.nextResultUri // empty')
                [[ -z "$next" ]] && { sleep 0.25; continue; }
                if [[ "$next" == http* ]]; then url="$next"; else url="$GATEWAY$next"; fi
                ;;
        esac
    done
}

# Split a file into statements by ';' boundaries; very naive but enough for these files.
run_file() {
    local file="$1"
    echo
    echo "────────────────────────────────────────────────────────────────"
    echo "▶ $(basename "$file")"
    echo "────────────────────────────────────────────────────────────────"
    local buf=""
    while IFS= read -r line || [[ -n "$line" ]]; do
        # Strip line comments
        local stripped="${line%%--*}"
        buf+="$stripped"$'\n'
        if [[ "$stripped" == *";"* ]]; then
            # Extract the statement(s) before the last ';'
            local stmt="${buf%;*};"
            stmt="$(echo "$stmt" | tr -d '\r' | sed 's/[[:space:]]*$//')"
            if [[ -n "$(echo "$stmt" | tr -d ' \n;')" ]]; then
                echo
                echo "  SQL: $(echo "$stmt" | tr '\n' ' ' | sed 's/  */ /g' | cut -c1-200)..."
                local op
                op=$(submit_sql "$stmt") || { echo "  submit failed"; buf=""; continue; }
                drain_result "$op"
            fi
            buf=""
        fi
    done < "$file"
}

for s in "${STEPS[@]}"; do
    f="$(ls "$SQL_DIR"/${s}-*.sql 2>/dev/null | head -1 || true)"
    if [[ -z "$f" ]]; then
        echo "No SQL file matches step $s — skipping"
        continue
    fi
    run_file "$f"
done

echo
echo "Done. Streaming queries continue running in the Gateway."
echo "Inspect them at: $GATEWAY/v1/sessions/$SESSION/operations"
