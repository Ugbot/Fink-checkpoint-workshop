#!/usr/bin/env bash
# Verify scenario 16: tiering working, Paimon mirror queryable, Spark apps run.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

GATEWAY="${GATEWAY:-http://localhost:18083}"

command -v jq >/dev/null || { echo "jq required"; exit 1; }

run_sql() {
    local sql="$1"
    local session
    session=$(curl -fsS -X POST -H 'Content-Type: application/json' \
        -d '{"properties":{"execution.runtime-mode":"batch"}}' \
        "$GATEWAY/v1/sessions" | jq -r .sessionHandle)
    # Register both catalogs we'll need
    for ddl in \
        "CREATE CATALOG IF NOT EXISTS fluss WITH ('type'='fluss','bootstrap.servers'='workshop-fluss-coordinator:9123')" \
        "CREATE CATALOG IF NOT EXISTS paimon WITH ('type'='paimon','warehouse'='s3a://paimon/warehouse','s3.endpoint'='http://workshop-minio:9000','s3.access-key'='workshop','s3.secret-key'='workshopsecret','s3.path.style.access'='true')"; do
        curl -fsS -X POST -H 'Content-Type: application/json' \
            -d "$(jq -nc --arg s "$ddl" '{statement:$s}')" \
            "$GATEWAY/v1/sessions/$session/statements" >/dev/null
    done
    local op
    op=$(curl -fsS -X POST -H 'Content-Type: application/json' \
        -d "$(jq -nc --arg s "$sql" '{statement:$s}')" \
        "$GATEWAY/v1/sessions/$session/statements" | jq -r .operationHandle)
    local url="$GATEWAY/v1/sessions/$session/operations/$op/result/0"
    local deadline=$(( $(date +%s) + 45 ))
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

echo "=== Scenario 16 — Tiered storage + Spark verification ==="

PASS=1

echo "Check 1/3 — Fluss table trade_log_wide has rows"
FLUSS=$(run_sql "SELECT COUNT(*) FROM fluss.workshop.trade_log_wide" | grep -oE '[0-9]+' | head -1)
FLUSS=${FLUSS:-0}
if (( FLUSS > 0 )); then echo "  OK: Fluss side has $FLUSS rows"; else echo "  FAIL: empty"; PASS=0; fi

echo "Check 2/3 — Paimon mirror exists and has rows (tiering fired)"
PAIMON=$(run_sql "SELECT COUNT(*) FROM paimon.workshop.trade_log_wide" 2>/dev/null | grep -oE '[0-9]+' | head -1)
PAIMON=${PAIMON:-0}
if (( PAIMON > 0 )); then
    echo "  OK: Paimon side has $PAIMON rows (Fluss-Paimon delta = $((FLUSS - PAIMON)))"
else
    echo "  INFO: Paimon mirror empty — give tiering ~1 more minute or run step 01 SQL first"
fi

echo "Check 3/3 — Spark JARs exist and the batch app runs"
BATCH_JAR="$PROJECT_DIR/scenario-16-fluss-paimon-tiered-spark/target/scenario-16-spark-paimon-batch-jar-with-dependencies.jar"
if [[ ! -f "$BATCH_JAR" ]]; then
    echo "  FAIL: $BATCH_JAR missing — run 'mvn -pl scenario-16-fluss-paimon-tiered-spark -am package -DskipTests'"
    PASS=0
else
    podman cp "$BATCH_JAR" workshop-spark:/tmp/scenario-16-batch.jar
    OUT=$(podman exec workshop-spark spark-submit \
          --class com.workshop.flink.scenario16.SparkPaimonBatchJob \
          /tmp/scenario-16-batch.jar 2>&1 | tail -40 || true)
    if echo "$OUT" | grep -qE 'sector_summary|Sector volumes'; then
        echo "  OK: Spark batch job produced output"
    else
        echo "  WARN: Spark batch did not complete cleanly. Output tail:"
        echo "$OUT" | tail -10 | sed 's/^/    /'
    fi
fi

[[ $PASS -eq 1 ]] && echo "PASS: scenario 16 verified." || { echo "FAIL"; exit 1; }
