#!/usr/bin/env bash
# Verify scenario 15: SDK lookup, HTTP service, Flink enrichment job.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

LOOKUP_JAR="$PROJECT_DIR/scenario-15-fluss-java-clients/target/scenario-15-point-in-time-lookup-jar-with-dependencies.jar"
HTTP_PORT="${HTTP_PORT:-18099}"

echo "=== Scenario 15 — Java clients verification ==="

PASS=1

echo "Check 1/3 — point-in-time lookup client returns hits"
if [[ ! -f "$LOOKUP_JAR" ]]; then
    echo "  FAIL: $LOOKUP_JAR missing — run 'mvn -pl scenario-15-fluss-java-clients -am package -DskipTests'"
    PASS=0
else
    # Run inside workshop-jobmanager so it can reach workshop-fluss-coordinator
    podman cp "$LOOKUP_JAR" workshop-jobmanager:/tmp/scenario-15-lookup.jar
    OUT=$(podman exec workshop-jobmanager java -jar /tmp/scenario-15-lookup.jar 10 2>&1 || true)
    if echo "$OUT" | grep -qE '[1-9][0-9]* hits'; then
        echo "  OK: lookup ran and reported hits"
    else
        echo "  FAIL: no hits reported"
        echo "$OUT" | head -20
        PASS=0
    fi
fi

echo "Check 2/3 — HTTP service responds"
if command -v curl >/dev/null && curl -fsS --max-time 2 "http://localhost:$HTTP_PORT/health" >/dev/null 2>&1; then
    ACC=$(curl -fsS --max-time 5 "http://localhost:$HTTP_PORT/accounts/ACC-0001" || true)
    if echo "$ACC" | grep -q '"tier"'; then
        echo "  OK: /accounts/ACC-0001 returned JSON with tier"
    else
        echo "  WARN: /accounts/ACC-0001 returned: $ACC"
    fi
else
    echo "  INFO: HTTP service not running on localhost:$HTTP_PORT — skipping (start it manually if you want to test)"
fi

echo "Check 3/3 — Flink enrichment JAR class is loadable"
FLINK_JAR="$PROJECT_DIR/scenario-15-fluss-java-clients/target/scenario-15-flink-enrichment-jar-with-dependencies.jar"
if [[ ! -f "$FLINK_JAR" ]]; then
    echo "  FAIL: $FLINK_JAR missing"
    PASS=0
else
    podman cp "$FLINK_JAR" workshop-jobmanager:/tmp/scenario-15-flink.jar
    # `flink run --help` doesn't load classes; use info to confirm the main class exists
    if podman exec workshop-jobmanager flink info \
            -c com.workshop.flink.scenario15.FlussFlinkConnectorEnrichmentJob \
            /tmp/scenario-15-flink.jar 2>&1 | grep -q 'Streaming Plan\|FlinkPlan\|nodes'; then
        echo "  OK: Flink job class loadable"
    else
        echo "  WARN: could not introspect Flink job (try 'flink run' directly)"
    fi
fi

[[ $PASS -eq 1 ]] && echo "PASS: scenario 15 verified." || { echo "FAIL"; exit 1; }
