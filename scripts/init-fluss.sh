#!/usr/bin/env bash
# Wait for the Fluss coordinator to be reachable, then submit the
# idempotent FlussInitJob that creates the workshop database + tables.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

INIT_JAR="$PROJECT_DIR/common/target/flink-workshop-common-fluss-init-jar-with-dependencies.jar"
ENTRY="com.workshop.flink.common.fluss.FlussInitJob"
FLUSS_INTERNAL="${FLUSS_INTERNAL:-workshop-fluss-coordinator:9123}"

if [[ ! -f "$INIT_JAR" ]]; then
    echo "Init JAR not found: $INIT_JAR"
    echo "Run: mvn -pl common -am package -DskipTests"
    exit 1
fi

echo "Waiting for Fluss coordinator at $FLUSS_INTERNAL ..."
RETRIES=60
while (( RETRIES > 0 )); do
    if podman exec workshop-fluss-coordinator bash -c \
            "echo ruok | nc workshop-fluss-zk 2181 2>/dev/null | grep -q imok" 2>/dev/null; then
        echo "  ZK reachable, assuming coordinator is up"
        break
    fi
    sleep 2
    RETRIES=$((RETRIES - 1))
done
[[ $RETRIES -gt 0 ]] || { echo "Fluss coordinator did not become ready in time"; exit 1; }

echo "Submitting FlussInitJob ..."
podman cp "$INIT_JAR" workshop-jobmanager:/tmp/fluss-init.jar
podman exec \
    -e FLUSS_BOOTSTRAP="$FLUSS_INTERNAL" \
    workshop-jobmanager \
    flink run -c "$ENTRY" /tmp/fluss-init.jar

echo "Fluss init complete."
