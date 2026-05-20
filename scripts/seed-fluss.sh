#!/usr/bin/env bash
# Launch the long-running FlussDatagenJob that seeds every Fluss table.
# Tune per-table rates via env vars (see FlussDatagenJob.java for the full list).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DATAGEN_JAR="$PROJECT_DIR/common/target/flink-workshop-common-fluss-datagen-jar-with-dependencies.jar"
ENTRY="com.workshop.flink.common.fluss.FlussDatagenJob"
FLUSS_INTERNAL="${FLUSS_INTERNAL:-workshop-fluss-coordinator:9123}"
ENABLE_TABLES="${ENABLE_TABLES:-all}"

if [[ ! -f "$DATAGEN_JAR" ]]; then
    echo "Datagen JAR not found: $DATAGEN_JAR"
    echo "Run: mvn -pl common -am package -DskipTests"
    exit 1
fi

echo "Submitting FlussDatagenJob (tables=$ENABLE_TABLES)..."
podman cp "$DATAGEN_JAR" workshop-jobmanager:/tmp/fluss-datagen.jar
podman exec -d \
    -e FLUSS_BOOTSTRAP="$FLUSS_INTERNAL" \
    -e ENABLE_TABLES="$ENABLE_TABLES" \
    -e INSTRUMENT_RATE="${INSTRUMENT_RATE:-50}" \
    -e ACCOUNT_RATE="${ACCOUNT_RATE:-50}" \
    -e CUSTOMER_CORE_RATE="${CUSTOMER_CORE_RATE:-50}" \
    -e CUSTOMER_AML_RATE="${CUSTOMER_AML_RATE:-25}" \
    -e CUSTOMER_MKT_RATE="${CUSTOMER_MKT_RATE:-25}" \
    -e TRADE_WIDE_RATE="${TRADE_WIDE_RATE:-200}" \
    -e INSTRUMENT_UNIVERSE="${INSTRUMENT_UNIVERSE:-5000}" \
    -e ACCOUNT_UNIVERSE="${ACCOUNT_UNIVERSE:-1000}" \
    workshop-jobmanager \
    flink run -d -c "$ENTRY" /tmp/fluss-datagen.jar

echo "FlussDatagenJob submitted — check the Flink UI at http://localhost:18081"
