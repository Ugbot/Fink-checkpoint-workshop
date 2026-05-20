#!/usr/bin/env bash
# Build the scenario 16 Spark JARs and submit them inside workshop-spark.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

BATCH_JAR="$PROJECT_DIR/scenario-16-fluss-paimon-tiered-spark/target/scenario-16-spark-paimon-batch-jar-with-dependencies.jar"
LIVE_JAR="$PROJECT_DIR/scenario-16-fluss-paimon-tiered-spark/target/scenario-16-spark-fluss-live-jar-with-dependencies.jar"

if [[ ! -f "$BATCH_JAR" || ! -f "$LIVE_JAR" ]]; then
    echo "Spark JAR(s) missing. Build first:"
    echo "  mvn -pl scenario-16-fluss-paimon-tiered-spark -am package -DskipTests"
    exit 1
fi

WHICH="${1:-both}"

run_batch() {
    echo "▶ SparkPaimonBatchJob (Paimon-direct)"
    podman cp "$BATCH_JAR" workshop-spark:/tmp/scenario-16-batch.jar
    podman exec workshop-spark spark-submit \
        --class com.workshop.flink.scenario16.SparkPaimonBatchJob \
        /tmp/scenario-16-batch.jar
}

run_live() {
    echo "▶ SparkFlussLiveJob (live Fluss read)"
    podman cp "$LIVE_JAR" workshop-spark:/tmp/scenario-16-live.jar
    podman exec workshop-spark spark-submit \
        --class com.workshop.flink.scenario16.SparkFlussLiveJob \
        /tmp/scenario-16-live.jar
}

case "$WHICH" in
    batch) run_batch ;;
    live)  run_live ;;
    both)  run_batch; echo; run_live ;;
    *) echo "Usage: $0 [batch|live|both]"; exit 1 ;;
esac
