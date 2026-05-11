#!/usr/bin/env bash
# quickstart.sh — build, start infra, and run any workshop scenario locally.
#
# Usage:
#   ./quickstart.sh             # interactive menu
#   ./quickstart.sh all         # datagen + all 5 scenarios
#   ./quickstart.sh datagen     # datagen only
#   ./quickstart.sh 1           # datagen + scenario 01
#   ./quickstart.sh 1 2 3       # datagen + scenarios 01, 02, 03
#   ./quickstart.sh stop        # tear down everything
#   ./quickstart.sh status      # show running Flink jobs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

FLINK_URL="http://localhost:8081"
KAFKA_INTERNAL="workshop-kafka:9093"   # container-to-container listener
PG_URL_INTERNAL="jdbc:postgresql://workshop-postgres:5432/workshop"
PG_USER="workshop"
PG_PASSWORD="workshop"

# ── colours ────────────────────────────────────────────────────────────────────
RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[1;33m'
CYAN=$'\033[0;36m'; BOLD=$'\033[1m'; RESET=$'\033[0m'

log()  { printf "${CYAN}▶${RESET} %s\n" "$*"; }
ok()   { printf "${GREEN}✓${RESET} %s\n" "$*"; }
warn() { printf "${YELLOW}!${RESET} %s\n" "$*" >&2; }
die()  { printf "${RED}✗${RESET} %s\n" "$*" >&2; exit 1; }
header() { printf "\n${BOLD}━━━  %s  ━━━${RESET}\n" "$*"; }

# ── prereq check ───────────────────────────────────────────────────────────────
check_prereqs() {
    local missing=0
    for cmd in podman curl jq mvn; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            warn "Missing: $cmd"
            missing=1
        fi
    done
    if ! podman compose version >/dev/null 2>&1; then
        warn "Missing: 'podman compose' plugin (or podman-compose)"
        missing=1
    fi
    [[ $missing -eq 0 ]] || die "Install missing tools and retry."
}

# ── build ──────────────────────────────────────────────────────────────────────
build_if_needed() {
    local marker="common/target/flink-workshop-common-datagen-jar-with-dependencies.jar"
    if [[ ! -f "$marker" ]]; then
        header "Building project (first run)"
        mvn clean package -DskipTests -q
        ok "Build complete"
    else
        ok "JARs already built — skipping (run 'mvn package -DskipTests' to rebuild)"
    fi
}

# ── infra ──────────────────────────────────────────────────────────────────────
start_infra() {
    header "Starting infrastructure"
    podman compose -f podman-compose.yml up -d
    ok "Containers started"

    log "Waiting for Flink JobManager to be ready..."
    local retries=30
    until curl -sf "$FLINK_URL/overview" >/dev/null 2>&1; do
        retries=$((retries - 1))
        [[ $retries -gt 0 ]] || die "Flink did not become ready in time. Check: podman logs workshop-jobmanager"
        printf "."
        sleep 3
    done
    printf "\n"
    ok "Flink ready at $FLINK_URL"

    log "Creating Kafka topics..."
    bash scripts/create-topics.sh
}

# ── job submission ─────────────────────────────────────────────────────────────
# submit_job <display-name> <jar-path> <entry-class> [KEY=VALUE ...]
# Uses the Flink REST API: upload JAR → run with env vars injected via
# `podman exec -e` wrapping `flink run` inside the jobmanager container.
submit_job() {
    local name="$1" jar="$2" entry="$3"
    shift 3
    local env_args=()
    for kv in "$@"; do env_args+=(-e "$kv"); done

    log "Submitting: $name"

    local jar_name
    jar_name="$(basename "$jar")"

    # Copy JAR into the jobmanager container
    podman cp "$jar" "workshop-jobmanager:/tmp/$jar_name"

    # Run via flink CLI inside the container (env vars honoured by the JVM process)
    local job_id
    job_id="$(podman exec "${env_args[@]}" workshop-jobmanager \
        flink run -d -c "$entry" "/tmp/$jar_name" \
        2>&1 | grep -oE 'JobID [0-9a-f]+' | awk '{print $2}')"

    if [[ -n "$job_id" ]]; then
        ok "$name  →  job $job_id"
    else
        warn "$name: could not parse job ID — check Flink UI at $FLINK_URL"
    fi
}

# ── scenario helpers ───────────────────────────────────────────────────────────
run_datagen() {
    header "Datagen (FinancialDatagenJob → topic.in)"
    submit_job "Datagen" \
        "common/target/flink-workshop-common-datagen-jar-with-dependencies.jar" \
        "com.workshop.flink.common.datagen.FinancialDatagenJob" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
}

run_scenario_01() {
    header "Scenario 01 — Baseline AT_LEAST_ONCE"
    submit_job "S01 App1 (AT_LEAST_ONCE producer)" \
        "scenario-01-baseline-at-least-once/target/scenario-01-app1-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario01.App1Pipeline" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
    submit_job "S01 App2 (tumbling-window aggregator)" \
        "scenario-01-baseline-at-least-once/target/scenario-01-app2-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario01.App2Pipeline" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
}

run_scenario_02() {
    header "Scenario 02 — EXACTLY_ONCE Kafka"
    submit_job "S02 App1 (transactional Kafka sink)" \
        "scenario-02-exactly-once-kafka/target/scenario-02-app1-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario02.App1Pipeline" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
    submit_job "S02 App2 (read_committed consumer)" \
        "scenario-02-exactly-once-kafka/target/scenario-02-app2-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario02.App2Pipeline" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
}

run_scenario_03() {
    header "Scenario 03 — External Sink Duplicates"
    submit_job "S03 App1 (EXACTLY_ONCE Kafka)" \
        "scenario-03-external-sink-duplicates/target/scenario-03-app1-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario03.App1Pipeline" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
    submit_job "S03 App2 (JDBC upsert sink — fixed mode)" \
        "scenario-03-external-sink-duplicates/target/scenario-03-app2-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario03.App2Pipeline" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL" \
        "SINK_MODE=fixed" \
        "PG_URL=$PG_URL_INTERNAL" \
        "PG_USER=$PG_USER" \
        "PG_PASSWORD=$PG_PASSWORD"
}

run_scenario_04() {
    header "Scenario 04 — Upstream True Duplicates + Semantic Dedup"
    submit_job "S04 Producer (injects business duplicates)" \
        "scenario-04-upstream-true-duplicates/target/scenario-04-producer-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario04.producer.DuplicateEventProducer" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
    submit_job "S04 App1 (pass-through EXACTLY_ONCE)" \
        "scenario-04-upstream-true-duplicates/target/scenario-04-app1-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario04.App1Pipeline" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
    submit_job "S04 App2 (TTL dedup — eventId keyed)" \
        "scenario-04-upstream-true-duplicates/target/scenario-04-app2-dedup-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario04.App2PipelineWithDedup" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
}

run_scenario_05() {
    header "Scenario 05 — Rescaling and Savepoint Replay"
    submit_job "S05 App1 (stateful enrichment, parallelism 2)" \
        "scenario-05-rescaling-replay/target/scenario-05-app1-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario05.App1Pipeline" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL" \
        "APP1_PARALLELISM=2"
    submit_job "S05 App2 (event-time windows, parallelism 2)" \
        "scenario-05-rescaling-replay/target/scenario-05-app2-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario05.App2Pipeline" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL" \
        "APP2_PARALLELISM=2"
}

# ── status ─────────────────────────────────────────────────────────────────────
show_status() {
    header "Running Flink jobs"
    curl -sf "$FLINK_URL/jobs" | jq -r '.jobs[] | "\(.id)  \(.status)"' 2>/dev/null \
        || warn "Could not reach Flink at $FLINK_URL"
    printf "\nFlink UI: ${CYAN}%s${RESET}\n" "$FLINK_URL"
    printf "Kafka UI: ${CYAN}http://localhost:8080${RESET}\n\n"
}

# ── stop ───────────────────────────────────────────────────────────────────────
stop_all() {
    header "Stopping all containers"
    podman compose -f podman-compose.yml down
    ok "Done"
    exit 0
}

# ── interactive menu ───────────────────────────────────────────────────────────
interactive_menu() {
    printf "\n${BOLD}Workshop scenarios:${RESET}\n"
    printf "  ${CYAN}all${RESET}     — datagen + all 5 scenarios\n"
    printf "  ${CYAN}datagen${RESET} — datagen job only\n"
    printf "  ${CYAN}1${RESET} … ${CYAN}5${RESET}   — datagen + that scenario\n"
    printf "  ${CYAN}stop${RESET}    — tear down containers\n"
    printf "  ${CYAN}status${RESET}  — show running jobs\n\n"
    read -r -p "Run which scenario(s)? [all] " choice
    choice="${choice:-all}"
    # shellcheck disable=SC2086
    dispatch $choice
}

# ── dispatch ───────────────────────────────────────────────────────────────────
dispatch() {
    local ran_datagen=0
    for arg in "$@"; do
        case "$arg" in
            stop)   stop_all ;;
            status) show_status; exit 0 ;;
            datagen)
                [[ $ran_datagen -eq 0 ]] && { run_datagen; ran_datagen=1; }
                ;;
            all)
                [[ $ran_datagen -eq 0 ]] && { run_datagen; ran_datagen=1; }
                run_scenario_01
                run_scenario_02
                run_scenario_03
                run_scenario_04
                run_scenario_05
                ;;
            [1-5])
                [[ $ran_datagen -eq 0 ]] && { run_datagen; ran_datagen=1; }
                "run_scenario_0$arg"
                ;;
            *)
                die "Unknown argument: $arg  (expected: all | datagen | 1-5 | stop | status)"
                ;;
        esac
    done
}

# ── main ───────────────────────────────────────────────────────────────────────
main() {
    header "Flink Workshop Quickstart"

    check_prereqs
    build_if_needed

    if [[ $# -gt 0 ]]; then
        case "$1" in
            stop)   stop_all ;;
            status) start_infra; show_status; exit 0 ;;  # infra must be up for status
        esac
    fi

    start_infra

    if [[ $# -eq 0 ]]; then
        interactive_menu
    else
        dispatch "$@"
    fi

    show_status
}

main "$@"
