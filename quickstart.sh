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
FLUSS_INTERNAL="workshop-fluss-coordinator:9123"

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

run_extra_datagen_joins() {
    header "Datagen for joins track (Quotes + FX + OrderFill splitter)"
    submit_job "Quote datagen" \
        "common/target/flink-workshop-common-quote-datagen-jar-with-dependencies.jar" \
        "com.workshop.flink.common.datagen.QuoteDatagenJob" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
    submit_job "FX rate datagen" \
        "common/target/flink-workshop-common-fx-datagen-jar-with-dependencies.jar" \
        "com.workshop.flink.common.datagen.FxRateDatagenJob" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
    submit_job "Order/Fill splitter" \
        "common/target/flink-workshop-common-order-fill-splitter-jar-with-dependencies.jar" \
        "com.workshop.flink.common.datagen.OrderFillSplitterJob" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
}

seed_accounts() {
    header "Seeding Postgres 'accounts' table (scenarios 08 + 09)"
    ACCOUNT_COUNT="${ACCOUNT_COUNT:-50}" \
    KAFKA_INTERNAL="$KAFKA_INTERNAL" \
    PG_URL_INTERNAL="$PG_URL_INTERNAL" \
    PG_USER="$PG_USER" \
    PG_PASSWORD="$PG_PASSWORD" \
        bash scripts/seed-accounts.sh
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

run_scenario_07() {
    header "Scenario 07 — Stream-Stream Joins (regular, interval, window)"
    run_extra_datagen_joins
    submit_job "S07a Regular join (orders ⋈ fills, all join kinds)" \
        "scenario-07-stream-stream-joins/target/scenario-07-regular-join-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario07.App07RegularJoin" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
    submit_job "S07b Interval join (trade ⋈ quote ±5s)" \
        "scenario-07-stream-stream-joins/target/scenario-07-interval-join-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario07.App07IntervalJoin" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
    submit_job "S07c Window join (1m tumbling)" \
        "scenario-07-stream-stream-joins/target/scenario-07-window-join-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario07.App07WindowJoin" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
}

run_scenario_08() {
    header "Scenario 08 — Stream-Table Joins (lookup vs temporal)"
    run_extra_datagen_joins
    seed_accounts
    submit_job "S08a Lookup join (Postgres accounts, async + Caffeine)" \
        "scenario-08-stream-table-joins/target/scenario-08-lookup-join-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario08.App08LookupJoin" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL" \
        "PG_URL=$PG_URL_INTERNAL" \
        "PG_USER=$PG_USER" \
        "PG_PASSWORD=$PG_PASSWORD"
    submit_job "S08b Temporal join (trade AS OF fx, versioned)" \
        "scenario-08-stream-table-joins/target/scenario-08-temporal-join-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario08.App08TemporalJoin" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
}

run_scenario_09() {
    header "Scenario 09 — Multi-Way Join + Crash Recovery"
    run_extra_datagen_joins
    seed_accounts
    submit_job "S09 Multi-way join (trade ⋈ quote ⋈ fx ⋈ account, crash @ 5000)" \
        "scenario-09-multiway-join-recovery/target/scenario-09-multiway-join-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario09.App09MultiWayJoin" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL" \
        "PG_URL=$PG_URL_INTERNAL" \
        "PG_USER=$PG_USER" \
        "PG_PASSWORD=$PG_PASSWORD" \
        "CRASH_AFTER_RECORDS=${CRASH_AFTER_RECORDS:-5000}"
}

run_scenario_10() {
    header "Scenario 10 — Event-Time, Watermarks, Late Data (SQL-only)"
    log "This scenario is SQL-first and uses the scenario-06 SQL Gateway."
    log "Submit the late-data datagen job, then run scripts/run-scenario-10-sql.sh."
    submit_job "Late-arriving trade datagen" \
        "common/target/flink-workshop-common-datagen-jar-with-dependencies.jar" \
        "com.workshop.flink.common.datagen.FinancialDatagenJob" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL"
    log "Run: bash scripts/run-scenario-10-sql.sh   # walks through the SQL teaching path"
}

bootstrap_fluss() {
    header "Bootstrapping Fluss catalog + seeding wide PK tables"
    bash scripts/init-fluss.sh
    bash scripts/seed-fluss.sh
}

run_scenario_11() {
    header "Scenario 11 — Fluss Fundamentals (SQL-only)"
    bootstrap_fluss
    log "Open the SQL Client (bash scripts/sql-client.sh) and paste sql/01..07 in order."
    log "See scenario-11-fluss-fundamentals/WORKSHOP.md for the deep walkthrough."
}

run_scenario_12() {
    header "Scenario 12 — Wide schemas + projection pushdown (SQL-only)"
    bootstrap_fluss
    log "Open the SQL Client and paste scenario-12-fluss-wide-schemas/sql/01..06."
}

run_scenario_13() {
    header "Scenario 13 — Partial Updates + Merge Engines"
    bootstrap_fluss
    log "SQL teaching path: sql/01..05 in the SQL Client."
    submit_job "S13 WideProfileUpdaterJob (production-shaped partial-update fan-out)" \
        "scenario-13-fluss-partial-updates/target/scenario-13-wide-profile-updater-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario13.WideProfileUpdaterJob" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL" \
        "FLUSS_BOOTSTRAP=$FLUSS_INTERNAL"
}

run_scenario_14() {
    header "Scenario 14 — Streaming Joins (SQL-only)"
    bootstrap_fluss
    log "Make sure FinancialDatagenJob is running (./quickstart.sh datagen)."
    log "Open the SQL Client and paste scenario-14-fluss-streaming-joins/sql/01..05."
}

run_scenario_15() {
    header "Scenario 15 — Fluss Java Clients"
    bootstrap_fluss
    log "Three apps available in scenario-15-fluss-java-clients/target/:"
    log "  - point-in-time-lookup: standalone Java SDK client"
    log "  - http-service: REST service over Fluss"
    log "  - flink-enrichment: DataStream + Fluss connector"
    log "See README for run commands."
    submit_job "S15 FlussFlinkConnectorEnrichmentJob (Flink + Fluss lookup join)" \
        "scenario-15-fluss-java-clients/target/scenario-15-flink-enrichment-jar-with-dependencies.jar" \
        "com.workshop.flink.scenario15.FlussFlinkConnectorEnrichmentJob" \
        "KAFKA_BOOTSTRAP=$KAFKA_INTERNAL" \
        "FLUSS_BOOTSTRAP=$FLUSS_INTERNAL"
}

run_scenario_16() {
    header "Scenario 16 — Tiered Storage + Spark"
    bootstrap_fluss
    log "1. Paste scenario-16-fluss-paimon-tiered-spark/sql/01-enable-tiering.sql"
    log "2. Wait ~1 minute for tiering to fire"
    log "3. Inspect with sql/02..04, then run:"
    log "   bash scripts/spark-submit-scenario-16.sh both"
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
    printf "  ${CYAN}all${RESET}     — datagen + all scenarios\n"
    printf "  ${CYAN}datagen${RESET} — datagen job only\n"
    printf "  ${CYAN}1${RESET}–${CYAN}5${RESET}     — reliability/exactly-once/dedup/rescale\n"
    printf "  ${CYAN}7${RESET}       — stream-stream joins (regular, interval, window)\n"
    printf "  ${CYAN}8${RESET}       — stream-table joins (lookup vs temporal)\n"
    printf "  ${CYAN}9${RESET}       — multi-way join + crash recovery\n"
    printf "  ${CYAN}10${RESET}      — event-time, watermarks, late data (SQL)\n"
    printf "  ${CYAN}11${RESET}      — Apache Fluss fundamentals (SQL)\n"
    printf "  ${CYAN}12${RESET}      — Fluss wide schemas + projection pushdown (SQL)\n"
    printf "  ${CYAN}13${RESET}      — Fluss partial updates + merge engines (SQL + Java)\n"
    printf "  ${CYAN}14${RESET}      — Fluss streaming joins (SQL)\n"
    printf "  ${CYAN}15${RESET}      — Fluss Java clients: SDK + HTTP service + Flink DataStream\n"
    printf "  ${CYAN}16${RESET}      — Fluss tiered storage to Paimon + Spark queries\n"
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
                run_scenario_07
                run_scenario_08
                run_scenario_09
                run_scenario_10
                run_scenario_11
                run_scenario_12
                run_scenario_13
                run_scenario_14
                run_scenario_15
                run_scenario_16
                ;;
            [1-5])
                [[ $ran_datagen -eq 0 ]] && { run_datagen; ran_datagen=1; }
                "run_scenario_0$arg"
                ;;
            7|8|9)
                [[ $ran_datagen -eq 0 ]] && { run_datagen; ran_datagen=1; }
                "run_scenario_0$arg"
                ;;
            10|11|12|13|14|15|16)
                [[ $ran_datagen -eq 0 ]] && { run_datagen; ran_datagen=1; }
                "run_scenario_$arg"
                ;;
            *)
                die "Unknown argument: $arg  (expected: all | datagen | 1-5 | 7..16 | stop | status)"
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
