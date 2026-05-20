package com.workshop.flink.common.util;

import com.workshop.flink.common.Constants;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Resolves all job configuration from CLI args, then env vars, then hard-coded defaults.
 *
 * Usage in any main:
 *   JobParams params = JobParams.fromArgs(args);
 *
 * Supported CLI flags (all optional; env var fallbacks shown):
 *
 *   --kafka             Kafka bootstrap (KAFKA_BOOTSTRAP / localhost:19092)
 *   --checkpoint-dir    Flink checkpoint path (CHECKPOINT_DIR / /tmp/flink-checkpoints/<id>)
 *   --crash-after       Crash after N records (CRASH_AFTER_RECORDS / disabled)
 *   --sink-mode         "buggy" or "fixed" for scenario-03 (SINK_MODE / buggy)
 *   --parallelism       Task parallelism (APP1_PARALLELISM or APP2_PARALLELISM / 2)
 *   --dedup-ttl-hours   Dedup state TTL in hours (DEDUP_TTL_HOURS / 1)
 *   --datagen-rate      Events per second from datagen (DATAGEN_RATE / 50)
 *   --pg-url            PostgreSQL JDBC URL (PG_URL / localhost:15432)
 *   --pg-user           PostgreSQL user (PG_USER / workshop)
 *   --pg-password       PostgreSQL password (PG_PASSWORD / workshop)
 *
 * When deploying via `flink run`:
 *   flink run myjar.jar --kafka broker:9092 --crash-after 50
 *
 * When running locally:
 *   java -jar myjar.jar --kafka broker:9092
 *   KAFKA_BOOTSTRAP=broker:9092 java -jar myjar.jar
 */
public final class JobParams {

    private final ParameterTool params;

    private JobParams(ParameterTool params) {
        this.params = params;
    }

    public static JobParams fromArgs(String[] args) {
        return new JobParams(ParameterTool.fromArgs(args));
    }

    public String kafka() {
        return params.get("kafka",
            System.getenv().getOrDefault(Constants.KAFKA_BOOTSTRAP_ENV,
                                         Constants.KAFKA_BOOTSTRAP_DEFAULT));
    }

    public String checkpointDir(String scenarioApp) {
        return params.get("checkpoint-dir",
            System.getenv().getOrDefault(Constants.CHECKPOINT_DIR_ENV,
                "file:///tmp/flink-checkpoints/" + scenarioApp));
    }

    public CrashTrigger crashTrigger() {
        String val = params.get("crash-after",
            System.getenv().getOrDefault(Constants.CRASH_AFTER_RECORDS_ENV, ""));
        if (val == null || val.isBlank()) {
            return CrashTrigger.disabled();
        }
        return new CrashTrigger(Integer.parseInt(val.trim()));
    }

    public String sinkMode() {
        return params.get("sink-mode",
            System.getenv().getOrDefault("SINK_MODE", "buggy"));
    }

    /** Resolves parallelism from --parallelism, then the given env var, then defaultVal. */
    public int parallelism(String envVar, int defaultVal) {
        String val = params.get("parallelism",
            System.getenv().getOrDefault(envVar, String.valueOf(defaultVal)));
        return Integer.parseInt(val);
    }

    public long dedupTtlHours() {
        return Long.parseLong(params.get("dedup-ttl-hours",
            System.getenv().getOrDefault("DEDUP_TTL_HOURS", "1")));
    }

    public int datagenRate() {
        return Integer.parseInt(params.get("datagen-rate",
            System.getenv().getOrDefault("DATAGEN_RATE", "50")));
    }

    public String pgUrl() {
        return params.get("pg-url",
            System.getenv().getOrDefault(Constants.PG_URL_ENV, Constants.PG_URL_DEFAULT));
    }

    public String pgUser() {
        return params.get("pg-user",
            System.getenv().getOrDefault(Constants.PG_USER_ENV, Constants.PG_USER_DEFAULT));
    }

    public String pgPassword() {
        return params.get("pg-password",
            System.getenv().getOrDefault(Constants.PG_PASSWORD_ENV, Constants.PG_PASSWORD_DEFAULT));
    }

    /** Number of Kafka topic partitions for topic setup. CLI: --topic-partitions <n> */
    public int topicPartitions() {
        return Integer.parseInt(params.get("topic-partitions",
            System.getenv().getOrDefault("TOPIC_PARTITIONS", "4")));
    }

    /** Total unique events for DuplicateEventProducer. CLI: --total-events <n> */
    public int totalEvents() {
        return Integer.parseInt(params.get("total-events",
            System.getenv().getOrDefault("TOTAL_EVENTS", "100")));
    }

    /** Duplicate every Nth event. CLI: --duplicate-every <n> */
    public int duplicateEvery() {
        return Integer.parseInt(params.get("duplicate-every",
            System.getenv().getOrDefault("DUPLICATE_EVERY", "10")));
    }

    /** Pause between original and duplicate send. CLI: --duplicate-delay-ms <ms> */
    public long duplicateDelayMs() {
        return Long.parseLong(params.get("duplicate-delay-ms",
            System.getenv().getOrDefault("DUPLICATE_DELAY_MS", "200")));
    }

    /** Quote events per second from QuoteDatagenJob. CLI: --quote-rate <n> */
    public int quoteRate() {
        return Integer.parseInt(params.get("quote-rate",
            System.getenv().getOrDefault("QUOTE_RATE", "500")));
    }

    /** FX rate changelog events per second. CLI: --fx-rate <n> */
    public int fxRate() {
        return Integer.parseInt(params.get("fx-rate",
            System.getenv().getOrDefault("FX_RATE", "5")));
    }

    /** Number of synthetic accounts seeded into Postgres. CLI: --account-count <n> */
    public int accountCount() {
        return Integer.parseInt(params.get("account-count",
            System.getenv().getOrDefault("ACCOUNT_COUNT", "50")));
    }

    /** Probability (0..1) of dropping an order's fill — drives LEFT-OUTER demos. CLI: --drop-fill-pct */
    public double dropFillPct() {
        return Double.parseDouble(params.get("drop-fill-pct",
            System.getenv().getOrDefault("DROP_FILL_PCT", "0.20")));
    }

    /** Probability of emitting a fill that has no order — drives RIGHT-OUTER demos. CLI: --orphan-fill-pct */
    public double orphanFillPct() {
        return Double.parseDouble(params.get("orphan-fill-pct",
            System.getenv().getOrDefault("ORPHAN_FILL_PCT", "0.05")));
    }
}
