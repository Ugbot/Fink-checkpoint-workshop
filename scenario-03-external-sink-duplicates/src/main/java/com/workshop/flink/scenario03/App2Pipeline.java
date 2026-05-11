package com.workshop.flink.scenario03;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSourceFactory;
import com.workshop.flink.scenario03.sink.PostgresInsertSink;
import com.workshop.flink.scenario03.sink.PostgresUpsertSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Scenario 03 — App2 (EXACTLY_ONCE Kafka, but external JDBC sink is NOT covered)
 *
 * CLI / env vars:
 *   --kafka             KAFKA_BOOTSTRAP      broker (default: localhost:19092)
 *   --sink-mode         SINK_MODE            "buggy" (default) or "fixed"
 *   --crash-after       CRASH_AFTER_RECORDS  crash after N sink invocations
 *   --checkpoint-dir    CHECKPOINT_DIR       checkpoint path
 *   --pg-url            PG_URL               JDBC URL (default: localhost:15432)
 *   --pg-user           PG_USER              PostgreSQL user (default: workshop)
 *   --pg-password       PG_PASSWORD          PostgreSQL password (default: workshop)
 */
public class App2Pipeline {

    private static final Logger LOG = LogManager.getLogger(App2Pipeline.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        String sinkMode = params.sinkMode();
        LOG.info("Starting Scenario 03 App2 — SINK_MODE={}, kafka={}", sinkMode, params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s03-app2")
        );

        var stream = env.fromSource(
                KafkaSourceFactory.readCommitted(params.kafka(), Constants.TOPIC_MID, "s03-app2"),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource (read_committed) -> " + Constants.TOPIC_MID)
            .map(event -> {
                event.setSourceApp("s03-app2");
                return event;
            })
            .name("Enrich");

        if ("fixed".equalsIgnoreCase(sinkMode)) {
            stream.addSink(new PostgresUpsertSink(params.pgUrl(), params.pgUser(), params.pgPassword()))
                  .name("PostgresUpsertSink (FIXED — idempotent ON CONFLICT)");
        } else {
            stream.addSink(new PostgresInsertSink(
                    params.crashTrigger(), params.pgUrl(), params.pgUser(), params.pgPassword()))
                  .name("PostgresInsertSink (BUGGY — plain INSERT, will duplicate on replay)");
        }

        env.execute("S03-App2-ExternalSink-" + sinkMode);
    }
}
