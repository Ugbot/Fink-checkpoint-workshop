package com.workshop.flink.scenario03;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSinkFactory;
import com.workshop.flink.common.util.KafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Scenario 03 — App1 (EXACTLY_ONCE, identical to scenario-02 App1)
 *
 * CLI / env vars:
 *   --kafka             KAFKA_BOOTSTRAP      broker (default: localhost:19092)
 *   --crash-after       CRASH_AFTER_RECORDS  crash after N records
 *   --checkpoint-dir    CHECKPOINT_DIR       checkpoint path
 */
public class App1Pipeline {

    private static final Logger LOG = LogManager.getLogger(App1Pipeline.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        LOG.info("Starting Scenario 03 App1 — EXACTLY_ONCE Kafka, kafka={}", params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s03-app1")
        );

        final var crashTrigger = params.crashTrigger();
        env.fromSource(
                KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_IN, "s03-app1"),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource -> " + Constants.TOPIC_IN)
            .map(event -> {
                crashTrigger.increment();
                event.setSourceApp("s03-app1");
                return event;
            })
            .name("Enrich + optional crash")
            .sinkTo(KafkaSinkFactory.exactlyOnce(params.kafka(), Constants.TOPIC_MID, "s03-app1"))
            .name("KafkaSink (EXACTLY_ONCE) -> " + Constants.TOPIC_MID);

        env.execute("S03-App1-ExactlyOnce");
    }
}
