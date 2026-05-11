package com.workshop.flink.scenario02;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSinkFactory;
import com.workshop.flink.common.util.KafkaSourceFactory;
import com.workshop.flink.scenario02.transform.EnrichmentFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Scenario 02 — App1 (EXACTLY_ONCE)
 *
 * Uses a Kafka transactional sink. Flink's two-phase commit protocol holds the Kafka
 * transaction open until the checkpoint succeeds. If App1 crashes before the checkpoint
 * completes, the in-flight Kafka transaction is aborted by the broker after
 * transaction.timeout.ms, and the records are never made visible to read_committed consumers.
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
        LOG.info("Starting Scenario 02 App1 — EXACTLY_ONCE transactional Kafka sink, kafka={}",
                 params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s02-app1")
        );

        env.fromSource(
                KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_IN, "s02-app1"),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource -> " + Constants.TOPIC_IN)
            .map(new EnrichmentFunction(params.crashTrigger()))
            .name("EnrichmentFunction (crash-enabled)")
            .sinkTo(KafkaSinkFactory.exactlyOnce(params.kafka(), Constants.TOPIC_MID, "s02-app1"))
            .name("KafkaSink (EXACTLY_ONCE) -> " + Constants.TOPIC_MID);

        env.execute("S02-App1-ExactlyOnce");
    }
}
