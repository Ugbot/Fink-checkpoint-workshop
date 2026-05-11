package com.workshop.flink.scenario01;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSinkFactory;
import com.workshop.flink.common.util.KafkaSourceFactory;
import com.workshop.flink.scenario01.transform.EnrichmentFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Scenario 01 — App1 (AT_LEAST_ONCE)
 *
 * Reads trades from topic.in and writes to topic.mid WITHOUT transactional guarantees.
 * When crashed and restarted, Flink replays records since the last checkpoint, producing
 * duplicate messages in topic.mid that App2 will consume.
 *
 * CLI / env vars:
 *   --kafka             KAFKA_BOOTSTRAP      broker (default: localhost:19092)
 *   --crash-after       CRASH_AFTER_RECORDS  crash after N records (default: disabled)
 *   --checkpoint-dir    CHECKPOINT_DIR       checkpoint path
 */
public class App1Pipeline {

    private static final Logger LOG = LogManager.getLogger(App1Pipeline.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        LOG.info("Starting Scenario 01 App1 — AT_LEAST_ONCE, no transactional sink, kafka={}",
                 params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.AT_LEAST_ONCE,
            params.checkpointDir("s01-app1")
        );

        env.fromSource(
                KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_IN, "s01-app1"),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource -> " + Constants.TOPIC_IN)
            .map(new EnrichmentFunction(params.crashTrigger()))
            .name("EnrichmentFunction (crash-enabled)")
            .sinkTo(KafkaSinkFactory.atLeastOnce(params.kafka(), Constants.TOPIC_MID))
            .name("KafkaSink -> " + Constants.TOPIC_MID);

        env.execute("S01-App1-AtLeastOnce");
    }
}
