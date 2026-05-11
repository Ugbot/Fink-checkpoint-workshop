package com.workshop.flink.scenario04;

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
 * Scenario 04 — App1 (EXACTLY_ONCE pass-through)
 *
 * CLI / env vars:
 *   --kafka             KAFKA_BOOTSTRAP      broker (default: localhost:19092)
 *   --checkpoint-dir    CHECKPOINT_DIR       checkpoint path
 */
public class App1Pipeline {

    private static final Logger LOG = LogManager.getLogger(App1Pipeline.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        LOG.info("Starting Scenario 04 App1 — EXACTLY_ONCE pass-through, kafka={}",
                 params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s04-app1")
        );

        env.fromSource(
                KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_IN, "s04-app1"),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource -> " + Constants.TOPIC_IN)
            .map(event -> {
                event.setSourceApp("s04-app1");
                return event;
            })
            .name("Set sourceApp")
            .sinkTo(KafkaSinkFactory.exactlyOnce(params.kafka(), Constants.TOPIC_MID, "s04-app1"))
            .name("KafkaSink (EXACTLY_ONCE) -> " + Constants.TOPIC_MID);

        env.execute("S04-App1-PassThrough");
    }
}
