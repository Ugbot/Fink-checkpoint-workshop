package com.workshop.flink.scenario05;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSinkFactory;
import com.workshop.flink.common.util.KafkaSourceFactory;
import com.workshop.flink.scenario05.transform.StatefulEnrichmentFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Scenario 05 — App1 (stateful, parallelism configurable)
 *
 * CLI / env vars:
 *   --kafka             KAFKA_BOOTSTRAP      broker (default: localhost:19092)
 *   --parallelism       APP1_PARALLELISM     task parallelism (default: 2)
 *   --checkpoint-dir    CHECKPOINT_DIR       checkpoint path
 *
 * Note: in cluster mode prefer `flink run -p N` over --parallelism since Flink's
 * built-in -p flag takes precedence. Use --parallelism only for local mode.
 */
public class App1Pipeline {

    private static final Logger LOG = LogManager.getLogger(App1Pipeline.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        int parallelism = params.parallelism("APP1_PARALLELISM", 2);

        LOG.info("Starting Scenario 05 App1 — stateful enrichment, parallelism={}, kafka={}",
                 parallelism, params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s05-app1")
        );

        env.fromSource(
                KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_IN, "s05-app1"),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource -> " + Constants.TOPIC_IN)
            .keyBy(TradeEvent::getAccountId)
            .flatMap(new StatefulEnrichmentFunction())
            .name("StatefulEnrichmentFunction (per-account count in ValueState)")
            .sinkTo(KafkaSinkFactory.exactlyOnce(params.kafka(), Constants.TOPIC_MID, "s05-app1"))
            .name("KafkaSink (EXACTLY_ONCE) -> " + Constants.TOPIC_MID);

        env.execute("S05-App1-Stateful-p" + parallelism);
    }
}
