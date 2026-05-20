package com.workshop.flink.scenario07;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.serde.JsonRecordSerializationSchema;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSourceFactory;
import com.workshop.flink.scenario07.model.JoinedOrderFill;
import com.workshop.flink.scenario07.operator.OrderFillRegularJoin;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Scenario 07a — Regular Stream-Stream Join
 *
 * Joins `topic.orders` ⋈ `topic.fills` by eventId. Emits a single output stream where each
 * record is tagged with one of INNER / LEFT_ORPHAN / RIGHT_ORPHAN — exposing all four
 * SQL join kinds (INNER, LEFT, RIGHT, FULL OUTER) from one job.
 *
 * State retention is bounded by:
 *   1. A 30-second processing-time timer per key (orphan emission deadline)
 *   2. A 1-hour StateTtlConfig fallback in case the timer is somehow skipped
 *
 * CLI / env vars:
 *   --kafka              KAFKA_BOOTSTRAP     broker (default: localhost:19092)
 *   --orphan-timeout-ms  ORPHAN_TIMEOUT_MS   orphan emission deadline (default: 30000)
 *   --parallelism        S07_PARALLELISM     task parallelism (default: 2)
 */
public class App07RegularJoin {

    private static final Logger LOG = LogManager.getLogger(App07RegularJoin.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        int parallelism = params.parallelism("S07_PARALLELISM", 2);
        long orphanTimeoutMs = Long.parseLong(
            System.getenv().getOrDefault("ORPHAN_TIMEOUT_MS", "30000"));

        LOG.info("Starting Scenario 07a — Regular Join, parallelism={}, orphanTimeoutMs={}, kafka={}",
                 parallelism, orphanTimeoutMs, params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s07-regular"));

        DataStream<TradeEvent> orders = env.fromSource(
            KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_ORDERS, "s07-regular-orders"),
            WatermarkStrategy.noWatermarks(),
            "KafkaSource <- " + Constants.TOPIC_ORDERS);

        DataStream<TradeEvent> fills = env.fromSource(
            KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_FILLS, "s07-regular-fills"),
            WatermarkStrategy.noWatermarks(),
            "KafkaSource <- " + Constants.TOPIC_FILLS);

        DataStream<JoinedOrderFill> joined = orders
            .keyBy(TradeEvent::getEventId)
            .connect(fills.keyBy(TradeEvent::getEventId))
            .process(new OrderFillRegularJoin(orphanTimeoutMs, /*stateTtlHours=*/ 1L))
            .name("RegularJoin(orders, fills) by eventId");

        KafkaSink<JoinedOrderFill> sink = KafkaSink.<JoinedOrderFill>builder()
            .setBootstrapServers(params.kafka())
            .setRecordSerializer(new JsonRecordSerializationSchema<>(
                Constants.TOPIC_ENRICHED_S07,
                JoinedOrderFill::getEventId))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        joined.sinkTo(sink).name("KafkaSink -> " + Constants.TOPIC_ENRICHED_S07);

        env.execute("S07-RegularJoin-p" + parallelism);
    }
}
