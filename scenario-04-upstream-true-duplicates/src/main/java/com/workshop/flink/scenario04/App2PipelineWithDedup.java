package com.workshop.flink.scenario04;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSinkFactory;
import com.workshop.flink.common.util.KafkaSourceFactory;
import com.workshop.flink.scenario04.operator.DeduplicationFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.time.Duration;

/**
 * Scenario 04 — App2 With Dedup
 *
 * CLI / env vars:
 *   --kafka             KAFKA_BOOTSTRAP      broker (default: localhost:19092)
 *   --dedup-ttl-hours   DEDUP_TTL_HOURS      dedup state TTL in hours (default: 1)
 *   --checkpoint-dir    CHECKPOINT_DIR       checkpoint path
 */
public class App2PipelineWithDedup {

    private static final Logger LOG = LogManager.getLogger(App2PipelineWithDedup.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        long ttlHours = params.dedupTtlHours();
        LOG.info("Starting Scenario 04 App2 WithDedup — TTL={}h, kafka={}", ttlHours, params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s04-app2-dedup")
        );

        env.fromSource(
                KafkaSourceFactory.readCommitted(params.kafka(), Constants.TOPIC_MID, "s04-app2-dedup"),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource (read_committed) -> " + Constants.TOPIC_MID)
            .keyBy(TradeEvent::getEventId)
            .process(new DeduplicationFunction(ttlHours))
            .name("DeduplicationFunction (TTL=" + ttlHours + "h, keyed by eventId)")
            .keyBy(TradeEvent::getAccountId)
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
            .aggregate(new TradeCountAggregate(), new TradeCountWindowFunction())
            .name("10s tumbling count (AFTER dedup — counts are accurate)")
            .sinkTo(KafkaSinkFactory.exactlyOnce(params.kafka(), Constants.TOPIC_OUT, "s04-app2-dedup"))
            .name("KafkaSink (EXACTLY_ONCE) -> " + Constants.TOPIC_OUT);

        env.execute("S04-App2-WithDedup");
    }

    static class TradeCountAggregate implements AggregateFunction<TradeEvent, long[], Long> {
        @Override public long[] createAccumulator()            { return new long[]{0L}; }
        @Override public long[] add(TradeEvent t, long[] acc) { acc[0]++; return acc; }
        @Override public Long getResult(long[] acc)            { return acc[0]; }
        @Override public long[] merge(long[] a, long[] b)     { a[0] += b[0]; return a; }
    }

    static class TradeCountWindowFunction
            implements WindowFunction<Long, TradeEvent, String, TimeWindow> {
        @Override
        public void apply(String accountId, TimeWindow window,
                          Iterable<Long> input, Collector<TradeEvent> out) {
            long count = input.iterator().next();
            TradeEvent result = TradeEvent.of(accountId, "COUNT", "REPORT-DEDUP", (int) count, 0.0);
            result.setSourceApp("s04-app2-dedup");
            out.collect(result);
        }
    }
}
