package com.workshop.flink.scenario01;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSinkFactory;
import com.workshop.flink.common.util.KafkaSourceFactory;
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
 * Scenario 01 — App2 (AT_LEAST_ONCE, no dedup)
 *
 * Reads from topic.mid and counts trades per account in 10-second tumbling windows.
 * Because there is no deduplication and App1 produces duplicates after a crash,
 * the counts in topic.out will be inflated.
 *
 * CLI / env vars:
 *   --kafka             KAFKA_BOOTSTRAP      broker (default: localhost:19092)
 *   --crash-after       CRASH_AFTER_RECORDS  optional crash for App2 itself
 *   --checkpoint-dir    CHECKPOINT_DIR       checkpoint path
 */
public class App2Pipeline {

    private static final Logger LOG = LogManager.getLogger(App2Pipeline.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        LOG.info("Starting Scenario 01 App2 — AT_LEAST_ONCE, no dedup, kafka={}",
                 params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.AT_LEAST_ONCE,
            params.checkpointDir("s01-app2")
        );

        env.fromSource(
                KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_MID, "s01-app2"),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource -> " + Constants.TOPIC_MID)
            .keyBy(TradeEvent::getAccountId)
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
            .aggregate(new TradeCountAggregate(), new TradeCountWindowFunction())
            .name("10s tumbling count per account (no dedup)")
            .sinkTo(KafkaSinkFactory.atLeastOnce(params.kafka(), Constants.TOPIC_OUT))
            .name("KafkaSink -> " + Constants.TOPIC_OUT);

        env.execute("S01-App2-AtLeastOnce");
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
            TradeEvent result = TradeEvent.of(accountId, "COUNT", "REPORT", (int) count, 0.0);
            result.setSourceApp("s01-app2");
            out.collect(result);
        }
    }
}
