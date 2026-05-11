package com.workshop.flink.scenario05;

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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.time.Duration;

/**
 * Scenario 05 — App2 (event-time windowed aggregation)
 *
 * CLI / env vars:
 *   --kafka             KAFKA_BOOTSTRAP      broker (default: localhost:19092)
 *   --parallelism       APP2_PARALLELISM     task parallelism (default: 2)
 *   --checkpoint-dir    CHECKPOINT_DIR       checkpoint path
 */
public class App2Pipeline {

    private static final Logger LOG = LogManager.getLogger(App2Pipeline.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        int parallelism = params.parallelism("APP2_PARALLELISM", 2);

        LOG.info("Starting Scenario 05 App2 — event-time windowed aggregation, parallelism={}, kafka={}",
                 parallelism, params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s05-app2")
        );

        WatermarkStrategy<TradeEvent> watermarkStrategy =
            WatermarkStrategy.<TradeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getTradeTime());

        env.fromSource(
                KafkaSourceFactory.readCommitted(params.kafka(), Constants.TOPIC_MID, "s05-app2"),
                watermarkStrategy,
                "KafkaSource (read_committed) -> " + Constants.TOPIC_MID)
            .keyBy(TradeEvent::getAccountId)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
            .aggregate(new TradeCountAggregate(), new TradeCountWindowFunction())
            .name("30s event-time tumbling count per account")
            .sinkTo(KafkaSinkFactory.exactlyOnce(params.kafka(), Constants.TOPIC_OUT, "s05-app2"))
            .name("KafkaSink (EXACTLY_ONCE) -> " + Constants.TOPIC_OUT);

        env.execute("S05-App2-EventTimeWindows-p" + parallelism);
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
            TradeEvent result = TradeEvent.of(accountId, "COUNT", "REPORT-S05", (int) count, 0.0);
            result.setSourceApp("s05-app2");
            out.collect(result);
        }
    }
}
