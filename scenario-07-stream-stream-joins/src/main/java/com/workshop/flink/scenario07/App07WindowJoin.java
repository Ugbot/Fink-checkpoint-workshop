package com.workshop.flink.scenario07;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.QuoteEvent;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.serde.JsonRecordSerializationSchema;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSourceFactory;
import com.workshop.flink.scenario07.model.TickerVolumeRow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

/**
 * Scenario 07c — Tumbling Window Join.
 *
 * For every (ticker, 1-minute window), emit a row with the number of trades and
 * the number of quotes that arrived in that window. Trades without quotes (or vice
 * versa) in the window are not joined — this is the cross-product side of a window join.
 *
 * Pedagogical point: a window join emits exactly one cross-product per (window, key),
 * unlike interval joins which can emit multiple pairs over a sliding bound. We
 * pre-aggregate both sides to counts before unioning, so the output stays compact.
 *
 * CLI / env vars:
 *   --kafka              KAFKA_BOOTSTRAP    broker
 *   --window-seconds     WINDOW_SECONDS     window size (default: 60)
 *   --parallelism        S07_PARALLELISM    parallelism (default: 2)
 */
public class App07WindowJoin {

    private static final Logger LOG = LogManager.getLogger(App07WindowJoin.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        int parallelism = params.parallelism("S07_PARALLELISM", 2);
        long windowSec = Long.parseLong(
            System.getenv().getOrDefault("WINDOW_SECONDS", "60"));

        LOG.info("Starting Scenario 07c — Window Join ({}s), parallelism={}, kafka={}",
                 windowSec, parallelism, params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s07-window"));

        WatermarkStrategy<TradeEvent> tradeWms =
            WatermarkStrategy.<TradeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.getTradeTime())
                .withIdleness(Duration.ofSeconds(30));

        WatermarkStrategy<QuoteEvent> quoteWms =
            WatermarkStrategy.<QuoteEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.getQuoteTime())
                .withIdleness(Duration.ofSeconds(30));

        DataStream<TradeEvent> trades = env.fromSource(
            KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_IN, "s07-window-trades"),
            tradeWms,
            "KafkaSource <- " + Constants.TOPIC_IN);

        DataStream<QuoteEvent> quotes = env.fromSource(
            KafkaSourceFactory.quotes(params.kafka(), Constants.TOPIC_QUOTES, "s07-window-quotes"),
            quoteWms,
            "KafkaSource <- " + Constants.TOPIC_QUOTES);

        // Co-window count of trades and quotes per (ticker, window). The DataStream
        // window-join API emits one row per pair, which is too verbose for high cardinalities;
        // instead, aggregate each side independently then key-cogroup by (ticker, windowStart).
        DataStream<Tuple3<String, Long, Long>> tradeCounts = trades
            .keyBy(TradeEvent::getTicker)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSec)))
            .aggregate(new CountAgg<>(), new TupleWindowFn<TradeEvent>("trade"))
            .name(windowSec + "s tumbling trade count");

        DataStream<Tuple3<String, Long, Long>> quoteCounts = quotes
            .keyBy(QuoteEvent::getTicker)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSec)))
            .aggregate(new CountAgg<>(), new TupleWindowFn<QuoteEvent>("quote"))
            .name(windowSec + "s tumbling quote count");

        DataStream<TickerVolumeRow> joined = tradeCounts
            .keyBy(t -> t.f0 + "@" + t.f1)
            .connect(quoteCounts.keyBy(t -> t.f0 + "@" + t.f1))
            .process(new TickerVolumeJoiner(windowSec * 1000L * 2L))
            .name("WindowJoin(trade-counts, quote-counts) by (ticker, windowStart)");

        KafkaSink<TickerVolumeRow> sink = KafkaSink.<TickerVolumeRow>builder()
            .setBootstrapServers(params.kafka())
            .setRecordSerializer(new JsonRecordSerializationSchema<>(
                Constants.TOPIC_ENRICHED_S07,
                row -> row.getTicker() + "@" + row.getWindowStart()))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        joined.sinkTo(sink).name("KafkaSink -> " + Constants.TOPIC_ENRICHED_S07);

        env.execute("S07-WindowJoin-p" + parallelism);
    }

    static class CountAgg<T> implements AggregateFunction<T, Long, Long> {
        private static final long serialVersionUID = 1L;
        @Override public Long createAccumulator()        { return 0L; }
        @Override public Long add(T value, Long acc)     { return acc + 1L; }
        @Override public Long getResult(Long acc)         { return acc; }
        @Override public Long merge(Long a, Long b)       { return a + b; }
    }

    static class TupleWindowFn<T> implements WindowFunction<Long, Tuple3<String, Long, Long>, String, TimeWindow> {
        private static final long serialVersionUID = 1L;
        @SuppressWarnings("unused") private final String side; // for readability/debug
        TupleWindowFn(String side) { this.side = side; }
        @Override
        public void apply(String ticker, TimeWindow w, Iterable<Long> in,
                          Collector<Tuple3<String, Long, Long>> out) {
            long count = in.iterator().next();
            out.collect(new Tuple3<>(ticker, w.getStart(), count));
        }
    }

    /**
     * Co-groups trade-counts and quote-counts that share the same (ticker, windowStart).
     * The first side to arrive is held in state; the second arrival emits the joined row.
     * If only one side arrives before a watermark-driven cleanup, no row is emitted —
     * matching INNER-join semantics for window joins.
     */
    static class TickerVolumeJoiner extends org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction<
            String, Tuple3<String, Long, Long>, Tuple3<String, Long, Long>, TickerVolumeRow> {

        private static final long serialVersionUID = 1L;

        private final long stateTtlMs;
        private transient org.apache.flink.api.common.state.ValueState<Long> tradeCountState;
        private transient org.apache.flink.api.common.state.ValueState<Long> quoteCountState;

        TickerVolumeJoiner(long stateTtlMs) { this.stateTtlMs = stateTtlMs; }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            org.apache.flink.api.common.state.StateTtlConfig ttl =
                org.apache.flink.api.common.state.StateTtlConfig
                    .newBuilder(org.apache.flink.api.common.time.Time.milliseconds(stateTtlMs))
                    .setUpdateType(org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
            org.apache.flink.api.common.state.ValueStateDescriptor<Long> td =
                new org.apache.flink.api.common.state.ValueStateDescriptor<>("trade-cnt", Long.class);
            td.enableTimeToLive(ttl);
            tradeCountState = getRuntimeContext().getState(td);
            org.apache.flink.api.common.state.ValueStateDescriptor<Long> qd =
                new org.apache.flink.api.common.state.ValueStateDescriptor<>("quote-cnt", Long.class);
            qd.enableTimeToLive(ttl);
            quoteCountState = getRuntimeContext().getState(qd);
        }

        @Override
        public void processElement1(Tuple3<String, Long, Long> tradeCnt,
                                    Context ctx,
                                    Collector<TickerVolumeRow> out) throws Exception {
            Long qc = quoteCountState.value();
            if (qc != null) {
                long windowSizeMs = stateTtlMs / 2;
                out.collect(new TickerVolumeRow(tradeCnt.f0, tradeCnt.f1, tradeCnt.f1 + windowSizeMs,
                                                tradeCnt.f2, qc));
                tradeCountState.clear();
                quoteCountState.clear();
            } else {
                tradeCountState.update(tradeCnt.f2);
            }
        }

        @Override
        public void processElement2(Tuple3<String, Long, Long> quoteCnt,
                                    Context ctx,
                                    Collector<TickerVolumeRow> out) throws Exception {
            Long tc = tradeCountState.value();
            if (tc != null) {
                long windowSizeMs = stateTtlMs / 2;
                out.collect(new TickerVolumeRow(quoteCnt.f0, quoteCnt.f1, quoteCnt.f1 + windowSizeMs,
                                                tc, quoteCnt.f2));
                tradeCountState.clear();
                quoteCountState.clear();
            } else {
                quoteCountState.update(quoteCnt.f2);
            }
        }
    }
}
