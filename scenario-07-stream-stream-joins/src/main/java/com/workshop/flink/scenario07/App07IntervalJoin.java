package com.workshop.flink.scenario07;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.QuoteEvent;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.serde.JsonRecordSerializationSchema;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSourceFactory;
import com.workshop.flink.scenario07.model.TradeWithQuote;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

/**
 * Scenario 07b — Interval Join (time-bounded stream-stream).
 *
 * For every trade, find quotes for the same ticker whose quoteTime is within ±5s of the
 * trade's tradeTime. Each (trade, quote) pair within the bound becomes one TradeWithQuote
 * output row. Quotes outside the window are automatically dropped from join state.
 *
 * Watermark strategy: 5-second bounded out-of-orderness on both streams. The join's
 * effective window depends on the slower watermark — see the SQL notes for the
 * equivalent BETWEEN clause.
 *
 * CLI / env vars:
 *   --kafka              KAFKA_BOOTSTRAP     broker
 *   --interval-seconds   INTERVAL_SECONDS    ±bound in seconds (default: 5)
 *   --parallelism        S07_PARALLELISM     parallelism (default: 2)
 */
public class App07IntervalJoin {

    private static final Logger LOG = LogManager.getLogger(App07IntervalJoin.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        int parallelism = params.parallelism("S07_PARALLELISM", 2);
        long intervalSec = Long.parseLong(
            System.getenv().getOrDefault("INTERVAL_SECONDS", "5"));

        LOG.info("Starting Scenario 07b — Interval Join ±{}s, parallelism={}, kafka={}",
                 intervalSec, parallelism, params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s07-interval"));

        WatermarkStrategy<TradeEvent> tradeWms =
            WatermarkStrategy.<TradeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.getTradeTime())
                .withIdleness(Duration.ofSeconds(30));

        WatermarkStrategy<QuoteEvent> quoteWms =
            WatermarkStrategy.<QuoteEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.getQuoteTime())
                .withIdleness(Duration.ofSeconds(30));

        DataStream<TradeEvent> trades = env.fromSource(
            KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_IN, "s07-interval-trades"),
            tradeWms,
            "KafkaSource <- " + Constants.TOPIC_IN);

        DataStream<QuoteEvent> quotes = env.fromSource(
            KafkaSourceFactory.quotes(params.kafka(), Constants.TOPIC_QUOTES, "s07-interval-quotes"),
            quoteWms,
            "KafkaSource <- " + Constants.TOPIC_QUOTES);

        DataStream<TradeWithQuote> joined = trades
            .keyBy(TradeEvent::getTicker)
            .intervalJoin(quotes.keyBy(QuoteEvent::getTicker))
            .between(Duration.ofSeconds(-intervalSec), Duration.ofSeconds(intervalSec))
            .process(new ProcessJoinFunction<TradeEvent, QuoteEvent, TradeWithQuote>() {
                private static final long serialVersionUID = 1L;
                @Override
                public void processElement(TradeEvent trade,
                                           QuoteEvent quote,
                                           Context ctx,
                                           Collector<TradeWithQuote> out) {
                    out.collect(new TradeWithQuote(trade, quote));
                }
            })
            .name("IntervalJoin(trade, quote ±" + intervalSec + "s) by ticker");

        KafkaSink<TradeWithQuote> sink = KafkaSink.<TradeWithQuote>builder()
            .setBootstrapServers(params.kafka())
            .setRecordSerializer(new JsonRecordSerializationSchema<>(
                Constants.TOPIC_ENRICHED_S07,
                t -> t.getTrade().getEventId()))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        joined.sinkTo(sink).name("KafkaSink -> " + Constants.TOPIC_ENRICHED_S07);

        env.execute("S07-IntervalJoin-p" + parallelism);
    }
}
