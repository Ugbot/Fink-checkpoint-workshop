package com.workshop.flink.scenario09;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.FxRate;
import com.workshop.flink.common.model.QuoteEvent;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.serde.JsonRecordSerializationSchema;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.CrashTrigger;
import com.workshop.flink.common.util.CurrencyOf;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSourceFactory;
import com.workshop.flink.scenario07.model.TradeWithQuote;
import com.workshop.flink.scenario09.model.EnrichedTrade;
import com.workshop.flink.scenario09.operator.AccountLookupAndCrash;
import com.workshop.flink.scenario09.operator.TradeWithQuoteFxJoin;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Scenario 09 — Multi-Way Join + Failure Recovery (capstone).
 *
 * Topology:
 *   trades      ─┐
 *                ├─(interval join, ticker, ±5s)──► tradeWithQuote ──┐
 *   quotes      ─┘                                                  │
 *                                                                   ├─(temporal join, currency, AS OF tradeTime)─┐
 *   fx_rates    ─────────────────────────────────────────────────────┘                                            │
 *                                                                                                                 ├─(async lookup, accountId)──► enriched ──► topic.enriched.s09
 *   accounts(PG) ───────────────────────────────────────────────────────────────────────────────────────────────────┘
 *
 * Failure injection: CRASH_AFTER_RECORDS environment variable triggers a crash N
 * enriched records into the run (transient counter, resets on restart).
 *
 * EXACTLY_ONCE Kafka sink + read_committed consumer downstream → zero duplicates on
 * topic.enriched.s09 even after the deliberate crash.
 *
 * CLI / env vars:
 *   --kafka              KAFKA_BOOTSTRAP        broker
 *   --pg-url / user / password                  Postgres (accounts table)
 *   --crash-after        CRASH_AFTER_RECORDS    crash threshold (default: disabled)
 *   --parallelism        S09_PARALLELISM        parallelism (default: 2)
 *   INTERVAL_SECONDS                            ±bound for interval join (default: 5)
 *   FX_HISTORY_RETAIN_MS                        FX state retention (default: 600000)
 *   FX_MAX_PENDING_MS                           max wait for covering FX rate (default: 60000)
 *   LOOKUP_CACHE_MAX                            lookup cache rows (default: 1000)
 *   LOOKUP_CACHE_TTL_SEC                        lookup cache TTL (default: 300)
 *   LOOKUP_ASYNC_THREADS                        lookup pool size (default: 8)
 *   LOOKUP_TIMEOUT_MS                           lookup timeout (default: 5000)
 *   LOOKUP_CAPACITY                             concurrent in-flight (default: 100)
 */
public class App09MultiWayJoin {

    private static final Logger LOG = LogManager.getLogger(App09MultiWayJoin.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        int parallelism = params.parallelism("S09_PARALLELISM", 2);

        long intervalSec = Long.parseLong(System.getenv().getOrDefault("INTERVAL_SECONDS",  "5"));
        long fxRetainMs  = Long.parseLong(System.getenv().getOrDefault("FX_HISTORY_RETAIN_MS", "600000"));
        long fxPendMs    = Long.parseLong(System.getenv().getOrDefault("FX_MAX_PENDING_MS",   "60000"));

        int  cacheMax     = Integer.parseInt(System.getenv().getOrDefault("LOOKUP_CACHE_MAX", "1000"));
        long cacheTtlSec  = Long.parseLong(System.getenv().getOrDefault("LOOKUP_CACHE_TTL_SEC", "300"));
        int  asyncThreads = Integer.parseInt(System.getenv().getOrDefault("LOOKUP_ASYNC_THREADS", "8"));
        long timeoutMs    = Long.parseLong(System.getenv().getOrDefault("LOOKUP_TIMEOUT_MS", "5000"));
        int  capacity     = Integer.parseInt(System.getenv().getOrDefault("LOOKUP_CAPACITY", "100"));

        CrashTrigger crashTrigger = params.crashTrigger();

        LOG.info("Starting Scenario 09 — Multi-Way Join, p={}, intervalSec={}, fxRetain={}ms, fxPend={}ms, " +
                 "lookup(cache={}, ttl={}s, threads={}, timeout={}ms, cap={}), kafka={}",
                 parallelism, intervalSec, fxRetainMs, fxPendMs,
                 cacheMax, cacheTtlSec, asyncThreads, timeoutMs, capacity,
                 params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s09-multiway"));

        // ── Sources with watermarks ───────────────────────────────────────────────
        WatermarkStrategy<TradeEvent> tradeWms =
            WatermarkStrategy.<TradeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.getTradeTime())
                .withIdleness(Duration.ofSeconds(30));

        WatermarkStrategy<QuoteEvent> quoteWms =
            WatermarkStrategy.<QuoteEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.getQuoteTime())
                .withIdleness(Duration.ofSeconds(30));

        WatermarkStrategy<FxRate> fxWms =
            WatermarkStrategy.<FxRate>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((e, ts) -> e.getRateTime())
                .withIdleness(Duration.ofSeconds(30));

        DataStream<TradeEvent> trades = env.fromSource(
            KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_IN, "s09-trades"),
            tradeWms, "KafkaSource <- " + Constants.TOPIC_IN);

        DataStream<QuoteEvent> quotes = env.fromSource(
            KafkaSourceFactory.quotes(params.kafka(), Constants.TOPIC_QUOTES, "s09-quotes"),
            quoteWms, "KafkaSource <- " + Constants.TOPIC_QUOTES);

        DataStream<FxRate> fxRates = env.fromSource(
            KafkaSourceFactory.fxRates(params.kafka(), Constants.TOPIC_FXRATES, "s09-fx"),
            fxWms, "KafkaSource <- " + Constants.TOPIC_FXRATES);

        // ── 1. Interval join: trades ⋈ quotes by ticker, ±5s ──────────────────────
        DataStream<TradeWithQuote> tradeWithQuote = trades
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

        // ── 2. Temporal join: tradeWithQuote ⋈ fx_rates AS OF tradeTime ───────────
        DataStream<EnrichedTrade> tradeQuoteFx = tradeWithQuote
            .keyBy(twq -> CurrencyOf.fromAccountId(twq.getTrade().getAccountId()))
            .connect(fxRates.keyBy(FxRate::getCurrency))
            .process(new TradeWithQuoteFxJoin(fxRetainMs, fxPendMs))
            .name("TemporalJoin(trade+quote, fx AS OF tradeTime) by currency");

        // ── 3. Async lookup join: account info from Postgres (+ crash trigger) ────
        AccountLookupAndCrash lookupAndCrash = new AccountLookupAndCrash(
            params.pgUrl(), params.pgUser(), params.pgPassword(),
            cacheMax, cacheTtlSec, asyncThreads, crashTrigger);

        DataStream<EnrichedTrade> enriched = AsyncDataStream.unorderedWait(
            tradeQuoteFx, lookupAndCrash, timeoutMs, TimeUnit.MILLISECONDS, capacity)
            .name("AsyncLookup(accounts) + CrashTrigger");

        // ── 4. EXACTLY_ONCE Kafka sink → topic.enriched.s09 ───────────────────────
        KafkaSink<EnrichedTrade> sink = KafkaSink.<EnrichedTrade>builder()
            .setBootstrapServers(params.kafka())
            .setRecordSerializer(new JsonRecordSerializationSchema<>(
                Constants.TOPIC_ENRICHED_S09,
                row -> row.getTrade().getEventId()))
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix("s09-multiway")
            .build();

        enriched.sinkTo(sink).name("KafkaSink (EXACTLY_ONCE) -> " + Constants.TOPIC_ENRICHED_S09);

        env.execute("S09-MultiWayJoin-p" + parallelism);
    }
}
