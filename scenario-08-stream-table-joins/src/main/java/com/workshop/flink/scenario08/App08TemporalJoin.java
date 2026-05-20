package com.workshop.flink.scenario08;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.FxRate;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.serde.JsonRecordSerializationSchema;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.CurrencyOf;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSourceFactory;
import com.workshop.flink.scenario08.model.TradeWithFx;
import com.workshop.flink.scenario08.operator.VersionedFxJoin;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

/**
 * Scenario 08b — Temporal / Versioned Join (Trades AS OF FX rate at tradeTime).
 *
 * Both streams are keyed by currency. The trade's currency is derived from its
 * accountId via {@link CurrencyOf}. The versioned-fx operator maintains a per-currency
 * history of rates and answers "what was the rate at trade.tradeTime?" for every trade.
 *
 * Pedagogical contrast with App08LookupJoin:
 *   - Lookup join (08a) → answers "current account row" at processing time
 *   - Temporal join (08b) → answers "rate as of event time" — stable under replay
 *
 * CLI / env vars:
 *   --kafka              KAFKA_BOOTSTRAP    broker
 *   --parallelism        S08_PARALLELISM    parallelism (default: 2)
 *   FX_HISTORY_RETAIN_MS                   keep rate history this long behind watermark
 *                                          (default: 600000 = 10 minutes)
 *   FX_MAX_PENDING_MS                      max wait for a covering rate (default: 60000)
 */
public class App08TemporalJoin {

    private static final Logger LOG = LogManager.getLogger(App08TemporalJoin.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        int parallelism = params.parallelism("S08_PARALLELISM", 2);

        long retainMs = Long.parseLong(System.getenv().getOrDefault("FX_HISTORY_RETAIN_MS", "600000"));
        long pendMs   = Long.parseLong(System.getenv().getOrDefault("FX_MAX_PENDING_MS",   "60000"));

        LOG.info("Starting Scenario 08b — Temporal FX Join, p={}, retainMs={}, pendingMs={}, kafka={}",
                 parallelism, retainMs, pendMs, params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s08-temporal"));

        WatermarkStrategy<TradeEvent> tradeWms =
            WatermarkStrategy.<TradeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.getTradeTime())
                .withIdleness(Duration.ofSeconds(30));

        WatermarkStrategy<FxRate> fxWms =
            WatermarkStrategy.<FxRate>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((e, ts) -> e.getRateTime())
                .withIdleness(Duration.ofSeconds(30));

        DataStream<TradeEvent> trades = env.fromSource(
            KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_IN, "s08-temporal-trades"),
            tradeWms,
            "KafkaSource <- " + Constants.TOPIC_IN);

        DataStream<FxRate> fxRates = env.fromSource(
            KafkaSourceFactory.fxRates(params.kafka(), Constants.TOPIC_FXRATES, "s08-temporal-fx"),
            fxWms,
            "KafkaSource <- " + Constants.TOPIC_FXRATES);

        DataStream<TradeWithFx> enriched = trades
            .keyBy(t -> CurrencyOf.fromAccountId(t.getAccountId()))
            .connect(fxRates.keyBy(FxRate::getCurrency))
            .process(new VersionedFxJoin(retainMs, pendMs))
            .name("TemporalJoin(trade AS OF fx) by currency");

        KafkaSink<TradeWithFx> sink = KafkaSink.<TradeWithFx>builder()
            .setBootstrapServers(params.kafka())
            .setRecordSerializer(new JsonRecordSerializationSchema<>(
                Constants.TOPIC_ENRICHED_S08,
                row -> row.getTrade().getEventId()))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        enriched.sinkTo(sink).name("KafkaSink -> " + Constants.TOPIC_ENRICHED_S08);

        env.execute("S08-TemporalJoin-p" + parallelism);
    }
}
