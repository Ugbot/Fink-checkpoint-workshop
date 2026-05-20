package com.workshop.flink.scenario08;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.serde.JsonRecordSerializationSchema;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSourceFactory;
import com.workshop.flink.scenario08.model.TradeWithAccount;
import com.workshop.flink.scenario08.operator.AsyncAccountLookupFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Scenario 08a — Async Lookup Join (Postgres `accounts`).
 *
 * For every trade, asynchronously fetch the matching AccountInfo row from Postgres.
 * Caching: a per-task Caffeine LRU caps DB load. Tunables via env vars below.
 *
 * Why this is different from a temporal join: the lookup happens at *processing*
 * time. There is no notion of "the account row as of trade.tradeTime." If the
 * accounts table is mutated mid-stream, future trades pick up the new value (after
 * the cache entry expires); older trades that were already enriched do not.
 *
 * CLI / env vars:
 *   --kafka              KAFKA_BOOTSTRAP   broker
 *   --pg-url             PG_URL            JDBC URL
 *   --pg-user            PG_USER
 *   --pg-password        PG_PASSWORD
 *   --parallelism        S08_PARALLELISM   parallelism (default: 2)
 *   LOOKUP_CACHE_MAX                       cache size (default: 1000)
 *   LOOKUP_CACHE_TTL_SEC                   cache TTL in seconds (default: 300)
 *   LOOKUP_ASYNC_THREADS                   pool size per task (default: 8)
 *   LOOKUP_TIMEOUT_MS                      per-call timeout (default: 5000)
 *   LOOKUP_CAPACITY                        max concurrent in-flight (default: 100)
 */
public class App08LookupJoin {

    private static final Logger LOG = LogManager.getLogger(App08LookupJoin.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        int parallelism = params.parallelism("S08_PARALLELISM", 2);

        int  cacheMax       = Integer.parseInt(System.getenv().getOrDefault("LOOKUP_CACHE_MAX", "1000"));
        long cacheTtlSec    = Long.parseLong(System.getenv().getOrDefault("LOOKUP_CACHE_TTL_SEC", "300"));
        int  asyncThreads   = Integer.parseInt(System.getenv().getOrDefault("LOOKUP_ASYNC_THREADS", "8"));
        long timeoutMs      = Long.parseLong(System.getenv().getOrDefault("LOOKUP_TIMEOUT_MS", "5000"));
        int  capacity       = Integer.parseInt(System.getenv().getOrDefault("LOOKUP_CAPACITY", "100"));

        LOG.info("Starting Scenario 08a — Async Lookup Join, p={}, cache(max={},ttlSec={}), threads={}, " +
                 "timeoutMs={}, capacity={}, pg={}",
                 parallelism, cacheMax, cacheTtlSec, asyncThreads, timeoutMs, capacity, params.pgUrl());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s08-lookup"));

        DataStream<TradeEvent> trades = env.fromSource(
            KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_IN, "s08-lookup"),
            WatermarkStrategy.noWatermarks(),
            "KafkaSource <- " + Constants.TOPIC_IN);

        AsyncAccountLookupFunction lookupFn = new AsyncAccountLookupFunction(
            params.pgUrl(), params.pgUser(), params.pgPassword(),
            cacheMax, cacheTtlSec, asyncThreads);

        DataStream<TradeWithAccount> enriched = AsyncDataStream.unorderedWait(
            trades, lookupFn, timeoutMs, TimeUnit.MILLISECONDS, capacity)
            .name("AsyncLookup(accounts) by accountId");

        KafkaSink<TradeWithAccount> sink = KafkaSink.<TradeWithAccount>builder()
            .setBootstrapServers(params.kafka())
            .setRecordSerializer(new JsonRecordSerializationSchema<>(
                Constants.TOPIC_ENRICHED_S08,
                row -> row.getTrade().getEventId()))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        enriched.sinkTo(sink).name("KafkaSink -> " + Constants.TOPIC_ENRICHED_S08);

        env.execute("S08-LookupJoin-p" + parallelism);
    }
}
