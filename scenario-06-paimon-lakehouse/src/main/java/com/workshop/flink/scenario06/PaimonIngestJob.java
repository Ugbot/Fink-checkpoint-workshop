package com.workshop.flink.scenario06;

import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.util.CheckpointConfigurator;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Scenario 06 — Kafka -> Paimon ingest.
 *
 * Reads {@link TradeEvent} JSON from Kafka via the shared {@link KafkaSourceFactory},
 * wraps the stream as a Table API view, and INSERTs into a Paimon primary-key table
 * stored on MinIO (s3a://paimon/warehouse).
 *
 * EXACTLY_ONCE checkpointing is required: Paimon only publishes a snapshot at
 * checkpoint barriers, so the checkpoint interval (10s, set by
 * {@link CheckpointConfigurator}) also defines the query visibility latency.
 */
public class PaimonIngestJob {

    private static final Logger LOG = LogManager.getLogger(PaimonIngestJob.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        LOG.info("Starting Scenario 06 — Kafka -> Paimon ingest. kafka={}, warehouse={}, endpoint={}",
            params.kafka(), PaimonConfig.warehouse(), PaimonConfig.s3Endpoint());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfigurator.configure(
            env,
            CheckpointingMode.EXACTLY_ONCE,
            params.checkpointDir("s06-ingest"));

        DataStream<TradeEvent> trades = env.fromSource(
            KafkaSourceFactory.readCommitted(
                params.kafka(),
                PaimonConfig.TOPIC,
                "scenario06-ingest"),
            WatermarkStrategy.noWatermarks(),
            "KafkaSource -> " + PaimonConfig.TOPIC);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Catalog + table DDL are idempotent; safe to re-run on every job submission.
        tEnv.executeSql(PaimonConfig.createCatalogSql());
        tEnv.executeSql(PaimonConfig.useCatalogSql());
        tEnv.executeSql(PaimonConfig.createDatabaseSql());
        tEnv.executeSql("USE " + PaimonConfig.DATABASE);
        tEnv.executeSql(PaimonConfig.createTableSql());

        // Expose the Kafka DataStream as a temp view in the default_catalog so we can
        // SELECT FROM it across the catalog boundary into Paimon.
        tEnv.createTemporaryView(
            "default_catalog.default_database.kafka_trades",
            tEnv.fromDataStream(
                trades,
                Schema.newBuilder()
                    .column("eventId",   "STRING")
                    .column("accountId", "STRING")
                    .column("ticker",    "STRING")
                    .column("side",      "STRING")
                    .column("quantity",  "INT")
                    .column("price",     "DOUBLE")
                    .column("tradeTime", "BIGINT")
                    .column("sourceApp", "STRING")
                    .build()));

        String insert = String.join("\n",
            "INSERT INTO " + PaimonConfig.CATALOG + "." + PaimonConfig.DATABASE + "." + PaimonConfig.TABLE,
            "SELECT",
            "  eventId,",
            "  accountId,",
            "  ticker,",
            "  side,",
            "  quantity,",
            "  price,",
            "  TO_TIMESTAMP_LTZ(tradeTime, 3),",
            "  sourceApp",
            "FROM default_catalog.default_database.kafka_trades");

        LOG.info("Submitting INSERT pipeline:\n{}", insert);
        tEnv.executeSql(insert);
        // executeSql submits a detached job in StreamTableEnvironment; no env.execute() needed.
    }
}
