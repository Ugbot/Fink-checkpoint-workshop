package com.workshop.flink.scenario15;

import com.workshop.flink.common.fluss.FlussTables;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Scenario 15 — Flink-native shape for Fluss lookups.
 *
 * <p>Reads trades from Kafka ({@code topic.in}, the workshop's standard
 * source), joins each trade against the Fluss {@code instrument_master}
 * PK table via Flink SQL's lookup-join, and emits the enriched row to a
 * blackhole sink (swap for a Kafka/Paimon sink in production).
 *
 * <p>This is the Flink-native shape of scenario 15: the Fluss connector
 * is used inside Flink as a lookup source. Compare with the SDK-only
 * variant ({@link FlussPointInTimeLookupClient}) which does the same
 * lookups directly without Flink in the loop.
 *
 * <p>Env vars: {@code KAFKA_BOOTSTRAP}, {@code FLUSS_BOOTSTRAP}.
 */
public class FlussFlinkConnectorEnrichmentJob {

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "workshop-kafka:9093");
        String flussBootstrap = FlussTables.bootstrap();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10_000L);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // ── Register Fluss catalog ──────────────────────────────────────────
        tEnv.executeSql(
                "CREATE CATALOG IF NOT EXISTS fluss WITH (" +
                "  'type' = 'fluss'," +
                "  'bootstrap.servers' = '" + flussBootstrap + "'" +
                ")");

        // ── Kafka source: standard workshop topic.in trade events ───────────
        KafkaSource<TradeJson> source = KafkaSource.<TradeJson>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("topic.in")
                .setGroupId("scenario-15-flink-enrichment")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new TradeJsonDeserializer())
                .build();
        DataStream<TradeJson> trades = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "kafka-trades");

        // Convert the DataStream to a Table with declared schema so Flink SQL
        // can join it against the Fluss PK table.
        DataStream<Row> rows = trades.map(t -> Row.of(t.eventId, t.accountId, t.ticker, t.quantity, t.price))
                .returns(org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
                        new String[]{"event_id", "account_id", "ticker", "quantity", "price"},
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.INT,
                        org.apache.flink.api.common.typeinfo.Types.DOUBLE));

        tEnv.createTemporaryView("kafka_trades", tEnv.fromDataStream(rows,
                Schema.newBuilder()
                        .column("event_id",   "STRING")
                        .column("account_id", "STRING")
                        .column("ticker",     "STRING")
                        .column("quantity",   "INT")
                        .column("price",      "DOUBLE")
                        .columnByExpression("proc_time", "PROCTIME()")
                        .build()));

        // ── Blackhole sink for the enriched stream ──────────────────────────
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE blackhole_sink (" +
                "  event_id STRING," +
                "  ticker STRING," +
                "  sector STRING," +
                "  last_close DOUBLE," +
                "  beta_1y DOUBLE," +
                "  quantity INT," +
                "  notional DOUBLE" +
                ") WITH ('connector' = 'blackhole')");

        // ── The lookup join — uses the Fluss connector's lookup source ──────
        // Note: only projects 4 columns of instrument_master, so the Fluss
        // lookup fetches just those off the right side. Zero in-Flink state
        // for the join's right side; Fluss serves each lookup on demand.
        tEnv.executeSql(
                "INSERT INTO blackhole_sink " +
                "SELECT t.event_id, t.ticker, im.sector, im.last_close, im.beta_1y, " +
                "       t.quantity, CAST(t.quantity AS DOUBLE) * t.price AS notional " +
                "FROM kafka_trades AS t " +
                "LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im " +
                "  ON t.ticker = im.ticker");

        env.execute("FlussFlinkConnectorEnrichmentJob");
    }

    /** Minimal POJO for a deserialized trade JSON message. */
    public static class TradeJson implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public String eventId;
        public String accountId;
        public String ticker;
        public int    quantity;
        public double price;
    }

    /** Jackson-based deserializer for {@code topic.in} JSON payloads. */
    public static class TradeJsonDeserializer extends AbstractDeserializationSchema<TradeJson> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;
        @Override
        public TradeJson deserialize(byte[] message) throws IOException {
            if (mapper == null) mapper = new ObjectMapper();
            JsonNode n = mapper.readTree(message);
            TradeJson t = new TradeJson();
            t.eventId   = n.path("eventId").asText(null);
            t.accountId = n.path("accountId").asText(null);
            t.ticker    = n.path("ticker").asText(null);
            t.quantity  = n.path("quantity").asInt(0);
            t.price     = n.path("price").asDouble(0.0);
            return t;
        }
    }
}
