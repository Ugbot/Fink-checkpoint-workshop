package com.workshop.flink.scenario13;

import com.workshop.flink.common.fluss.FlussTables;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Scenario 13 — production-shaped Java DataStream job for partial updates.
 *
 * <p>Reads a single Kafka topic carrying a MIXED event stream (KYC, AML,
 * Marketing events), routes each event to the appropriate column-set, and
 * writes partial updates into the Fluss {@code customer_360} primary-key
 * table via the Fluss connector.
 *
 * <p>This is the canonical "one Flink job that fans events into many
 * partial-update writes" pattern. In real life you would NOT want one
 * Flink job per source.
 *
 * <p>Each Kafka message carries a single envelope shape:
 * <pre>
 * {
 *   "source": "kyc" | "aml" | "marketing",
 *   "customerId": "CUST-NNNNNN",
 *   "fields": { … only the columns this source owns … }
 * }
 * </pre>
 *
 * <p>Side outputs split the unified stream into three typed Rows that match
 * the column subsets each source writes. Three INSERTs (driven by Table API)
 * carry them into Fluss; the merge engine handles the rest.
 *
 * <p>Run via:
 * <pre>
 *   podman exec workshop-jobmanager flink run \
 *     -c com.workshop.flink.scenario13.WideProfileUpdaterJob \
 *     /tmp/scenario-13-wide-profile-updater-jar-with-dependencies.jar
 * </pre>
 *
 * <p>Env vars:
 * <ul>
 *   <li>{@code FLUSS_BOOTSTRAP} — defaults to {@code workshop-fluss-coordinator:9123}</li>
 *   <li>{@code KAFKA_BOOTSTRAP} — defaults to {@code workshop-kafka:9093}</li>
 *   <li>{@code KAFKA_TOPIC}     — defaults to {@code customer-events}</li>
 * </ul>
 */
public class WideProfileUpdaterJob {

    private static final Logger LOG = LogManager.getLogger(WideProfileUpdaterJob.class);

    private static final OutputTag<Row> KYC_TAG = new OutputTag<Row>("kyc", buildKycType()) {};
    private static final OutputTag<Row> AML_TAG = new OutputTag<Row>("aml", buildAmlType()) {};
    private static final OutputTag<Row> MKT_TAG = new OutputTag<Row>("mkt", buildMktType()) {};

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "workshop-kafka:9093");
        String kafkaTopic     = System.getenv().getOrDefault("KAFKA_TOPIC", "customer-events");
        String flussBootstrap = FlussTables.bootstrap();

        LOG.info("WideProfileUpdaterJob starting: kafka={}, topic={}, fluss={}",
                kafkaBootstrap, kafkaTopic, flussBootstrap);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10_000L);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Register the Fluss catalog so we can INSERT INTO fluss.workshop.customer_360
        tEnv.executeSql(
                "CREATE CATALOG IF NOT EXISTS " + FlussTables.CATALOG + " WITH (" +
                "  'type' = 'fluss'," +
                "  'bootstrap.servers' = '" + flussBootstrap + "'" +
                ")");

        // ── Kafka source: unified envelope ───────────────────────────────────
        KafkaSource<CustomerEvent> source = KafkaSource.<CustomerEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(kafkaTopic)
                .setGroupId("scenario-13-wide-profile-updater")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CustomerEventDeserializer())
                .build();

        DataStream<CustomerEvent> events = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "kafka-source");

        // ── Route to three side outputs by source ────────────────────────────
        SingleOutputStreamOperator<Row> kycMain = events.process(new RouterFn())
                .name("router")
                .returns(Types.ROW(Types.STRING));  // main stream isn't used

        DataStream<Row> kyc = kycMain.getSideOutput(KYC_TAG);
        DataStream<Row> aml = kycMain.getSideOutput(AML_TAG);
        DataStream<Row> mkt = kycMain.getSideOutput(MKT_TAG);

        // ── Convert each side-output to a Table and INSERT into customer_360 ─
        tEnv.createTemporaryView("kyc_updates", tEnv.fromDataStream(kyc,
                Schema.newBuilder()
                        .column("customer_id", "STRING")
                        .column("legal_name", "STRING")
                        .column("date_of_birth", "STRING")
                        .column("nationality", "STRING")
                        .column("tax_residency", "STRING")
                        .column("kyc_status", "STRING")
                        .column("kyc_verified_at", "BIGINT")
                        .column("id_document_type", "STRING")
                        .column("id_document_number", "STRING")
                        .column("address_line", "STRING")
                        .column("address_country", "STRING")
                        .column("updated_at", "BIGINT")
                        .build()));

        tEnv.createTemporaryView("aml_updates", tEnv.fromDataStream(aml,
                Schema.newBuilder()
                        .column("customer_id", "STRING")
                        .column("aml_status", "STRING")
                        .column("risk_score", "INT")
                        .column("pep_flag", "BOOLEAN")
                        .column("sanctions_flag", "BOOLEAN")
                        .column("aml_reviewed_by", "STRING")
                        .column("aml_reviewed_at", "BIGINT")
                        .column("updated_at", "BIGINT")
                        .build()));

        tEnv.createTemporaryView("mkt_updates", tEnv.fromDataStream(mkt,
                Schema.newBuilder()
                        .column("customer_id", "STRING")
                        .column("marketing_opt_in", "BOOLEAN")
                        .column("email_opt_in", "BOOLEAN")
                        .column("sms_opt_in", "BOOLEAN")
                        .column("preferred_channel", "STRING")
                        .column("segment", "STRING")
                        .column("acquisition_source", "STRING")
                        .column("last_engagement_at", "BIGINT")
                        .column("updated_at", "BIGINT")
                        .build()));

        // The three INSERTs are independent partial-update writes against
        // the same Fluss PK table. Fluss merges columns automatically.
        String target = FlussTables.CATALOG + "." + FlussTables.DATABASE + "." + FlussTables.TABLE_CUSTOMER_360;

        tEnv.executeSql(
                "INSERT INTO " + target +
                " (customer_id, legal_name, date_of_birth, nationality, tax_residency," +
                "  kyc_status, kyc_verified_at, id_document_type, id_document_number," +
                "  address_line, address_country, updated_at)" +
                " SELECT * FROM kyc_updates");

        tEnv.executeSql(
                "INSERT INTO " + target +
                " (customer_id, aml_status, risk_score, pep_flag, sanctions_flag," +
                "  aml_reviewed_by, aml_reviewed_at, updated_at)" +
                " SELECT * FROM aml_updates");

        tEnv.executeSql(
                "INSERT INTO " + target +
                " (customer_id, marketing_opt_in, email_opt_in, sms_opt_in," +
                "  preferred_channel, segment, acquisition_source," +
                "  last_engagement_at, updated_at)" +
                " SELECT * FROM mkt_updates");

        env.execute("WideProfileUpdaterJob");
    }

    // ── Router function (ProcessFunction with side outputs) ────────────────────

    static class RouterFn extends ProcessFunction<CustomerEvent, Row> {
        private static final long serialVersionUID = 1L;
        @Override
        public void processElement(CustomerEvent e, Context ctx, Collector<Row> out) {
            if (e == null || e.customerId == null || e.fields == null) return;
            long now = System.currentTimeMillis();
            switch (e.source == null ? "" : e.source.toLowerCase()) {
                case "kyc":
                    ctx.output(KYC_TAG, Row.of(
                            e.customerId,
                            str(e, "legalName"),
                            str(e, "dateOfBirth"),
                            str(e, "nationality"),
                            str(e, "taxResidency"),
                            str(e, "kycStatus"),
                            lng(e, "kycVerifiedAt"),
                            str(e, "idDocumentType"),
                            str(e, "idDocumentNumber"),
                            str(e, "addressLine"),
                            str(e, "addressCountry"),
                            now));
                    break;
                case "aml":
                    ctx.output(AML_TAG, Row.of(
                            e.customerId,
                            str(e, "amlStatus"),
                            integerField(e, "riskScore"),
                            bln(e, "pepFlag"),
                            bln(e, "sanctionsFlag"),
                            str(e, "amlReviewedBy"),
                            lng(e, "amlReviewedAt"),
                            now));
                    break;
                case "marketing":
                case "mkt":
                    ctx.output(MKT_TAG, Row.of(
                            e.customerId,
                            bln(e, "marketingOptIn"),
                            bln(e, "emailOptIn"),
                            bln(e, "smsOptIn"),
                            str(e, "preferredChannel"),
                            str(e, "segment"),
                            str(e, "acquisitionSource"),
                            lng(e, "lastEngagementAt"),
                            now));
                    break;
                default:
                    LOG.warn("Dropping event with unknown source='{}' customerId={}",
                            e.source, e.customerId);
            }
        }
    }

    private static String str(CustomerEvent e, String k) {
        JsonNode n = e.fields.get(k);
        return n == null || n.isNull() ? null : n.asText();
    }
    private static Integer integerField(CustomerEvent e, String k) {
        JsonNode n = e.fields.get(k);
        return n == null || n.isNull() ? null : n.asInt();
    }
    private static Boolean bln(CustomerEvent e, String k) {
        JsonNode n = e.fields.get(k);
        return n == null || n.isNull() ? null : n.asBoolean();
    }
    private static Long lng(CustomerEvent e, String k) {
        JsonNode n = e.fields.get(k);
        return n == null || n.isNull() ? null : n.asLong();
    }

    // ── Side-output type information ─────────────────────────────────────────

    private static org.apache.flink.api.common.typeinfo.TypeInformation<Row> buildKycType() {
        return Types.ROW_NAMED(
                new String[]{"customer_id","legal_name","date_of_birth","nationality","tax_residency",
                             "kyc_status","kyc_verified_at","id_document_type","id_document_number",
                             "address_line","address_country","updated_at"},
                Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                Types.STRING, Types.LONG, Types.STRING, Types.STRING,
                Types.STRING, Types.STRING, Types.LONG);
    }
    private static org.apache.flink.api.common.typeinfo.TypeInformation<Row> buildAmlType() {
        return Types.ROW_NAMED(
                new String[]{"customer_id","aml_status","risk_score","pep_flag","sanctions_flag",
                             "aml_reviewed_by","aml_reviewed_at","updated_at"},
                Types.STRING, Types.STRING, Types.INT, Types.BOOLEAN, Types.BOOLEAN,
                Types.STRING, Types.LONG, Types.LONG);
    }
    private static org.apache.flink.api.common.typeinfo.TypeInformation<Row> buildMktType() {
        return Types.ROW_NAMED(
                new String[]{"customer_id","marketing_opt_in","email_opt_in","sms_opt_in",
                             "preferred_channel","segment","acquisition_source",
                             "last_engagement_at","updated_at"},
                Types.STRING, Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN,
                Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG);
    }

    // ── Wire envelope POJO + deserializer ────────────────────────────────────

    /** Mixed-source event envelope read from Kafka. */
    public static class CustomerEvent implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public String source;       // "kyc" / "aml" / "marketing"
        public String customerId;
        public JsonNode fields;     // sub-document of source-specific cols
    }

    /** Lightweight Jackson-based deserializer (no schema registry needed). */
    public static class CustomerEventDeserializer extends AbstractDeserializationSchema<CustomerEvent> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;
        @Override
        public CustomerEvent deserialize(byte[] message) throws java.io.IOException {
            if (mapper == null) mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(message);
            CustomerEvent e = new CustomerEvent();
            e.source = root.path("source").asText(null);
            e.customerId = root.path("customerId").asText(null);
            e.fields = root.path("fields");
            return e;
        }
    }
}
