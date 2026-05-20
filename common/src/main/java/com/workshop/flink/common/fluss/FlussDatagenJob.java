package com.workshop.flink.common.fluss;

import com.workshop.flink.common.datagen.WideFinancialGenerators;
import com.workshop.flink.common.model.AccountProfile;
import com.workshop.flink.common.model.CustomerAml;
import com.workshop.flink.common.model.CustomerCore;
import com.workshop.flink.common.model.CustomerMarketing;
import com.workshop.flink.common.model.InstrumentMaster;
import com.workshop.flink.common.model.TradeWide;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Streaming job that continuously seeds every wide Fluss table used by
 * scenarios 11–16. Each stream's rate is independently configurable via env
 * vars so the workshop can dial up traffic where the scenario demands it.
 *
 * <p>One job + Flink Table API ↔ Fluss catalog avoids hand-rolling a
 * per-table Fluss producer. Each generator emits POJOs; {@code fromDataStream}
 * converts to a Flink Table; an {@code INSERT INTO} carries them into Fluss.
 *
 * <p>Env vars:
 * <pre>
 *   FLUSS_BOOTSTRAP        coordinator host:port (default workshop-fluss-coordinator:9123)
 *   INSTRUMENT_RATE        rows/sec for instrument_master      (default 50)
 *   ACCOUNT_RATE           rows/sec for account_profile        (default 50)
 *   CUSTOMER_CORE_RATE     rows/sec for KYC partial updates    (default 50)
 *   CUSTOMER_AML_RATE      rows/sec for AML partial updates    (default 25)
 *   CUSTOMER_MKT_RATE      rows/sec for marketing updates      (default 25)
 *   TRADE_WIDE_RATE        rows/sec for trade_log_wide         (default 200)
 *   INSTRUMENT_UNIVERSE    number of unique ISINs              (default 5000)
 *   ACCOUNT_UNIVERSE       number of unique accounts/customers (default 1000)
 *   ENABLE_TABLES          comma-list, e.g. "instrument,account" — default "all"
 * </pre>
 */
public class FlussDatagenJob {

    private static final Logger LOG = LogManager.getLogger(FlussDatagenJob.class);

    public static void main(String[] args) throws Exception {
        int instrumentRate   = envInt("INSTRUMENT_RATE", 50);
        int accountRate      = envInt("ACCOUNT_RATE", 50);
        int customerCoreRate = envInt("CUSTOMER_CORE_RATE", 50);
        int customerAmlRate  = envInt("CUSTOMER_AML_RATE", 25);
        int customerMktRate  = envInt("CUSTOMER_MKT_RATE", 25);
        int tradeWideRate    = envInt("TRADE_WIDE_RATE", 200);
        int instrumentUniverse = envInt("INSTRUMENT_UNIVERSE", 5000);
        int accountUniverse    = envInt("ACCOUNT_UNIVERSE", 1000);
        String enable = System.getenv().getOrDefault("ENABLE_TABLES", "all");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        LOG.info("FlussDatagenJob starting: bootstrap={}, enabled={}", FlussTables.bootstrap(), enable);
        tEnv.executeSql(FlussTables.createCatalogDdl());
        tEnv.executeSql("USE CATALOG " + FlussTables.CATALOG);
        tEnv.executeSql("USE " + FlussTables.DATABASE);

        if (enabled(enable, "instrument")) {
            registerAndInsert(env, tEnv,
                new WideFinancialGenerators.InstrumentMasterGen(instrumentUniverse),
                InstrumentMaster.class,
                "instrument_view",
                instrumentMasterSchema(),
                FlussTables.TABLE_INSTRUMENT_MASTER,
                instrumentRate);
        }

        if (enabled(enable, "account")) {
            registerAndInsert(env, tEnv,
                new WideFinancialGenerators.AccountProfileGen(accountUniverse),
                AccountProfile.class,
                "account_view",
                accountProfileSchema(),
                FlussTables.TABLE_ACCOUNT_PROFILE,
                accountRate);
        }

        if (enabled(enable, "customer-core")) {
            registerAndInsertPartial(env, tEnv,
                new WideFinancialGenerators.CustomerCoreGen(accountUniverse),
                CustomerCore.class,
                "customer_core_view",
                Schema.newBuilder()
                    .column("customerId", "STRING")
                    .column("legalName", "STRING")
                    .column("dateOfBirth", "STRING")
                    .column("nationality", "STRING")
                    .column("taxResidency", "STRING")
                    .column("kycStatus", "STRING")
                    .column("kycVerifiedAt", "BIGINT")
                    .column("idDocumentType", "STRING")
                    .column("idDocumentNumber", "STRING")
                    .column("addressLine", "STRING")
                    .column("addressCountry", "STRING")
                    .column("updatedAt", "BIGINT")
                    .build(),
                "INSERT INTO " + FlussTables.TABLE_CUSTOMER_360
                    + " (customer_id, legal_name, date_of_birth, nationality, tax_residency,"
                    + "  kyc_status, kyc_verified_at, id_document_type, id_document_number,"
                    + "  address_line, address_country, updated_at)"
                    + " SELECT customerId, legalName, dateOfBirth, nationality, taxResidency,"
                    + "  kycStatus, kycVerifiedAt, idDocumentType, idDocumentNumber,"
                    + "  addressLine, addressCountry, updatedAt FROM customer_core_view",
                customerCoreRate);
        }

        if (enabled(enable, "customer-aml")) {
            registerAndInsertPartial(env, tEnv,
                new WideFinancialGenerators.CustomerAmlGen(accountUniverse),
                CustomerAml.class,
                "customer_aml_view",
                Schema.newBuilder()
                    .column("customerId", "STRING")
                    .column("amlStatus", "STRING")
                    .column("riskScore", "INT")
                    .column("pepFlag", "BOOLEAN")
                    .column("sanctionsFlag", "BOOLEAN")
                    .column("amlReviewedBy", "STRING")
                    .column("amlReviewedAt", "BIGINT")
                    .column("updatedAt", "BIGINT")
                    .build(),
                "INSERT INTO " + FlussTables.TABLE_CUSTOMER_360
                    + " (customer_id, aml_status, risk_score, pep_flag, sanctions_flag,"
                    + "  aml_reviewed_by, aml_reviewed_at, updated_at)"
                    + " SELECT customerId, amlStatus, riskScore, pepFlag, sanctionsFlag,"
                    + "  amlReviewedBy, amlReviewedAt, updatedAt FROM customer_aml_view",
                customerAmlRate);
        }

        if (enabled(enable, "customer-mkt")) {
            registerAndInsertPartial(env, tEnv,
                new WideFinancialGenerators.CustomerMarketingGen(accountUniverse),
                CustomerMarketing.class,
                "customer_mkt_view",
                Schema.newBuilder()
                    .column("customerId", "STRING")
                    .column("marketingOptIn", "BOOLEAN")
                    .column("emailOptIn", "BOOLEAN")
                    .column("smsOptIn", "BOOLEAN")
                    .column("preferredChannel", "STRING")
                    .column("segment", "STRING")
                    .column("acquisitionSource", "STRING")
                    .column("lastEngagementAt", "BIGINT")
                    .column("updatedAt", "BIGINT")
                    .build(),
                "INSERT INTO " + FlussTables.TABLE_CUSTOMER_360
                    + " (customer_id, marketing_opt_in, email_opt_in, sms_opt_in,"
                    + "  preferred_channel, segment, acquisition_source,"
                    + "  last_engagement_at, updated_at)"
                    + " SELECT customerId, marketingOptIn, emailOptIn, smsOptIn,"
                    + "  preferredChannel, segment, acquisitionSource,"
                    + "  lastEngagementAt, updatedAt FROM customer_mkt_view",
                customerMktRate);
        }

        if (enabled(enable, "trade-wide")) {
            registerAndInsertPartial(env, tEnv,
                new WideFinancialGenerators.TradeWideGen(instrumentUniverse, accountUniverse),
                TradeWide.class,
                "trade_wide_view",
                tradeWideSchema(),
                "INSERT INTO " + FlussTables.TABLE_TRADE_LOG_WIDE
                    + " (event_id, account_id, customer_id, ticker, isin, side, order_type,"
                    + "  quantity, price, notional, currency, fx_rate_to_usd, notional_usd,"
                    + "  exchange, venue, sector, country, desk, trader, trade_time,"
                    + "  settled_at, settlement_status, commission, tax, source_app)"
                    + " SELECT eventId, accountId, customerId, ticker, isin, side, orderType,"
                    + "  quantity, price, notional, currency, fxRateToUsd, notionalUsd,"
                    + "  exchange, venue, sector, country, desk, trader, tradeTime,"
                    + "  settledAt, settlementStatus, commission, tax, sourceApp"
                    + " FROM trade_wide_view",
                tradeWideRate);
        }

        env.execute("FlussDatagenJob");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static int envInt(String name, int defaultVal) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? defaultVal : Integer.parseInt(v.trim());
    }

    private static boolean enabled(String enable, String table) {
        return enable.equalsIgnoreCase("all") || enable.contains(table);
    }

    /**
     * Insert when the POJO field names match the table column names directly
     * (camelCase → snake_case mismatch handled by aliasing in {@code SELECT}).
     */
    private static <T> void registerAndInsert(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tEnv,
            org.apache.flink.connector.datagen.source.GeneratorFunction<Long, T> gen,
            Class<T> clazz,
            String viewName,
            Schema schema,
            String targetTable,
            int ratePerSec) {

        TypeInformation<T> typeInfo = TypeExtractor.getForClass(clazz);
        DataGeneratorSource<T> source = new DataGeneratorSource<>(
                gen, Long.MAX_VALUE, RateLimiterStrategy.perSecond(ratePerSec), typeInfo);
        DataStream<T> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(),
                clazz.getSimpleName() + "Gen");
        tEnv.createTemporaryView(viewName, tEnv.fromDataStream(stream, schema));
        String snakeSelect = camelToSnakeSelect(schema, viewName);
        String sql = "INSERT INTO " + targetTable + " " + snakeSelect;
        LOG.info("Submitting: {}", sql);
        tEnv.executeSql(sql);
    }

    /**
     * Insert when the column mapping is non-trivial (partial-updates, subset
     * of columns) — caller supplies the full INSERT statement.
     */
    private static <T> void registerAndInsertPartial(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tEnv,
            org.apache.flink.connector.datagen.source.GeneratorFunction<Long, T> gen,
            Class<T> clazz,
            String viewName,
            Schema schema,
            String insertSql,
            int ratePerSec) {

        TypeInformation<T> typeInfo = TypeExtractor.getForClass(clazz);
        DataGeneratorSource<T> source = new DataGeneratorSource<>(
                gen, Long.MAX_VALUE, RateLimiterStrategy.perSecond(ratePerSec), typeInfo);
        DataStream<T> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(),
                clazz.getSimpleName() + "Gen");
        tEnv.createTemporaryView(viewName, tEnv.fromDataStream(stream, schema));
        LOG.info("Submitting: {}", insertSql);
        tEnv.executeSql(insertSql);
    }

    /**
     * Build a {@code SELECT camelCol AS snake_col, ...} projection from a view
     * by lower-casing-and-snake-converting each column. Sufficient for the
     * direct-insert pattern; partial-update inserts always pass their own SQL.
     */
    private static String camelToSnakeSelect(Schema schema, String viewName) {
        StringBuilder sb = new StringBuilder("SELECT ");
        java.util.List<Schema.UnresolvedColumn> cols = schema.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            String name = cols.get(i).getName();
            if (i > 0) sb.append(", ");
            sb.append(name).append(" AS ").append(camelToSnake(name));
        }
        sb.append(" FROM ").append(viewName);
        return sb.toString();
    }

    private static String camelToSnake(String s) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) out.append('_');
                out.append(Character.toLowerCase(c));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    // ── Schemas mirror the InstrumentMaster / AccountProfile / TradeWide POJOs ─

    private static Schema instrumentMasterSchema() {
        return Schema.newBuilder()
            .column("isin", "STRING")
            .column("cusip", "STRING")
            .column("sedol", "STRING")
            .column("ticker", "STRING")
            .column("exchange", "STRING")
            .column("mic", "STRING")
            .column("securityType", "STRING")
            .column("sector", "STRING")
            .column("subSector", "STRING")
            .column("country", "STRING")
            .column("currency", "STRING")
            .column("lotSize", "DOUBLE")
            .column("tickSize", "DOUBLE")
            .column("minNotional", "DOUBLE")
            .column("maxOrderQty", "DOUBLE")
            .column("shortable", "BOOLEAN")
            .column("marginable", "BOOLEAN")
            .column("lastClose", "DOUBLE")
            .column("fiftyTwoWeekHigh", "DOUBLE")
            .column("fiftyTwoWeekLow", "DOUBLE")
            .column("prevDayVolume", "DOUBLE")
            .column("avgDailyVolume30d", "DOUBLE")
            .column("avgDailyVolume90d", "DOUBLE")
            .column("beta1y", "DOUBLE")
            .column("beta3y", "DOUBLE")
            .column("beta5y", "DOUBLE")
            .column("sectorBeta", "DOUBLE")
            .column("hedgeRatio", "DOUBLE")
            .column("volatility30d", "DOUBLE")
            .column("valueAtRisk95", "DOUBLE")
            .column("marketCapUsd", "DOUBLE")
            .column("freeFloat", "DOUBLE")
            .column("dividendYield", "DOUBLE")
            .column("priceEarnings", "DOUBLE")
            .column("priceBook", "DOUBLE")
            .column("debtToEquity", "DOUBLE")
            .column("esgScore", "DOUBLE")
            .column("environmentalScore", "DOUBLE")
            .column("socialScore", "DOUBLE")
            .column("governanceScore", "DOUBLE")
            .column("listedAt", "BIGINT")
            .column("updatedAt", "BIGINT")
            .column("issuerName", "STRING")
            .column("issuerLei", "STRING")
            .column("activeFlag", "BOOLEAN")
            .column("regulatoryStatus", "STRING")
            .column("sourceSystem", "STRING")
            .build();
    }

    private static Schema accountProfileSchema() {
        return Schema.newBuilder()
            .column("accountId", "STRING")
            .column("customerId", "STRING")
            .column("accountName", "STRING")
            .column("accountType", "STRING")
            .column("currency", "STRING")
            .column("tier", "STRING")
            .column("region", "STRING")
            .column("country", "STRING")
            .column("mifidClassification", "STRING")
            .column("fatcaStatus", "STRING")
            .column("taxResidency", "STRING")
            .column("kycStatus", "STRING")
            .column("kycVerifiedAt", "BIGINT")
            .column("amlStatus", "STRING")
            .column("riskScore", "INT")
            .column("pepFlag", "BOOLEAN")
            .column("sanctionsFlag", "BOOLEAN")
            .column("dailyTradeLimit", "DOUBLE")
            .column("monthlyTradeLimit", "DOUBLE")
            .column("creditLine", "DOUBLE")
            .column("availableBalance", "DOUBLE")
            .column("marginUsed", "DOUBLE")
            .column("iban", "STRING")
            .column("swiftBic", "STRING")
            .column("custodianBank", "STRING")
            .column("openedAt", "BIGINT")
            .column("lastLoginAt", "BIGINT")
            .column("updatedAt", "BIGINT")
            .column("activeFlag", "BOOLEAN")
            .column("sourceSystem", "STRING")
            .build();
    }

    private static Schema tradeWideSchema() {
        return Schema.newBuilder()
            .column("eventId", "STRING")
            .column("accountId", "STRING")
            .column("customerId", "STRING")
            .column("ticker", "STRING")
            .column("isin", "STRING")
            .column("side", "STRING")
            .column("orderType", "STRING")
            .column("quantity", "INT")
            .column("price", "DOUBLE")
            .column("notional", "DOUBLE")
            .column("currency", "STRING")
            .column("fxRateToUsd", "DOUBLE")
            .column("notionalUsd", "DOUBLE")
            .column("exchange", "STRING")
            .column("venue", "STRING")
            .column("sector", "STRING")
            .column("country", "STRING")
            .column("desk", "STRING")
            .column("trader", "STRING")
            .column("tradeTime", "BIGINT")
            .column("settledAt", "BIGINT")
            .column("settlementStatus", "STRING")
            .column("commission", "DOUBLE")
            .column("tax", "DOUBLE")
            .column("sourceApp", "STRING")
            .build();
    }
}
