package com.workshop.flink.common.fluss;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * One-shot BATCH job that creates the Fluss catalog, the {@code workshop}
 * database and the empty tables used across scenarios 11–16. Idempotent:
 * every DDL uses {@code IF NOT EXISTS}.
 *
 * <p>The table options here are the workshop defaults. Individual scenario
 * SQL files override or extend (e.g. scenario 13 recreates a table with
 * {@code 'table.merge-engine'='aggregation'}).
 */
public class FlussInitJob {

    private static final Logger LOG = LogManager.getLogger(FlussInitJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        LOG.info("Registering Fluss catalog at {}", FlussTables.bootstrap());
        tEnv.executeSql(FlussTables.createCatalogDdl());
        tEnv.executeSql("USE CATALOG " + FlussTables.CATALOG);
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + FlussTables.DATABASE);
        tEnv.executeSql("USE " + FlussTables.DATABASE);

        // ── instrument_master: 45-column PK table ───────────────────────────
        tEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS " + FlussTables.TABLE_INSTRUMENT_MASTER + " (\n"
          + "  isin                 STRING,\n"
          + "  cusip                STRING,\n"
          + "  sedol                STRING,\n"
          + "  ticker               STRING,\n"
          + "  exchange             STRING,\n"
          + "  mic                  STRING,\n"
          + "  security_type        STRING,\n"
          + "  sector               STRING,\n"
          + "  sub_sector           STRING,\n"
          + "  country              STRING,\n"
          + "  currency             STRING,\n"
          + "  lot_size             DOUBLE,\n"
          + "  tick_size            DOUBLE,\n"
          + "  min_notional         DOUBLE,\n"
          + "  max_order_qty        DOUBLE,\n"
          + "  shortable            BOOLEAN,\n"
          + "  marginable           BOOLEAN,\n"
          + "  last_close           DOUBLE,\n"
          + "  fifty_two_week_high  DOUBLE,\n"
          + "  fifty_two_week_low   DOUBLE,\n"
          + "  prev_day_volume      DOUBLE,\n"
          + "  avg_daily_volume_30d DOUBLE,\n"
          + "  avg_daily_volume_90d DOUBLE,\n"
          + "  beta_1y              DOUBLE,\n"
          + "  beta_3y              DOUBLE,\n"
          + "  beta_5y              DOUBLE,\n"
          + "  sector_beta          DOUBLE,\n"
          + "  hedge_ratio          DOUBLE,\n"
          + "  volatility_30d       DOUBLE,\n"
          + "  value_at_risk_95     DOUBLE,\n"
          + "  market_cap_usd       DOUBLE,\n"
          + "  free_float           DOUBLE,\n"
          + "  dividend_yield       DOUBLE,\n"
          + "  price_earnings       DOUBLE,\n"
          + "  price_book           DOUBLE,\n"
          + "  debt_to_equity       DOUBLE,\n"
          + "  esg_score            DOUBLE,\n"
          + "  environmental_score  DOUBLE,\n"
          + "  social_score         DOUBLE,\n"
          + "  governance_score     DOUBLE,\n"
          + "  listed_at            BIGINT,\n"
          + "  updated_at           BIGINT,\n"
          + "  issuer_name          STRING,\n"
          + "  issuer_lei           STRING,\n"
          + "  active_flag          BOOLEAN,\n"
          + "  regulatory_status    STRING,\n"
          + "  source_system        STRING,\n"
          + "  PRIMARY KEY (isin) NOT ENFORCED\n"
          + ") WITH (\n"
          + "  'bucket.num' = '4'\n"
          + ")");

        // ── account_profile: 30-column PK table ─────────────────────────────
        tEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS " + FlussTables.TABLE_ACCOUNT_PROFILE + " (\n"
          + "  account_id           STRING,\n"
          + "  customer_id          STRING,\n"
          + "  account_name         STRING,\n"
          + "  account_type         STRING,\n"
          + "  currency             STRING,\n"
          + "  tier                 STRING,\n"
          + "  region               STRING,\n"
          + "  country              STRING,\n"
          + "  mifid_classification STRING,\n"
          + "  fatca_status         STRING,\n"
          + "  tax_residency        STRING,\n"
          + "  kyc_status           STRING,\n"
          + "  kyc_verified_at      BIGINT,\n"
          + "  aml_status           STRING,\n"
          + "  risk_score           INT,\n"
          + "  pep_flag             BOOLEAN,\n"
          + "  sanctions_flag       BOOLEAN,\n"
          + "  daily_trade_limit    DOUBLE,\n"
          + "  monthly_trade_limit  DOUBLE,\n"
          + "  credit_line          DOUBLE,\n"
          + "  available_balance    DOUBLE,\n"
          + "  margin_used          DOUBLE,\n"
          + "  iban                 STRING,\n"
          + "  swift_bic            STRING,\n"
          + "  custodian_bank       STRING,\n"
          + "  opened_at            BIGINT,\n"
          + "  last_login_at        BIGINT,\n"
          + "  updated_at           BIGINT,\n"
          + "  active_flag          BOOLEAN,\n"
          + "  source_system        STRING,\n"
          + "  PRIMARY KEY (account_id) NOT ENFORCED\n"
          + ") WITH (\n"
          + "  'bucket.num' = '4'\n"
          + ")");

        // ── customer_360: wide PK table with last-write-wins default merge ──
        // Scenarios 13 recreates this with merge-engine variants.
        tEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS " + FlussTables.TABLE_CUSTOMER_360 + " (\n"
          + "  customer_id          STRING,\n"
          + "  legal_name           STRING,\n"
          + "  date_of_birth        STRING,\n"
          + "  nationality          STRING,\n"
          + "  tax_residency        STRING,\n"
          + "  kyc_status           STRING,\n"
          + "  kyc_verified_at      BIGINT,\n"
          + "  id_document_type     STRING,\n"
          + "  id_document_number   STRING,\n"
          + "  address_line         STRING,\n"
          + "  address_country      STRING,\n"
          + "  aml_status           STRING,\n"
          + "  risk_score           INT,\n"
          + "  pep_flag             BOOLEAN,\n"
          + "  sanctions_flag       BOOLEAN,\n"
          + "  aml_reviewed_by      STRING,\n"
          + "  aml_reviewed_at      BIGINT,\n"
          + "  marketing_opt_in     BOOLEAN,\n"
          + "  email_opt_in         BOOLEAN,\n"
          + "  sms_opt_in           BOOLEAN,\n"
          + "  preferred_channel    STRING,\n"
          + "  segment              STRING,\n"
          + "  acquisition_source   STRING,\n"
          + "  last_engagement_at   BIGINT,\n"
          + "  updated_at           BIGINT,\n"
          + "  PRIMARY KEY (customer_id) NOT ENFORCED\n"
          + ") WITH (\n"
          + "  'bucket.num' = '4'\n"
          + ")");

        // ── trade_log_wide: append-only log table, partitioned daily ───────
        tEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS " + FlussTables.TABLE_TRADE_LOG_WIDE + " (\n"
          + "  event_id            STRING,\n"
          + "  account_id          STRING,\n"
          + "  customer_id         STRING,\n"
          + "  ticker              STRING,\n"
          + "  isin                STRING,\n"
          + "  side                STRING,\n"
          + "  order_type          STRING,\n"
          + "  quantity            INT,\n"
          + "  price               DOUBLE,\n"
          + "  notional            DOUBLE,\n"
          + "  currency            STRING,\n"
          + "  fx_rate_to_usd      DOUBLE,\n"
          + "  notional_usd        DOUBLE,\n"
          + "  exchange            STRING,\n"
          + "  venue               STRING,\n"
          + "  sector              STRING,\n"
          + "  country             STRING,\n"
          + "  desk                STRING,\n"
          + "  trader              STRING,\n"
          + "  trade_time          BIGINT,\n"
          + "  settled_at          BIGINT,\n"
          + "  settlement_status   STRING,\n"
          + "  commission          DOUBLE,\n"
          + "  tax                 DOUBLE,\n"
          + "  source_app          STRING\n"
          + ") WITH (\n"
          + "  'bucket.num' = '8'\n"
          + ")");

        // ── daily_volumes_agg: aggregation-merge demo (scenario 13 step 04) ──
        tEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS " + FlussTables.TABLE_DAILY_VOLUMES_AGG + " (\n"
          + "  account_id     STRING,\n"
          + "  trade_date     STRING,\n"
          + "  total_qty      BIGINT,\n"
          + "  total_notional DOUBLE,\n"
          + "  max_price      DOUBLE,\n"
          + "  min_price      DOUBLE,\n"
          + "  last_trade_ts  BIGINT,\n"
          + "  PRIMARY KEY (account_id, trade_date) NOT ENFORCED\n"
          + ") WITH (\n"
          + "  'bucket.num' = '4',\n"
          + "  'table.merge-engine' = 'aggregation',\n"
          + "  'fields.total_qty.aggregate-function'      = 'sum',\n"
          + "  'fields.total_notional.aggregate-function' = 'sum',\n"
          + "  'fields.max_price.aggregate-function'      = 'max',\n"
          + "  'fields.min_price.aggregate-function'      = 'min',\n"
          + "  'fields.last_trade_ts.aggregate-function'  = 'max'\n"
          + ")");

        // ── first_observed: first_row merge demo ────────────────────────────
        tEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS " + FlussTables.TABLE_FIRST_OBSERVED + " (\n"
          + "  event_id       STRING,\n"
          + "  first_seen_at  BIGINT,\n"
          + "  source_system  STRING,\n"
          + "  payload        STRING,\n"
          + "  PRIMARY KEY (event_id) NOT ENFORCED\n"
          + ") WITH (\n"
          + "  'bucket.num' = '4',\n"
          + "  'table.merge-engine' = 'first_row'\n"
          + ")");

        // ── versioned_config: versioned merge demo ──────────────────────────
        tEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS " + FlussTables.TABLE_VERSIONED_CONFIG + " (\n"
          + "  config_key     STRING,\n"
          + "  version        BIGINT,\n"
          + "  config_value   STRING,\n"
          + "  updated_at     BIGINT,\n"
          + "  PRIMARY KEY (config_key) NOT ENFORCED\n"
          + ") WITH (\n"
          + "  'bucket.num' = '2',\n"
          + "  'table.merge-engine' = 'versioned',\n"
          + "  'table.merge-engine.versioned.ver-field' = 'version'\n"
          + ")");

        LOG.info("Fluss init complete: catalog={}, database={}, tables created/verified",
                 FlussTables.CATALOG, FlussTables.DATABASE);
    }
}
