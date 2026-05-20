package com.workshop.flink.common.fluss;

/**
 * Single source of truth for the Fluss catalog, database, and table names
 * used across scenarios 11–16. Mirror the {@code Constants.TOPIC_*} pattern.
 */
public final class FlussTables {

    public static final String CATALOG       = "fluss";
    public static final String DATABASE      = "workshop";
    public static final String DEFAULT_BOOTSTRAP = "workshop-fluss-coordinator:9123";
    public static final String BOOTSTRAP_ENV    = "FLUSS_BOOTSTRAP";

    // Log tables (append-only) — bucket-partitioned, no PK
    public static final String TABLE_TRADE_LOG_WIDE      = "trade_log_wide";

    // Primary-key tables (upsert / merge) — bucketed by PK hash
    public static final String TABLE_INSTRUMENT_MASTER   = "instrument_master";
    public static final String TABLE_ACCOUNT_PROFILE     = "account_profile";
    public static final String TABLE_CUSTOMER_360        = "customer_360";

    // Aggregation-merge variant for scenario 13 step 04
    public static final String TABLE_DAILY_VOLUMES_AGG   = "daily_volumes_agg";

    // First-row / versioned merge demonstration tables
    public static final String TABLE_FIRST_OBSERVED      = "first_observed";
    public static final String TABLE_VERSIONED_CONFIG    = "versioned_config";

    private FlussTables() {}

    public static String bootstrap() {
        return System.getenv().getOrDefault(BOOTSTRAP_ENV, DEFAULT_BOOTSTRAP);
    }

    /**
     * Returns the standard {@code CREATE CATALOG} DDL with the bootstrap
     * already substituted in. Used both by {@code FlussInitJob} and the
     * datagen drivers so every job uses identical catalog options.
     */
    public static String createCatalogDdl() {
        return "CREATE CATALOG IF NOT EXISTS " + CATALOG + " WITH (\n"
             + "  'type' = 'fluss',\n"
             + "  'bootstrap.servers' = '" + bootstrap() + "'\n"
             + ")";
    }
}
