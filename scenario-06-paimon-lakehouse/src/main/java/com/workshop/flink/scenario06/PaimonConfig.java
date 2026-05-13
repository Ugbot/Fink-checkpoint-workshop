package com.workshop.flink.scenario06;

/**
 * Resolution of Paimon / MinIO connection settings from environment variables.
 * Defaults assume the containerised workshop network (workshop-minio, workshop-kafka).
 *
 * The CREATE CATALOG DDL emitted here is used by both the streaming ingest job
 * and the standalone bootstrap so the table schema stays in one place.
 */
public final class PaimonConfig {

    public static final String TOPIC = "trades.scenario06";
    public static final String CATALOG = "paimon";
    public static final String DATABASE = "workshop";
    public static final String TABLE = "trades";

    private PaimonConfig() {}

    public static String warehouse() {
        return env("PAIMON_WAREHOUSE", "s3a://paimon/warehouse");
    }

    public static String s3Endpoint() {
        return env("MINIO_ENDPOINT", "http://workshop-minio:9000");
    }

    public static String s3AccessKey() {
        return env("MINIO_ACCESS_KEY", "workshop");
    }

    public static String s3SecretKey() {
        return env("MINIO_SECRET_KEY", "workshopsecret");
    }

    /** CREATE CATALOG DDL, parameterised by environment. */
    public static String createCatalogSql() {
        return String.join("\n",
            "CREATE CATALOG IF NOT EXISTS " + CATALOG + " WITH (",
            "  'type' = 'paimon',",
            "  'warehouse' = '" + warehouse() + "',",
            "  's3.endpoint' = '" + s3Endpoint() + "',",
            "  's3.access-key' = '" + s3AccessKey() + "',",
            "  's3.secret-key' = '" + s3SecretKey() + "',",
            "  's3.path.style.access' = 'true'",
            ")");
    }

    public static String useCatalogSql() {
        return "USE CATALOG " + CATALOG;
    }

    public static String createDatabaseSql() {
        return "CREATE DATABASE IF NOT EXISTS " + DATABASE;
    }

    public static String createTableSql() {
        return String.join("\n",
            "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + TABLE + " (",
            "  event_id   STRING,",
            "  account_id STRING,",
            "  ticker     STRING,",
            "  side       STRING,",
            "  quantity   INT,",
            "  price      DOUBLE,",
            "  trade_time TIMESTAMP_LTZ(3),",
            "  source_app STRING,",
            "  PRIMARY KEY (event_id) NOT ENFORCED",
            ") WITH (",
            "  'bucket' = '4',",
            "  'changelog-producer' = 'input',",
            "  'snapshot.time-retained' = '1h'",
            ")");
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? fallback : v;
    }
}
