package com.workshop.flink.scenario16;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

/**
 * Scenario 16 — Spark reading Paimon directly.
 *
 * <p>This is the canonical "analytics on cold data" pattern. Spark connects
 * to the Paimon catalog (the SAME catalog that scenario 06 created and that
 * scenario 16's tiering writes into), runs heavy aggregations over the
 * tiered Fluss data, and writes a summary table back into Paimon.
 *
 * <p>Run inside the workshop-spark container:
 * <pre>
 *   podman cp scenario-16-fluss-paimon-tiered-spark/target/scenario-16-spark-paimon-batch-jar-with-dependencies.jar \
 *       workshop-spark:/tmp/scenario-16-batch.jar
 *   podman exec workshop-spark spark-submit \
 *       --class com.workshop.flink.scenario16.SparkPaimonBatchJob \
 *       /tmp/scenario-16-batch.jar
 * </pre>
 *
 * <p>The Spark image's {@code spark-defaults.conf} pre-configures the
 * `paimon` catalog with the workshop's MinIO endpoint; no extra args needed.
 */
public class SparkPaimonBatchJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkPaimonBatchJob")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("─── Catalogs ─────────────────────────────────────");
        spark.sql("SHOW CATALOGS").show(false);

        spark.sql("USE paimon");
        spark.sql("USE workshop");

        System.out.println("─── Tables in paimon.workshop ────────────────────");
        spark.sql("SHOW TABLES").show(false);

        // ── Aggregation 1: sector volumes ──────────────────────────────────
        Dataset<Row> sectorVolumes = spark.table("trade_log_wide")
                .groupBy(col("sector"))
                .agg(count("*").alias("trade_count"),
                     sum("quantity").alias("total_qty"),
                     sum("notional_usd").alias("total_notional_usd"),
                     avg("notional_usd").alias("avg_notional_usd"))
                .orderBy(col("total_notional_usd").desc());

        System.out.println("─── Sector volumes ──────────────────────────────");
        sectorVolumes.show(false);

        // ── Aggregation 2: per-country trade counts ────────────────────────
        Dataset<Row> byCountry = spark.table("trade_log_wide")
                .groupBy(col("country"))
                .agg(count("*").alias("trade_count"),
                     sum("notional_usd").alias("total_notional_usd"))
                .orderBy(col("trade_count").desc());

        System.out.println("─── Trades by country ────────────────────────────");
        byCountry.show(false);

        // ── Persist a summary table back into Paimon ───────────────────────
        spark.sql("DROP TABLE IF EXISTS paimon.workshop.spark_sector_summary");
        sectorVolumes.writeTo("paimon.workshop.spark_sector_summary")
                .option("primary-key", "sector")
                .createOrReplace();

        System.out.println("─── Wrote paimon.workshop.spark_sector_summary ──");
        spark.sql("SELECT * FROM paimon.workshop.spark_sector_summary").show(false);

        spark.stop();
    }
}
