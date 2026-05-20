package com.workshop.flink.scenario16;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

/**
 * Scenario 16 — Spark reading the live Fluss table via the Fluss Spark
 * connector. Same aggregation as {@link SparkPaimonBatchJob} but on the
 * live (hot + cold) data instead of just the Paimon mirror.
 *
 * <p>Use this when the 1-minute Paimon-tier latency is too stale and you
 * need "current snapshot" analytics from Spark.
 *
 * <p>The Fluss Spark connector is configured via Spark catalog options.
 * The workshop's Spark image ships with the connector jar already on the
 * classpath; this app just registers the catalog at runtime.
 *
 * <p>Run inside the workshop-spark container:
 * <pre>
 *   podman cp scenario-16-fluss-paimon-tiered-spark/target/scenario-16-spark-fluss-live-jar-with-dependencies.jar \
 *       workshop-spark:/tmp/scenario-16-live.jar
 *   podman exec workshop-spark spark-submit \
 *       --class com.workshop.flink.scenario16.SparkFlussLiveJob \
 *       /tmp/scenario-16-live.jar
 * </pre>
 */
public class SparkFlussLiveJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkFlussLiveJob")
                // Register a Fluss catalog. The workshop image has the Fluss
                // Spark connector on its classpath; the catalog implementation
                // class name follows the Fluss-Spark convention.
                .config("spark.sql.catalog.fluss", "org.apache.fluss.spark.SparkCatalog")
                .config("spark.sql.catalog.fluss.bootstrap.servers", "workshop-fluss-coordinator:9123")
                .config("spark.sql.catalog.fluss.default-database", "workshop")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("─── Catalogs ─────────────────────────────────────");
        spark.sql("SHOW CATALOGS").show(false);

        spark.sql("USE fluss");
        spark.sql("USE workshop");

        System.out.println("─── Tables in fluss.workshop ─────────────────────");
        spark.sql("SHOW TABLES").show(false);

        // ── Aggregation: sector volumes, live (hot + tiered) ───────────────
        Dataset<Row> sectorVolumes = spark.table("trade_log_wide")
                .groupBy(col("sector"))
                .agg(count("*").alias("trade_count"),
                     sum("quantity").alias("total_qty"),
                     sum("notional_usd").alias("total_notional_usd"))
                .orderBy(col("total_notional_usd").desc());

        System.out.println("─── Sector volumes (live, Fluss) ─────────────────");
        sectorVolumes.show(false);

        // ── Account-level live snapshot ────────────────────────────────────
        Dataset<Row> byAccount = spark.table("trade_log_wide")
                .groupBy(col("account_id"))
                .agg(count("*").alias("trade_count"),
                     sum("notional_usd").alias("total_notional_usd"))
                .orderBy(col("total_notional_usd").desc())
                .limit(20);

        System.out.println("─── Top 20 accounts by notional (live) ───────────");
        byAccount.show(false);

        spark.stop();
    }
}
