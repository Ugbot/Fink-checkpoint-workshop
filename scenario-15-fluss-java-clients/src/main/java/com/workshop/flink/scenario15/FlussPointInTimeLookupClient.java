package com.workshop.flink.scenario15;

import com.workshop.flink.common.fluss.FlussTables;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.GenericRow;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Scenario 15 — point-in-time lookups against a Fluss primary-key table
 * using ONLY the Fluss Java SDK. No Flink, no Kafka, no Spark.
 *
 * <p>This is the workload that compacted Kafka topics cannot serve well.
 * Build a microservice on top of this app and you have a point-lookup
 * cache backed directly by Fluss state, with no sidecar materialiser.
 *
 * <p>Usage:
 * <pre>
 *   java -jar scenario-15-point-in-time-lookup-jar-with-dependencies.jar [N]
 * </pre>
 * where N is the number of random lookups to perform (default 20).
 *
 * <p>Env vars:
 * <ul>
 *   <li>{@code FLUSS_BOOTSTRAP} — defaults to {@code workshop-fluss-coordinator:9123}
 *     (or {@code localhost:19123} when running on the host).</li>
 *   <li>{@code FLUSS_DATABASE}  — defaults to {@code workshop}.</li>
 * </ul>
 */
public class FlussPointInTimeLookupClient {

    public static void main(String[] args) throws Exception {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 20;
        String bootstrap = FlussTables.bootstrap();
        String database  = System.getenv().getOrDefault("FLUSS_DATABASE", FlussTables.DATABASE);

        System.out.printf("Connecting to Fluss at %s (database=%s)%n", bootstrap, database);

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);

        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            // ── Point-lookup against account_profile (PK = account_id) ───────
            lookupAccounts(conn, database, n);
            // ── Point-lookup against instrument_master (PK = isin) ───────────
            lookupInstruments(conn, database, n);
        }
    }

    private static void lookupAccounts(Connection conn, String database, int n) throws Exception {
        TablePath path = TablePath.of(database, FlussTables.TABLE_ACCOUNT_PROFILE);
        try (Table table = conn.getTable(path)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            System.out.printf("%n── %s: random point lookups (%d) ──────────────────────────%n",
                    FlussTables.TABLE_ACCOUNT_PROFILE, n);
            int hit = 0, miss = 0;
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                String key = String.format("ACC-%04d", ThreadLocalRandom.current().nextInt(1, 1001));
                InternalRow rowKey = GenericRow.of(key);
                LookupResult res = lookuper.lookup(rowKey).get();
                List<InternalRow> rows = res.getRowList();
                if (rows == null || rows.isEmpty()) {
                    miss++;
                    System.out.printf("  %s  →  (no row)%n", key);
                } else {
                    hit++;
                    InternalRow row = rows.get(0);
                    // Schema-aware field access by column index; column 0 is the
                    // PK (account_id) and columns 5 (tier) + 14 (risk_score) make
                    // a compact demo print line.
                    String tier = row.getString(5).toString();
                    int riskScore = row.getInt(14);
                    System.out.printf("  %s  →  tier=%s, risk_score=%d%n", key, tier, riskScore);
                }
            }
            long ms = (System.nanoTime() - t0) / 1_000_000L;
            System.out.printf("  %d hits, %d misses in %d ms (%.1f lookups/sec)%n",
                    hit, miss, ms, n * 1000.0 / Math.max(1, ms));
        }
    }

    private static void lookupInstruments(Connection conn, String database, int n) throws Exception {
        TablePath path = TablePath.of(database, FlussTables.TABLE_INSTRUMENT_MASTER);
        try (Table table = conn.getTable(path)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            System.out.printf("%n── %s: random point lookups (%d) ──────────────────────────%n",
                    FlussTables.TABLE_INSTRUMENT_MASTER, n);
            int hit = 0, miss = 0;
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                String key = String.format("ISIN%08d", ThreadLocalRandom.current().nextInt(0, 5000));
                InternalRow rowKey = GenericRow.of(key);
                LookupResult res = lookuper.lookup(rowKey).get();
                List<InternalRow> rows = res.getRowList();
                if (rows == null || rows.isEmpty()) {
                    miss++;
                } else {
                    hit++;
                    InternalRow row = rows.get(0);
                    String ticker = row.getString(3).toString();        // col 3: ticker
                    String sector = row.getString(7).toString();        // col 7: sector
                    double lastClose = row.getDouble(17);              // col 17: last_close
                    System.out.printf("  %s  →  ticker=%s, sector=%s, last_close=%.2f%n",
                            key, ticker, sector, lastClose);
                }
            }
            long ms = (System.nanoTime() - t0) / 1_000_000L;
            System.out.printf("  %d hits, %d misses in %d ms (%.1f lookups/sec)%n",
                    hit, miss, ms, n * 1000.0 / Math.max(1, ms));
        }
    }
}
