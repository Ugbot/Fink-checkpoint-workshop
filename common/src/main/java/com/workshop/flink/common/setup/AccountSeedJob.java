package com.workshop.flink.common.setup;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.util.JobParams;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.concurrent.ThreadLocalRandom;

/**
 * One-shot Flink BATCH job that seeds the `accounts` table in Postgres with N rows.
 * Idempotent: uses ON CONFLICT DO NOTHING.
 *
 * The account IDs are ACC-0001 … ACC-NNNN, matching what TradeEventGeneratorFunction
 * produces. This is what the lookup-join scenarios join against.
 *
 * CLI / env vars:
 *   --pg-url           PG_URL          jdbc:postgresql://localhost:15432/workshop
 *   --pg-user          PG_USER         workshop
 *   --pg-password      PG_PASSWORD     workshop
 *   --account-count    ACCOUNT_COUNT   number of rows (default: 50)
 */
public class AccountSeedJob {

    private static final Logger LOG = LogManager.getLogger(AccountSeedJob.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        env.fromElements(params.accountCount())
           .map(new AccountSeederFunction(params.pgUrl(), params.pgUser(), params.pgPassword()))
           .print();

        env.execute("AccountSeedJob");
    }

    static class AccountSeederFunction extends RichMapFunction<Integer, String> {
        private static final long serialVersionUID = 1L;

        private static final String[] TIERS   = {"PLATINUM", "GOLD", "SILVER", "BRONZE"};
        private static final String[] REGIONS = {"NA", "EMEA", "APAC", "LATAM"};

        private static final String SQL =
            "INSERT INTO " + Constants.PG_TABLE_ACCOUNTS +
            " (account_id, account_name, tier, region) VALUES (?, ?, ?, ?) " +
            "ON CONFLICT (account_id) DO NOTHING";

        private final String pgUrl;
        private final String pgUser;
        private final String pgPassword;

        AccountSeederFunction(String pgUrl, String pgUser, String pgPassword) {
            this.pgUrl      = pgUrl;
            this.pgUser     = pgUser;
            this.pgPassword = pgPassword;
        }

        @Override
        public String map(Integer count) throws Exception {
            // Use a fixed seed so the seeded values are stable across re-runs.
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            int inserted = 0;

            try (Connection cx = DriverManager.getConnection(pgUrl, pgUser, pgPassword);
                 PreparedStatement ps = cx.prepareStatement(SQL)) {
                cx.setAutoCommit(true);
                for (int i = 1; i <= count; i++) {
                    String accountId   = String.format("ACC-%04d", i);
                    String accountName = "Account-" + i;
                    String tier        = TIERS[rng.nextInt(TIERS.length)];
                    String region      = REGIONS[rng.nextInt(REGIONS.length)];

                    ps.setString(1, accountId);
                    ps.setString(2, accountName);
                    ps.setString(3, tier);
                    ps.setString(4, region);
                    ps.executeUpdate();
                    inserted++;
                }
            }

            String msg = String.format("AccountSeedJob: upserted %d rows into %s",
                                       inserted, Constants.PG_TABLE_ACCOUNTS);
            LOG.info(msg);
            return msg;
        }
    }
}
