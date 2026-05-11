package com.workshop.flink.scenario03.sink;

import com.workshop.flink.common.model.TradeEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * FIXED sink: upsert using ON CONFLICT DO UPDATE.
 *
 * When Flink replays the same event after a crash, this SQL is idempotent:
 * the second write updates the existing row rather than inserting a second one.
 * The processed_trades table always ends up with exactly one row per event_id.
 *
 * This is the correct pattern for external sinks that are not part of Flink's
 * two-phase commit protocol. For sinks that support transactions (e.g., the
 * flink-connector-jdbc with ExactlyOnceSinkWriter), the framework can manage
 * idempotency at the connector level instead.
 */
public class PostgresUpsertSink extends RichSinkFunction<TradeEvent> {

    private static final Logger LOG = LogManager.getLogger(PostgresUpsertSink.class);

    private static final String SQL =
        "INSERT INTO processed_trades (event_id, account_id, ticker, net_qty, processed_at) " +
        "VALUES (?, ?, ?, ?, NOW()) " +
        "ON CONFLICT (event_id) DO UPDATE SET " +
        "    account_id   = EXCLUDED.account_id, " +
        "    ticker       = EXCLUDED.ticker, " +
        "    net_qty      = EXCLUDED.net_qty, " +
        "    processed_at = NOW()";

    private final String pgUrl;
    private final String pgUser;
    private final String pgPassword;

    private transient Connection connection;

    public PostgresUpsertSink(String pgUrl, String pgUser, String pgPassword) {
        this.pgUrl = pgUrl;
        this.pgUser = pgUser;
        this.pgPassword = pgPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(pgUrl, pgUser, pgPassword);
        connection.setAutoCommit(true);
        LOG.info("PostgresUpsertSink (FIXED) opened connection to {}", pgUrl);
    }

    @Override
    public void invoke(TradeEvent event, Context context) throws Exception {
        try (PreparedStatement ps = connection.prepareStatement(SQL)) {
            ps.setString(1, event.getEventId());
            ps.setString(2, event.getAccountId());
            ps.setString(3, event.getTicker());
            ps.setInt(4, event.getQuantity());
            ps.executeUpdate();
            LOG.debug("Upserted event_id={} into processed_trades", event.getEventId());
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
