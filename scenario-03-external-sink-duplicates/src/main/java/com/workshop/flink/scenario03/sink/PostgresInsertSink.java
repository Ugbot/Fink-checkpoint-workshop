package com.workshop.flink.scenario03.sink;

import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.util.CrashTrigger;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * BUGGY sink: plain INSERT with no conflict handling.
 *
 * When App2 crashes after this write but before the Flink checkpoint completes,
 * Flink replays the same events on restart and this INSERT fires again.
 * If event_id has a PRIMARY KEY constraint, the second run gets a PK violation.
 * If the PK constraint is removed (to show the duplicate clearly), two rows are inserted.
 *
 * The bug: this sink is NOT idempotent. The fix is PostgresUpsertSink.
 */
public class PostgresInsertSink extends RichSinkFunction<TradeEvent> {

    private static final Logger LOG = LogManager.getLogger(PostgresInsertSink.class);

    private static final String SQL =
        "INSERT INTO processed_trades (event_id, account_id, ticker, net_qty, processed_at) " +
        "VALUES (?, ?, ?, ?, NOW())";

    private final CrashTrigger crashTrigger;
    private final String pgUrl;
    private final String pgUser;
    private final String pgPassword;

    private transient Connection connection;

    public PostgresInsertSink(CrashTrigger crashTrigger, String pgUrl, String pgUser, String pgPassword) {
        this.crashTrigger = crashTrigger;
        this.pgUrl = pgUrl;
        this.pgUser = pgUser;
        this.pgPassword = pgPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(pgUrl, pgUser, pgPassword);
        connection.setAutoCommit(true);
        LOG.info("PostgresInsertSink (BUGGY) opened connection to {}", pgUrl);
    }

    @Override
    public void invoke(TradeEvent event, Context context) throws Exception {
        try (PreparedStatement ps = connection.prepareStatement(SQL)) {
            ps.setString(1, event.getEventId());
            ps.setString(2, event.getAccountId());
            ps.setString(3, event.getTicker());
            ps.setInt(4, event.getQuantity());
            ps.executeUpdate();
            LOG.debug("Inserted event_id={} into processed_trades", event.getEventId());
        }
        // Crash AFTER the DB write but BEFORE the checkpoint commits.
        // On restart, Flink replays from the last checkpoint offset.
        // The same event arrives again and INSERT fires again → duplicate or PK violation.
        crashTrigger.increment();
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
