package com.workshop.flink.scenario08.operator;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.workshop.flink.common.model.AccountInfo;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.scenario08.model.TradeWithAccount;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Async lookup join: for each TradeEvent, look up its AccountInfo from Postgres,
 * caching results in a per-task Caffeine LRU. Cache misses run on a fixed-size
 * thread pool to avoid blocking the Flink mailbox thread.
 *
 * Semantics: this is a LEFT JOIN — when no `accounts` row exists, account=null
 * still flows through. Use a downstream filter for INNER semantics.
 *
 * Why async + cache?
 *   - Each lookup is one round-trip to Postgres (~1ms LAN, 10-100ms WAN)
 *   - At 50 trades/sec a synchronous lookup serializes the whole pipeline
 *   - Caffeine bounds DB load: an account row is fetched at most once per TTL window
 *
 * Trade-off: the cache TTL controls staleness. New accounts inserted into Postgres
 * may briefly miss the join until existing cache entries expire. This is the
 * fundamental difference from a *temporal* join, which uses Flink-managed state
 * keyed by event time rather than wall-clock TTL.
 */
public class AsyncAccountLookupFunction extends RichAsyncFunction<TradeEvent, TradeWithAccount> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(AsyncAccountLookupFunction.class);

    private static final String SQL =
        "SELECT account_id, account_name, tier, region FROM accounts WHERE account_id = ?";

    private final String pgUrl;
    private final String pgUser;
    private final String pgPassword;
    private final int    cacheMaxRows;
    private final long   cacheTtlSeconds;
    private final int    asyncThreads;

    private transient Cache<String, AccountInfo> cache;
    private transient ExecutorService executor;
    private transient ThreadLocal<Connection> connection;

    public AsyncAccountLookupFunction(String pgUrl, String pgUser, String pgPassword,
                                      int cacheMaxRows, long cacheTtlSeconds, int asyncThreads) {
        this.pgUrl           = pgUrl;
        this.pgUser          = pgUser;
        this.pgPassword      = pgPassword;
        this.cacheMaxRows    = cacheMaxRows;
        this.cacheTtlSeconds = cacheTtlSeconds;
        this.asyncThreads    = asyncThreads;
    }

    @Override
    public void open(Configuration parameters) {
        cache = Caffeine.newBuilder()
            .maximumSize(cacheMaxRows)
            .expireAfterWrite(Duration.ofSeconds(cacheTtlSeconds))
            .recordStats()
            .build();
        executor = Executors.newFixedThreadPool(asyncThreads);
        connection = ThreadLocal.withInitial(this::newConnection);
        LOG.info("AsyncAccountLookup opened: cache_max={} ttl={}s threads={}",
                 cacheMaxRows, cacheTtlSeconds, asyncThreads);
    }

    private Connection newConnection() {
        try {
            Connection cx = DriverManager.getConnection(pgUrl, pgUser, pgPassword);
            cx.setAutoCommit(true);
            return cx;
        } catch (Exception e) {
            throw new RuntimeException("Failed to open Postgres connection", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
        // ThreadLocal connections are leaked on close; for a workshop demo this is fine.
    }

    @Override
    public void asyncInvoke(TradeEvent trade, ResultFuture<TradeWithAccount> resultFuture) {
        AccountInfo cached = cache.getIfPresent(trade.getAccountId());
        if (cached != null) {
            resultFuture.complete(Collections.singleton(new TradeWithAccount(trade, cached, true)));
            return;
        }

        executor.submit(() -> {
            try {
                AccountInfo fetched = lookup(trade.getAccountId());
                if (fetched != null) {
                    cache.put(trade.getAccountId(), fetched);
                }
                resultFuture.complete(Collections.singleton(
                    new TradeWithAccount(trade, fetched, false)));
            } catch (Exception e) {
                LOG.warn("Lookup failed for accountId={}: {}", trade.getAccountId(), e.toString());
                resultFuture.completeExceptionally(e);
            }
        });
    }

    @Override
    public void timeout(TradeEvent trade, ResultFuture<TradeWithAccount> resultFuture) {
        // Treat timeout as LEFT JOIN miss: emit the trade with a null account.
        LOG.warn("Lookup timed out for accountId={}", trade.getAccountId());
        resultFuture.complete(Collections.singleton(new TradeWithAccount(trade, null, false)));
    }

    private AccountInfo lookup(String accountId) throws Exception {
        Connection cx = connection.get();
        try (PreparedStatement ps = cx.prepareStatement(SQL)) {
            ps.setString(1, accountId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new AccountInfo(
                        rs.getString("account_id"),
                        rs.getString("account_name"),
                        rs.getString("tier"),
                        rs.getString("region"));
                }
                return null;
            }
        }
    }
}
