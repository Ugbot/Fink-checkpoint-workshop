package com.workshop.flink.scenario09.operator;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.workshop.flink.common.model.AccountInfo;
import com.workshop.flink.common.util.CrashTrigger;
import com.workshop.flink.scenario09.model.EnrichedTrade;
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
 * Final stage of the multi-way pipeline: async account lookup + optional crash trigger.
 *
 * Combining the two lets the verify script easily count records that flowed through
 * before vs. after the crash by inspecting topic.enriched.s09 with read_committed.
 *
 * Crash semantics:
 *   - CrashTrigger.count is transient — does NOT survive restart
 *   - After restart, the counter resets; if the upstream replays > crashAfter records
 *     between recovery and the next checkpoint, the crash re-fires
 *   - For workshop reproducibility set CRASH_AFTER_RECORDS large enough that the next
 *     checkpoint catches up before the threshold is hit again (~5000 is good with 10s ckpt)
 */
public class AccountLookupAndCrash extends RichAsyncFunction<EnrichedTrade, EnrichedTrade> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(AccountLookupAndCrash.class);

    private static final String SQL =
        "SELECT account_id, account_name, tier, region FROM accounts WHERE account_id = ?";

    private final String pgUrl;
    private final String pgUser;
    private final String pgPassword;
    private final int    cacheMaxRows;
    private final long   cacheTtlSeconds;
    private final int    asyncThreads;
    private final CrashTrigger crashTrigger;

    private transient Cache<String, AccountInfo> cache;
    private transient ExecutorService executor;
    private transient ThreadLocal<Connection> connection;

    public AccountLookupAndCrash(String pgUrl, String pgUser, String pgPassword,
                                 int cacheMaxRows, long cacheTtlSeconds, int asyncThreads,
                                 CrashTrigger crashTrigger) {
        this.pgUrl           = pgUrl;
        this.pgUser          = pgUser;
        this.pgPassword      = pgPassword;
        this.cacheMaxRows    = cacheMaxRows;
        this.cacheTtlSeconds = cacheTtlSeconds;
        this.asyncThreads    = asyncThreads;
        this.crashTrigger    = crashTrigger;
    }

    @Override
    public void open(Configuration parameters) {
        cache = Caffeine.newBuilder()
            .maximumSize(cacheMaxRows)
            .expireAfterWrite(Duration.ofSeconds(cacheTtlSeconds))
            .recordStats()
            .build();
        executor   = Executors.newFixedThreadPool(asyncThreads);
        connection = ThreadLocal.withInitial(() -> {
            try {
                Connection cx = DriverManager.getConnection(pgUrl, pgUser, pgPassword);
                cx.setAutoCommit(true);
                return cx;
            } catch (Exception e) {
                throw new RuntimeException("Failed to open PG connection", e);
            }
        });
        LOG.info("AccountLookupAndCrash opened: cache_max={} ttl={}s threads={}",
                 cacheMaxRows, cacheTtlSeconds, asyncThreads);
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    @Override
    public void asyncInvoke(EnrichedTrade in, ResultFuture<EnrichedTrade> resultFuture) {
        String accountId = in.getTrade().getAccountId();
        AccountInfo cached = cache.getIfPresent(accountId);
        if (cached != null) {
            in.setAccount(cached);
            crashTrigger.increment();
            resultFuture.complete(Collections.singleton(in));
            return;
        }
        executor.submit(() -> {
            try {
                AccountInfo fetched = lookup(accountId);
                if (fetched != null) {
                    cache.put(accountId, fetched);
                }
                in.setAccount(fetched);
                crashTrigger.increment();
                resultFuture.complete(Collections.singleton(in));
            } catch (Exception e) {
                resultFuture.completeExceptionally(e);
            }
        });
    }

    @Override
    public void timeout(EnrichedTrade in, ResultFuture<EnrichedTrade> resultFuture) {
        LOG.warn("Account lookup timed out for accountId={}", in.getTrade().getAccountId());
        resultFuture.complete(Collections.singleton(in));
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
