package com.workshop.flink.scenario08.operator;

import com.workshop.flink.common.model.FxRate;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.util.CurrencyOf;
import com.workshop.flink.scenario08.model.TradeWithFx;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Temporal / versioned join: trades enriched with the FX rate that applied
 * *at trade time* (event time). Keyed by currency.
 *
 * State per currency:
 *   rateHistory: MapState&lt;rateTime, rate&gt; — every rate change ever seen
 *   pendingTrades: MapState&lt;tradeId, TradeEvent&gt; — trades waiting for a rate ≤ their tradeTime
 *   maxKnownRateTime: ValueState&lt;Long&gt;        — newest rateTime observed (for late-rate detection)
 *
 * On each new FX rate:
 *   1. Append (rateTime, rate) to rateHistory.
 *   2. Update maxKnownRateTime.
 *   3. Scan pendingTrades and emit any trades whose tradeTime ≥ newly-inserted rateTime
 *      AND whose tradeTime is now ≤ maxKnownRateTime+epsilon (we know the latest rate).
 *      In practice we use a heuristic: emit any pending trade whose tradeTime is ≤
 *      the new rate's rateTime, because at that point we know the prior rate is the
 *      best match. Trades newer than the latest known rate keep waiting.
 *
 * On each new Trade:
 *   1. If a rate with rateTime ≤ trade.tradeTime exists, emit immediately with the
 *      largest such rateTime ("AS OF" semantics).
 *   2. Otherwise buffer in pendingTrades until a rate covers it, or it gets evicted
 *      by a watermark timer (configurable max-wait).
 *
 * State eviction:
 *   - Rate history entries older than (currentWatermark - retainRateHistoryMs) are GC'd
 *     via an event-time timer scheduled on rate arrival.
 *   - Pending trades older than (currentWatermark - maxPendingMs) are emitted with
 *     a null fxRate (LEFT JOIN miss) — bounding state size.
 */
public class VersionedFxJoin
        extends KeyedCoProcessFunction<String, TradeEvent, FxRate, TradeWithFx> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(VersionedFxJoin.class);

    private final long retainRateHistoryMs;
    private final long maxPendingMs;

    private transient MapState<Long, Double>     rateHistory;     // rateTime → rate
    private transient MapState<String, TradeEvent> pendingTrades; // eventId → trade
    private transient ValueState<Long> maxKnownRateTime;

    public VersionedFxJoin(long retainRateHistoryMs, long maxPendingMs) {
        this.retainRateHistoryMs = retainRateHistoryMs;
        this.maxPendingMs        = maxPendingMs;
    }

    @Override
    public void open(Configuration parameters) {
        rateHistory = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("fx-rate-history", Types.LONG, Types.DOUBLE));
        pendingTrades = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("pending-trades", Types.STRING,
                org.apache.flink.api.java.typeutils.TypeExtractor.getForClass(TradeEvent.class)));
        maxKnownRateTime = getRuntimeContext().getState(
            new ValueStateDescriptor<>("max-known-rate-time", Long.class));
    }

    /** Trade arrives. */
    @Override
    public void processElement1(TradeEvent trade,
                                Context ctx,
                                Collector<TradeWithFx> out) throws Exception {
        String currency = CurrencyOf.fromAccountId(trade.getAccountId());
        // (Currency derivation is also done in the App as the keyBy, but kept here as
        // a safety net since this function expects the key to be `currency`.)

        Long latestRateTimeAtOrBefore = findRateTimeAtOrBefore(trade.getTradeTime());
        Long knownMax = maxKnownRateTime.value();

        if (latestRateTimeAtOrBefore != null) {
            // We have a rate ≤ tradeTime. Whether it's the "final" answer depends on
            // whether we've seen a newer rate ≥ tradeTime; if not, a later-arriving
            // rate could still come in for an even-earlier rateTime > our pick. Given
            // FX rates are monotonic per currency, in practice the latest ≤ rule is
            // exactly correct under in-order delivery (topic.fxrates has 1 partition).
            Double rate = rateHistory.get(latestRateTimeAtOrBefore);
            out.collect(new TradeWithFx(trade, currency, rate, latestRateTimeAtOrBefore));
            return;
        }
        // No rate yet. Buffer the trade until one arrives or we time out.
        pendingTrades.put(trade.getEventId(), trade);
        long expireAt = trade.getTradeTime() + maxPendingMs;
        ctx.timerService().registerEventTimeTimer(expireAt);
        if (knownMax == null) {
            LOG.debug("Buffered tradeId={} (currency={}) — no rates yet", trade.getEventId(), currency);
        }
    }

    /** FX rate arrives. */
    @Override
    public void processElement2(FxRate fx,
                                Context ctx,
                                Collector<TradeWithFx> out) throws Exception {
        String currency = ctx.getCurrentKey();
        rateHistory.put(fx.getRateTime(), fx.getRate());

        Long currentMax = maxKnownRateTime.value();
        if (currentMax == null || fx.getRateTime() > currentMax) {
            maxKnownRateTime.update(fx.getRateTime());
        }

        // Flush any pending trades whose tradeTime ≤ fx.rateTime (i.e. this rate is
        // a valid AS-OF candidate). For each such trade, find the *latest* rate ≤ tradeTime
        // and emit.
        Iterator<Map.Entry<String, TradeEvent>> it = pendingTrades.iterator();
        while (it.hasNext()) {
            Map.Entry<String, TradeEvent> entry = it.next();
            TradeEvent trade = entry.getValue();
            if (trade.getTradeTime() < fx.getRateTime()) {
                Long bestRateTime = findRateTimeAtOrBefore(trade.getTradeTime());
                if (bestRateTime != null) {
                    Double rate = rateHistory.get(bestRateTime);
                    out.collect(new TradeWithFx(trade, currency, rate, bestRateTime));
                    it.remove();
                }
            }
        }

        // Schedule a cleanup timer to GC old rate history entries.
        long gcAt = fx.getRateTime() + retainRateHistoryMs;
        ctx.timerService().registerEventTimeTimer(gcAt);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TradeWithFx> out) throws Exception {
        String currency = ctx.getCurrentKey();

        // 1. Emit any pending trade older than maxPendingMs with a null rate (LEFT-JOIN miss).
        Iterator<Map.Entry<String, TradeEvent>> pit = pendingTrades.iterator();
        while (pit.hasNext()) {
            Map.Entry<String, TradeEvent> e = pit.next();
            TradeEvent t = e.getValue();
            if (timestamp >= t.getTradeTime() + maxPendingMs) {
                out.collect(new TradeWithFx(t, currency, null, null));
                pit.remove();
                LOG.debug("Emitted unresolved tradeId={} (currency={}) — no rate within {}ms",
                          t.getEventId(), currency, maxPendingMs);
            }
        }

        // 2. GC rate history older than retainRateHistoryMs before the current watermark.
        long cutoff = timestamp - retainRateHistoryMs;
        Iterator<Map.Entry<Long, Double>> rit = rateHistory.iterator();
        TreeMap<Long, Double> snapshot = new TreeMap<>();
        while (rit.hasNext()) {
            Map.Entry<Long, Double> e = rit.next();
            snapshot.put(e.getKey(), e.getValue());
        }
        // Keep the youngest entry ≤ cutoff so pre-cutoff trades that arrive late
        // can still be resolved with the prevailing rate.
        Long keepAtMost = snapshot.floorKey(cutoff);
        for (Long rt : snapshot.keySet()) {
            if (rt < cutoff && (keepAtMost == null || rt < keepAtMost)) {
                rateHistory.remove(rt);
            }
        }
    }

    private Long findRateTimeAtOrBefore(long t) throws Exception {
        Long best = null;
        Iterator<Map.Entry<Long, Double>> it = rateHistory.iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Double> e = it.next();
            long rt = e.getKey();
            if (rt <= t && (best == null || rt > best)) {
                best = rt;
            }
        }
        return best;
    }
}
