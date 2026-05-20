package com.workshop.flink.scenario09.operator;

import com.workshop.flink.common.model.FxRate;
import com.workshop.flink.common.util.CurrencyOf;
import com.workshop.flink.scenario07.model.TradeWithQuote;
import com.workshop.flink.scenario09.model.EnrichedTrade;
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

/**
 * Temporal/versioned join for the multi-way pipeline: keyed by currency,
 * joins (trade ⋈ quote) rows against FX rate history.
 *
 * Mirrors {@link com.workshop.flink.scenario08.operator.VersionedFxJoin} but
 * accepts {@link TradeWithQuote} on the left so the quote field is preserved.
 * The account field of the returned EnrichedTrade is left null — populated by
 * the downstream lookup join.
 */
public class TradeWithQuoteFxJoin
        extends KeyedCoProcessFunction<String, TradeWithQuote, FxRate, EnrichedTrade> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(TradeWithQuoteFxJoin.class);

    private final long retainRateHistoryMs;
    private final long maxPendingMs;

    private transient MapState<Long, Double> rateHistory;
    private transient MapState<String, TradeWithQuote> pending;
    private transient ValueState<Long> maxKnownRateTime;

    public TradeWithQuoteFxJoin(long retainRateHistoryMs, long maxPendingMs) {
        this.retainRateHistoryMs = retainRateHistoryMs;
        this.maxPendingMs        = maxPendingMs;
    }

    @Override
    public void open(Configuration parameters) {
        rateHistory = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("fx-rate-history", Types.LONG, Types.DOUBLE));
        pending = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("pending-tradewithquote", Types.STRING,
                org.apache.flink.api.java.typeutils.TypeExtractor.getForClass(TradeWithQuote.class)));
        maxKnownRateTime = getRuntimeContext().getState(
            new ValueStateDescriptor<>("max-known-rate-time", Long.class));
    }

    @Override
    public void processElement1(TradeWithQuote twq,
                                Context ctx,
                                Collector<EnrichedTrade> out) throws Exception {
        String currency = CurrencyOf.fromAccountId(twq.getTrade().getAccountId());
        long tradeTime = twq.getTrade().getTradeTime();
        Long rt = findRateTimeAtOrBefore(tradeTime);
        if (rt != null) {
            Double rate = rateHistory.get(rt);
            out.collect(new EnrichedTrade(twq.getTrade(), twq.getQuote(), currency, rate, rt, null));
            return;
        }
        pending.put(twq.getTrade().getEventId(), twq);
        ctx.timerService().registerEventTimeTimer(tradeTime + maxPendingMs);
    }

    @Override
    public void processElement2(FxRate fx,
                                Context ctx,
                                Collector<EnrichedTrade> out) throws Exception {
        String currency = ctx.getCurrentKey();
        rateHistory.put(fx.getRateTime(), fx.getRate());

        Long max = maxKnownRateTime.value();
        if (max == null || fx.getRateTime() > max) {
            maxKnownRateTime.update(fx.getRateTime());
        }

        Iterator<Map.Entry<String, TradeWithQuote>> it = pending.iterator();
        while (it.hasNext()) {
            Map.Entry<String, TradeWithQuote> e = it.next();
            TradeWithQuote t = e.getValue();
            if (t.getTrade().getTradeTime() < fx.getRateTime()) {
                Long rt = findRateTimeAtOrBefore(t.getTrade().getTradeTime());
                if (rt != null) {
                    Double rate = rateHistory.get(rt);
                    out.collect(new EnrichedTrade(t.getTrade(), t.getQuote(), currency, rate, rt, null));
                    it.remove();
                }
            }
        }
        ctx.timerService().registerEventTimeTimer(fx.getRateTime() + retainRateHistoryMs);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedTrade> out) throws Exception {
        String currency = ctx.getCurrentKey();

        // 1. Drop pending past the wait limit → emit with null FX (LEFT-JOIN miss).
        Iterator<Map.Entry<String, TradeWithQuote>> pit = pending.iterator();
        while (pit.hasNext()) {
            Map.Entry<String, TradeWithQuote> e = pit.next();
            TradeWithQuote t = e.getValue();
            if (timestamp >= t.getTrade().getTradeTime() + maxPendingMs) {
                out.collect(new EnrichedTrade(t.getTrade(), t.getQuote(), currency, null, null, null));
                pit.remove();
                LOG.debug("Pending TWQ timed out: eventId={}", t.getTrade().getEventId());
            }
        }

        // 2. GC old rate history (keep youngest at-or-before cutoff for late trades).
        long cutoff = timestamp - retainRateHistoryMs;
        Iterator<Map.Entry<Long, Double>> rit = rateHistory.iterator();
        Long youngestKeep = null;
        while (rit.hasNext()) {
            Map.Entry<Long, Double> e = rit.next();
            if (e.getKey() <= cutoff) {
                if (youngestKeep == null || e.getKey() > youngestKeep) {
                    youngestKeep = e.getKey();
                }
            }
        }
        Iterator<Map.Entry<Long, Double>> rit2 = rateHistory.iterator();
        while (rit2.hasNext()) {
            Map.Entry<Long, Double> e = rit2.next();
            if (e.getKey() < cutoff && (youngestKeep == null || e.getKey() < youngestKeep)) {
                rit2.remove();
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
