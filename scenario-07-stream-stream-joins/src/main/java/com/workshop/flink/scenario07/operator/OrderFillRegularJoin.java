package com.workshop.flink.scenario07.operator;

import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.scenario07.model.JoinedOrderFill;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Regular stream-stream join keyed by eventId (used as orderId).
 *
 * State model per key:
 *   - orderState : ValueState&lt;TradeEvent&gt; holding the most recent order seen
 *   - fillState  : ValueState&lt;TradeEvent&gt; holding the most recent fill seen
 *   - matchedFlag: ValueState&lt;Boolean&gt;    set to true after the first INNER emission,
 *                                              so we never re-emit on duplicate side arrivals
 *
 * Flow for each incoming order:
 *   1. If a fill is already in state → emit INNER, set matchedFlag, clear both sides.
 *   2. Else → store the order in state and register an event-time timer at order.tradeTime
 *      + orphanTimeoutMs. If no fill has arrived by then, emit LEFT_ORPHAN.
 *
 * Flow for each incoming fill: symmetric with RIGHT_ORPHAN.
 *
 * The state TTL is a safety net (1h) — the timer-based eviction is the primary
 * mechanism for bounding state size in this demo.
 */
public class OrderFillRegularJoin
        extends KeyedCoProcessFunction<String, TradeEvent, TradeEvent, JoinedOrderFill> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(OrderFillRegularJoin.class);

    private final long orphanTimeoutMs;
    private final long stateTtlHours;

    private transient ValueState<TradeEvent> orderState;
    private transient ValueState<TradeEvent> fillState;
    private transient ValueState<Boolean>   matchedFlag;

    public OrderFillRegularJoin(long orphanTimeoutMs, long stateTtlHours) {
        this.orphanTimeoutMs = orphanTimeoutMs;
        this.stateTtlHours   = stateTtlHours;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttl = StateTtlConfig
            .newBuilder(Time.hours(stateTtlHours))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

        ValueStateDescriptor<TradeEvent> orderDesc =
            new ValueStateDescriptor<>("order-state", TradeEvent.class);
        orderDesc.enableTimeToLive(ttl);
        orderState = getRuntimeContext().getState(orderDesc);

        ValueStateDescriptor<TradeEvent> fillDesc =
            new ValueStateDescriptor<>("fill-state", TradeEvent.class);
        fillDesc.enableTimeToLive(ttl);
        fillState = getRuntimeContext().getState(fillDesc);

        ValueStateDescriptor<Boolean> matchedDesc =
            new ValueStateDescriptor<>("matched-flag", Boolean.class);
        matchedDesc.enableTimeToLive(ttl);
        matchedFlag = getRuntimeContext().getState(matchedDesc);
    }

    @Override
    public void processElement1(TradeEvent order,
                                Context ctx,
                                Collector<JoinedOrderFill> out) throws Exception {
        TradeEvent existingFill = fillState.value();
        if (existingFill != null && !Boolean.TRUE.equals(matchedFlag.value())) {
            out.collect(new JoinedOrderFill(order.getEventId(),
                                            JoinedOrderFill.JoinKind.INNER,
                                            order, existingFill));
            matchedFlag.update(Boolean.TRUE);
            orderState.clear();
            fillState.clear();
            return;
        }
        orderState.update(order);
        // Processing-time timer (orders/fills arrive close in wall-clock time).
        long fireAt = ctx.timerService().currentProcessingTime() + orphanTimeoutMs;
        ctx.timerService().registerProcessingTimeTimer(fireAt);
    }

    @Override
    public void processElement2(TradeEvent fill,
                                Context ctx,
                                Collector<JoinedOrderFill> out) throws Exception {
        TradeEvent existingOrder = orderState.value();
        if (existingOrder != null && !Boolean.TRUE.equals(matchedFlag.value())) {
            out.collect(new JoinedOrderFill(fill.getEventId(),
                                            JoinedOrderFill.JoinKind.INNER,
                                            existingOrder, fill));
            matchedFlag.update(Boolean.TRUE);
            orderState.clear();
            fillState.clear();
            return;
        }
        fillState.update(fill);
        long fireAt = ctx.timerService().currentProcessingTime() + orphanTimeoutMs;
        ctx.timerService().registerProcessingTimeTimer(fireAt);
    }

    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<JoinedOrderFill> out) throws Exception {
        if (Boolean.TRUE.equals(matchedFlag.value())) {
            return;
        }
        TradeEvent order = orderState.value();
        TradeEvent fill  = fillState.value();

        if (order != null && fill == null) {
            out.collect(new JoinedOrderFill(order.getEventId(),
                                            JoinedOrderFill.JoinKind.LEFT_ORPHAN,
                                            order, null));
            orderState.clear();
            LOG.debug("LEFT_ORPHAN emitted for orderId={}", order.getEventId());
        } else if (fill != null && order == null) {
            out.collect(new JoinedOrderFill(fill.getEventId(),
                                            JoinedOrderFill.JoinKind.RIGHT_ORPHAN,
                                            null, fill));
            fillState.clear();
            LOG.debug("RIGHT_ORPHAN emitted for fillId={}", fill.getEventId());
        }
        // If both sides got matched in the meantime (race against an earlier timer), no-op.
    }
}
