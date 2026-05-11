package com.workshop.flink.scenario04.operator;

import com.workshop.flink.common.model.TradeEvent;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Stateful dedup operator keyed by eventId.
 *
 * For each event, checks whether this eventId has been seen within the TTL window.
 * If not seen: forward the event and record the first-seen timestamp in state.
 * If already seen: drop the event and log a warning.
 *
 * TTL is managed by Flink's StateTtlConfig rather than manual timers. This means:
 * - State is cleaned up lazily on access after the TTL expires.
 * - No explicit timer registration is required.
 * - The operator works correctly across checkpoints and restores.
 *
 * Workshop note: TTL must be long enough to cover the maximum inter-duplicate interval.
 * If a duplicate arrives after TTL expiry, it will be forwarded as a new event.
 * Choose TTL based on your business SLA for duplicate detection, not on checkpoint intervals.
 */
public class DeduplicationFunction extends KeyedProcessFunction<String, TradeEvent, TradeEvent> {

    private static final Logger LOG = LogManager.getLogger(DeduplicationFunction.class);

    private final long ttlHours;
    private ValueState<Long> firstSeenTimestamp;

    public DeduplicationFunction(long ttlHours) {
        this.ttlHours = ttlHours;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> descriptor =
            new ValueStateDescriptor<>("dedup-first-seen-ts", Long.class);

        // Flink manages state cleanup automatically. OnCreateAndWrite means the TTL clock
        // resets each time the state is written — so a duplicate that arrives within TTL
        // always resets the window, preventing state from expiring mid-detection.
        StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.hours(ttlHours))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

        descriptor.enableTimeToLive(ttlConfig);
        firstSeenTimestamp = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(TradeEvent event,
                               Context ctx,
                               Collector<TradeEvent> out) throws Exception {
        Long seenAt = firstSeenTimestamp.value();

        if (seenAt == null) {
            // First time we see this eventId within the TTL window — forward it
            firstSeenTimestamp.update(ctx.timerService().currentProcessingTime());
            out.collect(event);
        } else {
            // Duplicate within the TTL window — drop it
            LOG.info("Dropping duplicate eventId={} (first seen at {} ms ago)",
                     event.getEventId(),
                     ctx.timerService().currentProcessingTime() - seenAt);
        }
    }
}
