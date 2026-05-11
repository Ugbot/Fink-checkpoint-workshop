package com.workshop.flink.scenario05.transform;

import com.workshop.flink.common.model.TradeEvent;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Stateful enrichment function that accumulates a per-account trade count across
 * the lifetime of the job. The count is stored in Flink ValueState (checkpointed).
 *
 * When this job is rescaled (parallelism change via savepoint restore), Flink redistributes
 * key groups across the new number of subtasks. The state is correctly migrated — but any
 * Kafka offsets stored in the savepoint may lag behind the current log-end offsets, causing
 * records to be replayed on restore.
 *
 * The running count in this state makes replay effects visible: after rescale+restore,
 * the emitted count values may reset or jump depending on which savepoint was used.
 */
public class StatefulEnrichmentFunction extends RichFlatMapFunction<TradeEvent, TradeEvent> {

    private static final Logger LOG = LogManager.getLogger(StatefulEnrichmentFunction.class);

    private ValueState<Long> tradeCount;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> descriptor =
            new ValueStateDescriptor<>("per-account-trade-count", Long.class);
        tradeCount = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(TradeEvent event, Collector<TradeEvent> out) throws Exception {
        Long current = tradeCount.value();
        long newCount = (current == null ? 0L : current) + 1L;
        tradeCount.update(newCount);

        // Append running count to ticker for visibility in topic.mid
        event.setTicker(event.getTicker() + "#" + newCount);
        event.setSourceApp("s05-app1");

        LOG.debug("account={} runningCount={}", event.getAccountId(), newCount);
        out.collect(event);
    }
}
