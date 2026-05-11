package com.workshop.flink.scenario01.transform;

import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.util.CrashTrigger;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class EnrichmentFunction extends RichMapFunction<TradeEvent, TradeEvent> {

    private final CrashTrigger crashTrigger;

    public EnrichmentFunction(CrashTrigger crashTrigger) {
        this.crashTrigger = crashTrigger;
    }

    @Override
    public void open(Configuration parameters) {}

    @Override
    public TradeEvent map(TradeEvent event) throws Exception {
        // Increment before enrichment so the crash fires mid-processing,
        // simulating work done that will need to be replayed.
        crashTrigger.increment();
        event.setSourceApp("s01-app1");
        return event;
    }
}
