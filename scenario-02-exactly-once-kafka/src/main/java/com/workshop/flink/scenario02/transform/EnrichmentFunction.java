package com.workshop.flink.scenario02.transform;

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
        crashTrigger.increment();
        event.setSourceApp("s02-app1");
        return event;
    }
}
