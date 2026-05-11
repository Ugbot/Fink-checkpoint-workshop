package com.workshop.flink.scenario04.producer;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSinkFactory;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Scenario 04 — Duplicate Event Producer
 *
 * Flink BATCH job that sends a finite stream of trade events to topic.in, deliberately
 * repeating every Nth event with the same eventId to simulate an upstream at-least-once
 * producer that retried a delivery. App1 faithfully forwards these to topic.mid, so
 * App2 sees the duplicates regardless of Flink's exactly-once transport guarantees.
 *
 * CLI / env vars:
 *   --kafka                KAFKA_BOOTSTRAP       broker (default: localhost:19092)
 *   --total-events         TOTAL_EVENTS          unique events to send (default: 100)
 *   --duplicate-every      DUPLICATE_EVERY       repeat the Nth event (default: 10)
 *   --duplicate-delay-ms   DUPLICATE_DELAY_MS    pause between send and re-send (default: 200)
 */
public class DuplicateEventProducer {

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        int totalEvents      = params.totalEvents();
        int duplicateEvery   = params.duplicateEvery();
        long duplicateDelayMs = params.duplicateDelayMs();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        env.addSource(new DuplicateEventSource(totalEvents, duplicateEvery, duplicateDelayMs))
           .sinkTo(KafkaSinkFactory.atLeastOnce(params.kafka(), Constants.TOPIC_IN))
           .name("KafkaSink -> " + Constants.TOPIC_IN);

        env.execute("DuplicateEventProducer");
    }

    /**
     * Bounded source that emits totalEvents unique TradeEvents, re-emitting every
     * duplicateEvery-th event (same object = same eventId) after a short delay.
     */
    static class DuplicateEventSource implements SourceFunction<TradeEvent>, Serializable {

        private static final Logger LOG = LogManager.getLogger(DuplicateEventSource.class);

        private static final String[] TICKERS = {"AAPL", "MSFT", "GOOGL", "JPM", "GS"};

        private final int totalEvents;
        private final int duplicateEvery;
        private final long duplicateDelayMs;

        DuplicateEventSource(int totalEvents, int duplicateEvery, long duplicateDelayMs) {
            this.totalEvents      = totalEvents;
            this.duplicateEvery   = duplicateEvery;
            this.duplicateDelayMs = duplicateDelayMs;
        }

        @Override
        public void run(SourceContext<TradeEvent> ctx) throws Exception {
            ThreadLocalRandom rng = ThreadLocalRandom.current();

            for (int i = 0; i < totalEvents; i++) {
                String ticker = TICKERS[rng.nextInt(TICKERS.length)];
                String side   = rng.nextBoolean() ? "BUY" : "SELL";
                int    qty    = rng.nextInt(1, 1001);
                double price  = Math.round((10.0 + rng.nextDouble(990.0)) * 100.0) / 100.0;
                String acct   = String.format("ACC-%04d", rng.nextInt(1, 51));

                TradeEvent event = TradeEvent.of(acct, ticker, side, qty, price);
                ctx.collect(event);
                LOG.info("Sent event #{}: eventId={} ticker={}", i, event.getEventId(), ticker);

                if (i > 0 && i % duplicateEvery == 0) {
                    Thread.sleep(duplicateDelayMs);
                    // Re-emit the SAME object: same eventId = business duplicate
                    ctx.collect(event);
                    LOG.info("  Sent DUPLICATE for eventId={}", event.getEventId());
                }
            }
        }

        @Override
        public void cancel() {
            // bounded source — cancel is never called during normal completion
        }
    }
}
