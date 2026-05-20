package com.workshop.flink.common.datagen;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSinkFactory;
import com.workshop.flink.common.util.KafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Fans the trade stream into separate `orders` and `fills` streams for scenario 07a
 * (regular join: orders ⋈ fills, with all four join kinds).
 *
 * Behaviour for each input trade:
 *   - Always emits an "order" record (sourceApp=ORDER) to topic.orders.
 *   - With probability (1 - dropFillPct), emits a matching "fill" with the same
 *     eventId and a slight price drift to topic.fills.
 *   - With probability orphanFillPct, additionally emits an "orphan" fill whose
 *     eventId does NOT match any order — this drives the RIGHT-OUTER demo.
 *
 * CLI / env vars:
 *   --kafka            KAFKA_BOOTSTRAP    broker
 *   --drop-fill-pct    DROP_FILL_PCT      default 0.20 (20% of orders are unfilled → LEFT)
 *   --orphan-fill-pct  ORPHAN_FILL_PCT    default 0.05 (5% of fills are orphans → RIGHT)
 */
public class OrderFillSplitterJob {

    private static final Logger LOG = LogManager.getLogger(OrderFillSplitterJob.class);

    private static final OutputTag<TradeEvent> FILL_TAG =
        new OutputTag<TradeEvent>("fills") {};

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        double dropFillPct   = params.dropFillPct();
        double orphanFillPct = params.orphanFillPct();

        LOG.info("Starting OrderFillSplitterJob: drop_fill_pct={}, orphan_fill_pct={}, kafka={}",
                 dropFillPct, orphanFillPct, params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<TradeEvent> orders = env
            .fromSource(
                KafkaSourceFactory.standard(params.kafka(), Constants.TOPIC_IN, "splitter-s07"),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource <- " + Constants.TOPIC_IN)
            .process(new SplitterFunction(dropFillPct, orphanFillPct))
            .name("OrderFillSplitter");

        DataStream<TradeEvent> fills = orders.getSideOutput(FILL_TAG);

        orders
            .sinkTo(KafkaSinkFactory.atLeastOnce(params.kafka(), Constants.TOPIC_ORDERS))
            .name("KafkaSink -> " + Constants.TOPIC_ORDERS);

        fills
            .sinkTo(KafkaSinkFactory.atLeastOnce(params.kafka(), Constants.TOPIC_FILLS))
            .name("KafkaSink -> " + Constants.TOPIC_FILLS);

        env.execute("OrderFillSplitterJob");
    }

    static class SplitterFunction extends ProcessFunction<TradeEvent, TradeEvent> {
        private static final long serialVersionUID = 1L;

        private final double dropFillPct;
        private final double orphanFillPct;

        SplitterFunction(double dropFillPct, double orphanFillPct) {
            this.dropFillPct   = dropFillPct;
            this.orphanFillPct = orphanFillPct;
        }

        @Override
        public void processElement(TradeEvent in,
                                   Context ctx,
                                   Collector<TradeEvent> out) {
            ThreadLocalRandom rng = ThreadLocalRandom.current();

            // 1. Always emit the order.
            TradeEvent order = copyOf(in);
            order.setSourceApp("ORDER");
            out.collect(order);

            // 2. With probability (1 - dropFillPct) emit a matching fill.
            if (rng.nextDouble() >= dropFillPct) {
                TradeEvent fill = copyOf(in);
                fill.setSourceApp("FILL");
                // Fills usually arrive a bit later and at a slightly different price.
                fill.setTradeTime(in.getTradeTime() + rng.nextInt(50, 1500));
                fill.setPrice(round2(in.getPrice() * (1.0 + rng.nextDouble(-0.001, 0.001))));
                ctx.output(FILL_TAG, fill);
            }

            // 3. With probability orphanFillPct, emit an orphan fill (no matching order).
            if (rng.nextDouble() < orphanFillPct) {
                TradeEvent orphan = copyOf(in);
                orphan.setEventId(UUID.randomUUID().toString());
                orphan.setSourceApp("FILL");
                orphan.setTradeTime(in.getTradeTime() + rng.nextInt(0, 1000));
                ctx.output(FILL_TAG, orphan);
            }
        }

        private static TradeEvent copyOf(TradeEvent t) {
            TradeEvent c = new TradeEvent();
            c.setEventId(t.getEventId());
            c.setAccountId(t.getAccountId());
            c.setTicker(t.getTicker());
            c.setSide(t.getSide());
            c.setQuantity(t.getQuantity());
            c.setPrice(t.getPrice());
            c.setTradeTime(t.getTradeTime());
            c.setSourceApp(t.getSourceApp());
            return c;
        }

        private static double round2(double v) {
            return Math.round(v * 100.0) / 100.0;
        }
    }
}
