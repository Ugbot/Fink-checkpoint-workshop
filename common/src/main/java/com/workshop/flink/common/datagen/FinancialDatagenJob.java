package com.workshop.flink.common.datagen;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSinkFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Continuous source of synthetic financial trade events for topic.in.
 *
 * CLI / env vars:
 *   --kafka           KAFKA_BOOTSTRAP    broker address (default: localhost:19092)
 *   --datagen-rate    DATAGEN_RATE       events per second (default: 50)
 */
public class FinancialDatagenJob {

    private static final Logger LOG = LogManager.getLogger(FinancialDatagenJob.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        int ratePerSec = params.datagenRate();

        LOG.info("Starting FinancialDatagenJob at {} events/sec → {}, kafka={}",
                 ratePerSec, Constants.TOPIC_IN, params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        GeneratorFunction<Long, TradeEvent> generator = new TradeEventGeneratorFunction();
        TypeInformation<TradeEvent> typeInfo = TypeExtractor.getForClass(TradeEvent.class);

        DataGeneratorSource<TradeEvent> source = new DataGeneratorSource<>(
            generator,
            Long.MAX_VALUE,
            RateLimiterStrategy.perSecond(ratePerSec),
            typeInfo
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "FinancialTradeGenerator")
            .sinkTo(KafkaSinkFactory.atLeastOnce(params.kafka(), Constants.TOPIC_IN))
            .name("KafkaSink -> " + Constants.TOPIC_IN);

        env.execute("FinancialDatagenJob");
    }
}
