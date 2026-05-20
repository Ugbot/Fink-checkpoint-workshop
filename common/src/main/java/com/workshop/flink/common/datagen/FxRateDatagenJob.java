package com.workshop.flink.common.datagen;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.FxRate;
import com.workshop.flink.common.util.JobParams;
import com.workshop.flink.common.util.KafkaSinkFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Slow-moving FX rate changelog → topic.fxrates. Used by scenarios 08 (temporal join) and 09.
 *
 * Runs at parallelism 1 so the generator's per-currency walk state is global.
 *
 * CLI / env vars:
 *   --kafka       KAFKA_BOOTSTRAP    broker (default: localhost:19092)
 *   --fx-rate     FX_RATE            rate updates/sec (default: 5)
 */
public class FxRateDatagenJob {

    private static final Logger LOG = LogManager.getLogger(FxRateDatagenJob.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        int ratePerSec = params.fxRate();

        LOG.info("Starting FxRateDatagenJob at {} updates/sec → {}, kafka={}",
                 ratePerSec, Constants.TOPIC_FXRATES, params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        GeneratorFunction<Long, FxRate> generator = new FxRateGeneratorFunction();
        TypeInformation<FxRate> typeInfo = TypeExtractor.getForClass(FxRate.class);

        DataGeneratorSource<FxRate> source = new DataGeneratorSource<>(
            generator,
            Long.MAX_VALUE,
            RateLimiterStrategy.perSecond(ratePerSec),
            typeInfo
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "FxRateGenerator")
            .sinkTo(KafkaSinkFactory.fxRatesAtLeastOnce(params.kafka(), Constants.TOPIC_FXRATES))
            .name("KafkaSink -> " + Constants.TOPIC_FXRATES);

        env.execute("FxRateDatagenJob");
    }
}
