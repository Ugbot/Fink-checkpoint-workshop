package com.workshop.flink.common.datagen;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.model.QuoteEvent;
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
 * Continuous synthetic quote stream for topic.quotes. Used by scenarios 07–09.
 *
 * CLI / env vars:
 *   --kafka         KAFKA_BOOTSTRAP   broker (default: localhost:19092)
 *   --quote-rate    QUOTE_RATE        quotes/sec (default: 500 → ~10× trade rate)
 */
public class QuoteDatagenJob {

    private static final Logger LOG = LogManager.getLogger(QuoteDatagenJob.class);

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);
        int ratePerSec = params.quoteRate();

        LOG.info("Starting QuoteDatagenJob at {} quotes/sec → {}, kafka={}",
                 ratePerSec, Constants.TOPIC_QUOTES, params.kafka());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        GeneratorFunction<Long, QuoteEvent> generator = new QuoteEventGeneratorFunction();
        TypeInformation<QuoteEvent> typeInfo = TypeExtractor.getForClass(QuoteEvent.class);

        DataGeneratorSource<QuoteEvent> source = new DataGeneratorSource<>(
            generator,
            Long.MAX_VALUE,
            RateLimiterStrategy.perSecond(ratePerSec),
            typeInfo
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "QuoteGenerator")
            .sinkTo(KafkaSinkFactory.quotesAtLeastOnce(params.kafka(), Constants.TOPIC_QUOTES))
            .name("KafkaSink -> " + Constants.TOPIC_QUOTES);

        env.execute("QuoteDatagenJob");
    }
}
