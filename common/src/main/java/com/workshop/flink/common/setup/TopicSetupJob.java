package com.workshop.flink.common.setup;

import com.workshop.flink.common.Constants;
import com.workshop.flink.common.util.JobParams;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * One-shot Flink BATCH job that creates the three workshop Kafka topics.
 *
 * Idempotent: skips topics that already exist. Safe to run repeatedly.
 *
 * CLI / env vars:
 *   --kafka               KAFKA_BOOTSTRAP    broker (default: localhost:19092)
 *   --topic-partitions    TOPIC_PARTITIONS   partitions per topic (default: 4)
 */
public class TopicSetupJob {

    public static void main(String[] args) throws Exception {
        JobParams params = JobParams.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        // fromElements drives a single-element pipeline; all topic creation happens in the map.
        env.fromElements(params.kafka())
           .map(new TopicCreatorFunction(params.topicPartitions()))
           .print();

        env.execute("TopicSetupJob");
    }

    static class TopicCreatorFunction extends RichMapFunction<String, String> {

        private static final Logger LOG = LogManager.getLogger(TopicCreatorFunction.class);
        private final int partitions;

        TopicCreatorFunction(int partitions) {
            this.partitions = partitions;
        }

        @Override
        public void open(Configuration parameters) {}

        @Override
        public String map(String bootstrap) throws Exception {
            // Topic → partition count. The `fxrates` topic uses 1 partition so the
            // per-currency changelog stays globally ordered for the temporal join.
            Map<String, Integer> required = new LinkedHashMap<>();
            // Scenarios 01–05
            required.put(Constants.TOPIC_IN,  partitions);
            required.put(Constants.TOPIC_MID, partitions);
            required.put(Constants.TOPIC_OUT, partitions);
            // Scenarios 07–09 (joins)
            required.put(Constants.TOPIC_QUOTES,       partitions);
            required.put(Constants.TOPIC_FXRATES,      1);
            required.put(Constants.TOPIC_ORDERS,       partitions);
            required.put(Constants.TOPIC_FILLS,        partitions);
            required.put(Constants.TOPIC_ENRICHED_S07, partitions);
            required.put(Constants.TOPIC_ENRICHED_S08, partitions);
            required.put(Constants.TOPIC_ENRICHED_S09, partitions);

            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

            try (AdminClient admin = AdminClient.create(props)) {
                Set<String> existing = admin.listTopics().names().get();

                java.util.List<NewTopic> toCreate = required.entrySet().stream()
                    .filter(e -> !existing.contains(e.getKey()))
                    .map(e -> new NewTopic(e.getKey(), e.getValue(), (short) 1))
                    .collect(Collectors.toList());

                if (toCreate.isEmpty()) {
                    LOG.info("All topics already exist: {}", required.keySet());
                } else {
                    admin.createTopics(toCreate).all().get();
                    toCreate.forEach(t -> LOG.info("Created topic {} ({} partitions)",
                                                   t.name(), t.numPartitions()));
                }

                Set<String> after = admin.listTopics().names().get();
                long found = required.keySet().stream().filter(after::contains).count();
                return String.format("Topics ready (%d/%d): %s",
                                     found, required.size(), required.keySet());
            }
        }
    }
}
