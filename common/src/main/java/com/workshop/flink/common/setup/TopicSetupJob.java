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

import java.util.Arrays;
import java.util.List;
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
            List<String> required = Arrays.asList(
                Constants.TOPIC_IN, Constants.TOPIC_MID, Constants.TOPIC_OUT);

            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

            try (AdminClient admin = AdminClient.create(props)) {
                Set<String> existing = admin.listTopics().names().get();

                List<NewTopic> toCreate = required.stream()
                    .filter(t -> !existing.contains(t))
                    .map(t -> new NewTopic(t, partitions, (short) 1))
                    .collect(Collectors.toList());

                if (toCreate.isEmpty()) {
                    LOG.info("All topics already exist: {}", required);
                } else {
                    admin.createTopics(toCreate).all().get();
                    toCreate.forEach(t -> LOG.info("Created topic {} ({} partitions)", t.name(), partitions));
                }

                Set<String> after = admin.listTopics().names().get();
                long found = required.stream().filter(after::contains).count();
                return String.format("Topics ready (%d/%d): %s", found, required.size(), required);
            }
        }
    }
}
