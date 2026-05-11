package com.workshop.flink.common.util;

import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.serde.TradeEventDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public final class KafkaSourceFactory {

    public static KafkaSource<TradeEvent> standard(String bootstrap, String topic, String groupId) {
        return KafkaSource.<TradeEvent>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new TradeEventDeserializationSchema())
            .build();
    }

    /**
     * Source that only reads committed Kafka transactions. Required when the upstream
     * producer uses EXACTLY_ONCE delivery guarantee (scenario-02 onward).
     */
    public static KafkaSource<TradeEvent> readCommitted(String bootstrap, String topic, String groupId) {
        return KafkaSource.<TradeEvent>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new TradeEventDeserializationSchema())
            .setProperty("isolation.level", "read_committed")
            .build();
    }

    private KafkaSourceFactory() {}
}
