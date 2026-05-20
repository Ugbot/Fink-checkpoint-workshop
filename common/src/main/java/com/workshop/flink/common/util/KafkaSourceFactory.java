package com.workshop.flink.common.util;

import com.workshop.flink.common.model.FxRate;
import com.workshop.flink.common.model.QuoteEvent;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.serde.FxRateDeserializationSchema;
import com.workshop.flink.common.serde.QuoteEventDeserializationSchema;
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

    public static KafkaSource<QuoteEvent> quotes(String bootstrap, String topic, String groupId) {
        return KafkaSource.<QuoteEvent>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new QuoteEventDeserializationSchema())
            .build();
    }

    public static KafkaSource<QuoteEvent> quotesReadCommitted(String bootstrap, String topic, String groupId) {
        return KafkaSource.<QuoteEvent>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new QuoteEventDeserializationSchema())
            .setProperty("isolation.level", "read_committed")
            .build();
    }

    public static KafkaSource<FxRate> fxRates(String bootstrap, String topic, String groupId) {
        return KafkaSource.<FxRate>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new FxRateDeserializationSchema())
            .build();
    }

    private KafkaSourceFactory() {}
}
