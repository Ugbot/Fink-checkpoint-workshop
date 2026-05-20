package com.workshop.flink.common.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Generic JSON Kafka serializer. The key selector returns the Kafka message key
 * (e.g., eventId for routing/colocation); the value is the Jackson-serialized object.
 *
 * KeySelector is a Flink-provided {@link Serializable} functional interface, so this
 * schema is itself fully serializable.
 */
public class JsonRecordSerializationSchema<T> implements KafkaRecordSerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final String topic;
    private final KeySelector<T, String> keySelector;
    private transient ObjectMapper mapper;

    public JsonRecordSerializationSchema(String topic, KeySelector<T, String> keySelector) {
        this.topic       = topic;
        this.keySelector = keySelector;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context,
                     KafkaSinkContext sinkContext) throws Exception {
        mapper = new ObjectMapper();
    }

    @Override
    @Nullable
    public ProducerRecord<byte[], byte[]> serialize(T element,
                                                    KafkaSinkContext context,
                                                    @Nullable Long timestamp) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            String keyStr = keySelector.getKey(element);
            byte[] key    = keyStr == null ? null : keyStr.getBytes(StandardCharsets.UTF_8);
            byte[] value  = mapper.writeValueAsBytes(element);
            return new ProducerRecord<>(topic, key, value);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize element: " + element, e);
        } catch (Exception e) {
            throw new RuntimeException("Key selector failed for: " + element, e);
        }
    }
}
