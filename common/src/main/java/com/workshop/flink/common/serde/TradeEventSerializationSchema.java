package com.workshop.flink.common.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.workshop.flink.common.model.TradeEvent;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public class TradeEventSerializationSchema
        implements KafkaRecordSerializationSchema<TradeEvent> {

    private static final long serialVersionUID = 1L;

    private final String topic;
    private transient ObjectMapper mapper;

    public TradeEventSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context,
                     KafkaSinkContext sinkContext) throws Exception {
        mapper = new ObjectMapper();
    }

    @Override
    @Nullable
    public ProducerRecord<byte[], byte[]> serialize(TradeEvent element,
                                                     KafkaSinkContext context,
                                                     @Nullable Long timestamp) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            // Key = eventId bytes — ensures the same eventId always routes to the same partition.
            // This is load-bearing for scenario-04 dedup (KeyedProcessFunction requires same key
            // lands on the same subtask).
            byte[] key   = element.getEventId().getBytes(StandardCharsets.UTF_8);
            byte[] value = mapper.writeValueAsBytes(element);
            return new ProducerRecord<>(topic, key, value);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize TradeEvent: " + element, e);
        }
    }
}
