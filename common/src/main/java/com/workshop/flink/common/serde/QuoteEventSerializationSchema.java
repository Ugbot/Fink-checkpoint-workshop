package com.workshop.flink.common.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.workshop.flink.common.model.QuoteEvent;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public class QuoteEventSerializationSchema
        implements KafkaRecordSerializationSchema<QuoteEvent> {

    private static final long serialVersionUID = 1L;

    private final String topic;
    private transient ObjectMapper mapper;

    public QuoteEventSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context,
                     KafkaSinkContext sinkContext) throws Exception {
        mapper = new ObjectMapper();
    }

    @Override
    @Nullable
    public ProducerRecord<byte[], byte[]> serialize(QuoteEvent element,
                                                    KafkaSinkContext context,
                                                    @Nullable Long timestamp) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            // Key = ticker so all quotes for the same ticker land on the same partition.
            // This is required for the interval/window joins to align with the trade stream.
            byte[] key   = element.getTicker().getBytes(StandardCharsets.UTF_8);
            byte[] value = mapper.writeValueAsBytes(element);
            return new ProducerRecord<>(topic, key, value);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize QuoteEvent: " + element, e);
        }
    }
}
