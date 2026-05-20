package com.workshop.flink.common.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.workshop.flink.common.model.FxRate;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class FxRateDeserializationSchema
        implements KafkaRecordDeserializationSchema<FxRate> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<FxRate> out) throws IOException {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        if (record.value() != null) {
            out.collect(mapper.readValue(record.value(), FxRate.class));
        }
    }

    @Override
    public TypeInformation<FxRate> getProducedType() {
        return TypeExtractor.getForClass(FxRate.class);
    }
}
