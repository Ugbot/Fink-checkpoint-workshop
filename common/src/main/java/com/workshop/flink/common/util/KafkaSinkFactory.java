package com.workshop.flink.common.util;

import com.workshop.flink.common.model.FxRate;
import com.workshop.flink.common.model.QuoteEvent;
import com.workshop.flink.common.model.TradeEvent;
import com.workshop.flink.common.serde.FxRateSerializationSchema;
import com.workshop.flink.common.serde.QuoteEventSerializationSchema;
import com.workshop.flink.common.serde.TradeEventSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.base.DeliveryGuarantee;

public final class KafkaSinkFactory {

    public static KafkaSink<TradeEvent> atLeastOnce(String bootstrap, String topic) {
        return KafkaSink.<TradeEvent>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(new TradeEventSerializationSchema(topic))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }

    /**
     * Exactly-once Kafka sink using two-phase commit. The transactional ID prefix must be
     * unique per job to avoid conflicts if multiple jobs write to the same topic. Flink
     * appends the subtask index and checkpoint ID to form the full transactional ID.
     *
     * Requires the Kafka broker to have:
     *   transaction.state.log.replication.factor=1 (single-broker local dev)
     *   transaction.state.log.min.isr=1
     */
    public static KafkaSink<TradeEvent> exactlyOnce(String bootstrap, String topic, String transactionalIdPrefix) {
        return KafkaSink.<TradeEvent>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(new TradeEventSerializationSchema(topic))
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix(transactionalIdPrefix)
            .build();
    }

    public static KafkaSink<QuoteEvent> quotesAtLeastOnce(String bootstrap, String topic) {
        return KafkaSink.<QuoteEvent>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(new QuoteEventSerializationSchema(topic))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }

    public static KafkaSink<FxRate> fxRatesAtLeastOnce(String bootstrap, String topic) {
        return KafkaSink.<FxRate>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(new FxRateSerializationSchema(topic))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }

    private KafkaSinkFactory() {}
}
