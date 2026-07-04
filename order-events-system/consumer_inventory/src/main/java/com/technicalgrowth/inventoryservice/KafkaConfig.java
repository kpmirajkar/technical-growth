package com.technicalgrowth.inventoryservice;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Retry + dead-letter wiring for the "orders.created" consumer.
 * On failure (e.g. malformed/undeserializable event), Spring retries twice
 * with a 1s backoff, then republishes the raw record to "orders.created.dlq"
 * instead of blocking the partition or dropping the message.
 */
@Configuration
public class KafkaConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template,
                (record, ex) -> new TopicPartition("orders.created.dlq", record.partition())
        );
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
    }
}
