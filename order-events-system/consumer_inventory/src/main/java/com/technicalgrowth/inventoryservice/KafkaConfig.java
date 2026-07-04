package com.technicalgrowth.inventoryservice;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
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

    // Must match orders.created's partition count (producer's KafkaTopicConfig):
    // the recoverer below routes each failed record to the *same partition
    // number* on the DLQ topic, so the DLQ needs at least as many partitions.
    private static final int PARTITIONS = 3;

    @Bean
    public NewTopic ordersCreatedDlqTopic() {
        return TopicBuilder.name("orders.created.dlq").partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryResultTopic() {
        return TopicBuilder.name("inventory.result").partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template,
                (record, ex) -> new TopicPartition("orders.created.dlq", record.partition())
        );
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
    }
}
