package com.technicalgrowth.notificationservice;

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
 * Retry + dead-letter wiring for the "inventory.result" consumer, mirroring
 * consumer_inventory's handling of "orders.created". Without this, a poison
 * message falls to Spring Kafka's default handler, which retries briefly and
 * then logs-and-skips — i.e. silently drops the record.
 */
@Configuration
public class KafkaConfig {

    // Must match inventory.result's partition count (consumer_inventory's
    // KafkaConfig): the recoverer routes each failed record to the same
    // partition number on the DLQ topic.
    private static final int PARTITIONS = 3;

    @Bean
    public NewTopic inventoryResultDlqTopic() {
        return TopicBuilder.name("inventory.result.dlq").partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template,
                (record, ex) -> new TopicPartition("inventory.result.dlq", record.partition())
        );
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
    }
}
