package com.technicalgrowth.orderservice;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Explicit multi-partition topic so keyed publishing (see OrderController) has
 * something to demonstrate — a single-partition topic makes any key equivalent.
 * Partition count must match consumer_inventory's orders.created.dlq (see its
 * KafkaConfig): failed records get republished to the same partition number.
 */
@Configuration
public class KafkaTopicConfig {

    private static final int PARTITIONS = 3;

    @Bean
    public NewTopic ordersCreatedTopic() {
        return TopicBuilder.name("orders.created").partitions(PARTITIONS).replicas(1).build();
    }
}
