package com.technicalgrowth.orderservice;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Relays pending outbox rows to Kafka. This is the "polling publisher" flavor
 * of the outbox pattern — production systems often use CDC (Debezium tailing
 * the WAL) instead, which avoids polling entirely.
 *
 * Delivery is at-least-once by construction: a crash after send() but before
 * the published_at update means the row is re-sent next tick, and the
 * consumer's dedup absorbs it. Sends are synchronous (.get()) one at a time,
 * which keeps per-key ordering and stops the batch at the first broker error
 * so nothing is marked published that wasn't acked.
 */
@Component
public class OutboxPublisher {

    private static final String TOPIC = "orders.created";

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(OutboxRepository outbox, KafkaTemplate<String, String> kafkaTemplate) {
        this.outbox = outbox;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publishPending() {
        List<OutboxEvent> pending = outbox.findTop50ByPublishedAtIsNullOrderByCreatedAtAsc();
        for (OutboxEvent event : pending) {
            try {
                kafkaTemplate.send(TOPIC, event.getEventKey(), event.getPayload()).get();
                // No explicit save(): `event` is a managed entity (loaded
                // inside this @Transactional method), so Hibernate's dirty
                // checking flushes this change as an UPDATE at commit —
                // only works because it never leaves this transaction.
                event.setPublishedAt(Instant.now());
            } catch (Exception e) {
                System.out.printf("[order-service] outbox publish failed for event_id=%s, will retry: %s%n",
                        event.getEventId(), e.getMessage());
                break;
            }
        }
    }
}
