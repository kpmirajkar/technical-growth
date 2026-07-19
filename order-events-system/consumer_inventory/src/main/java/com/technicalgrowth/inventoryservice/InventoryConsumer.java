package com.technicalgrowth.inventoryservice;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
public class InventoryConsumer {

    private static final String OUT_TOPIC = "inventory.result";

    private final ProcessedEventRepository processedEvents;
    private final StockRepository stock;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public InventoryConsumer(ProcessedEventRepository processedEvents,
                             StockRepository stock,
                             KafkaTemplate<Object, Object> kafkaTemplate) {
        this.processedEvents = processedEvents;
        this.stock = stock;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Idempotent, database-backed consumption: the dedup insert and the stock
     * decrement commit or roll back together. If two deliveries of the same
     * event race (redelivery, rebalance, or a second replica), one commits
     * and the other dies on the processed_events primary key — the DB
     * enforces what the old in-memory HashSet only promised.
     *
     * Known gap (deliberate, mirrors hop 1's pre-outbox state): the result
     * publish below happens inside the DB transaction — a crash between
     * send() and commit can emit a result for a rolled-back reservation.
     * Fixing it properly means an outbox here too; kept as a discussion
     * point for now.
     */
    @KafkaListener(topics = "orders.created", groupId = "inventory-service")
    @Transactional
    public void onOrderCreated(OrderCreatedEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            System.out.printf("[inventory-service] duplicate event_id=%s, skipping (idempotency)%n", event.eventId());
            return;
        }
        processedEvents.save(new ProcessedEvent(event.eventId(), Instant.now()));

        boolean reserved = stock.tryReserve(event.sku(), event.quantity()) == 1;

        InventoryResultEvent result = reserved
                ? new InventoryResultEvent("InventoryReserved", event.orderId(), event.customerId(),
                        event.sku(), event.quantity(), null, Instant.now().toString())
                : new InventoryResultEvent("InventoryFailed", event.orderId(), event.customerId(),
                        event.sku(), null, "insufficient_stock", Instant.now().toString());

        kafkaTemplate.send(OUT_TOPIC, event.customerId(), result);
        System.out.printf("[inventory-service] processed order_id=%s -> %s%n", event.orderId(), result.eventType());
    }
}
