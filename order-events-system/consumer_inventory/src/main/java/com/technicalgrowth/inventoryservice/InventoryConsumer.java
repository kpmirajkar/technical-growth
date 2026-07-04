package com.technicalgrowth.inventoryservice;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class InventoryConsumer {

    private static final String OUT_TOPIC = "inventory.result";

    // Simulated stock table — replace with a real database in production.
    private final Map<String, Integer> stock = new ConcurrentHashMap<>(Map.of(
            "WIDGET-1", 50,
            "WIDGET-2", 5,
            "WIDGET-3", 0
    ));

    // In-memory idempotency store — dedup by event_id. In production this
    // would be a DB unique constraint or an outbox table, not a HashSet.
    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public InventoryConsumer(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "orders.created", groupId = "inventory-service")
    public void onOrderCreated(OrderCreatedEvent event) {
        if (!processedEventIds.add(event.eventId())) {
            System.out.printf("[inventory-service] duplicate event_id=%s, skipping (idempotency)%n", event.eventId());
            return;
        }

        int available = stock.getOrDefault(event.sku(), 0);
        InventoryResultEvent result;

        if (available >= event.quantity()) {
            stock.put(event.sku(), available - event.quantity());
            result = new InventoryResultEvent(
                    "InventoryReserved",
                    event.orderId(),
                    event.customerId(),
                    event.sku(),
                    event.quantity(),
                    null,
                    Instant.now().toString()
            );
        } else {
            result = new InventoryResultEvent(
                    "InventoryFailed",
                    event.orderId(),
                    event.customerId(),
                    event.sku(),
                    null,
                    "insufficient_stock",
                    Instant.now().toString()
            );
        }

        // Key by customer_id, matching orders.created, so a customer's events
        // stay on the same partition end-to-end and preserve per-customer ordering.
        kafkaTemplate.send(OUT_TOPIC, event.customerId(), result);
        System.out.printf("[inventory-service] processed order_id=%s -> %s%n", event.orderId(), result.eventType());
    }
}
