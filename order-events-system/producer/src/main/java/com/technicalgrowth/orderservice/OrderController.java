package com.technicalgrowth.orderservice;

import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
public class OrderController {

    private static final String TOPIC = "orders.created";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/orders")
    public Map<String, Object> createOrder(@Valid @RequestBody OrderRequest request) throws Exception {
        String eventId = UUID.randomUUID().toString();

        OrderCreatedEvent event = new OrderCreatedEvent(
                eventId,
                "OrderCreated",
                Instant.now().toString(),
                UUID.randomUUID().toString(),
                request.customerId(),
                request.sku(),
                request.quantity()
        );

        // Key by customer_id so all events for a customer land on the same
        // partition — this preserves per-customer ordering.
        kafkaTemplate.send(TOPIC, request.customerId(), event).get();

        return Map.of("status", "published", "event", event);
    }
}
