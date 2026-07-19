package com.technicalgrowth.orderservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
public class OrderController {

    private final OrderRepository orders;
    private final OutboxRepository outbox;
    private final ObjectMapper objectMapper;

    public OrderController(OrderRepository orders, OutboxRepository outbox, ObjectMapper objectMapper) {
        this.orders = orders;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
    }

    /**
     * Transactional outbox: the order row and its OrderCreated event are
     * written in ONE database transaction — no dual write to DB + Kafka.
     * OutboxPublisher relays the event to Kafka within ~500ms. HTTP 200 here
     * means "order accepted and durably recorded", not "event published".
     */
    @PostMapping("/orders")
    @Transactional
    public Map<String, Object> createOrder(@Valid @RequestBody OrderRequest request) throws JsonProcessingException {
        String orderId = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        OrderEntity order = new OrderEntity(
                orderId, request.customerId(), request.sku(), request.quantity(), "PLACED", now);
        orders.save(order);

        OrderCreatedEvent event = new OrderCreatedEvent(
                eventId,
                "OrderCreated",
                now.toString(),
                orderId,
                request.customerId(),
                request.sku(),
                request.quantity()
        );
        outbox.save(new OutboxEvent(
                eventId, orderId, "OrderCreated", request.customerId(),
                objectMapper.writeValueAsString(event), now));

        return Map.of("status", "accepted", "event", event);
    }

    @GetMapping("/orders/{id}")
    public ResponseEntity<OrderEntity> getOrder(@PathVariable String id) {
        return orders.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
