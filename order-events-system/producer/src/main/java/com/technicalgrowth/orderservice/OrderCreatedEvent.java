package com.technicalgrowth.orderservice;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Event published to the "orders.created" topic.
 *
 * Note: this class is duplicated (with the same shape) in inventory-service.
 * That duplication is intentional for Week 1-2 — it's the pain point that
 * motivates introducing a schema registry / shared event contract in Week 3.
 */
public record OrderCreatedEvent(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("occurred_at") String occurredAt,
        @JsonProperty("order_id") String orderId,
        @JsonProperty("customer_id") String customerId,
        @JsonProperty("sku") String sku,
        @JsonProperty("quantity") int quantity
) {
}
