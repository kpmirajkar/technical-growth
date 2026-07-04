package com.technicalgrowth.inventoryservice;

import com.fasterxml.jackson.annotation.JsonProperty;

public record InventoryResultEvent(
        @JsonProperty("event_type") String eventType,
        @JsonProperty("order_id") String orderId,
        @JsonProperty("customer_id") String customerId,
        @JsonProperty("sku") String sku,
        @JsonProperty("quantity") Integer quantity,
        @JsonProperty("reason") String reason,
        @JsonProperty("occurred_at") String occurredAt
) {
}
