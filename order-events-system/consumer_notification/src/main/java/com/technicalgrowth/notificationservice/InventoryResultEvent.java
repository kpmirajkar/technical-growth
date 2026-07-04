package com.technicalgrowth.notificationservice;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
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
