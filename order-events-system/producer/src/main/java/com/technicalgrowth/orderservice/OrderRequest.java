package com.technicalgrowth.orderservice;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record OrderRequest(
        @JsonProperty("customer_id") @NotBlank String customerId,
        @JsonProperty("sku") @NotBlank String sku,
        @JsonProperty("quantity") @Min(1) int quantity
) {
}
