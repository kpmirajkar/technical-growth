package com.technicalgrowth.notificationservice;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {

    @KafkaListener(topics = "inventory.result", groupId = "notification-service")
    public void onInventoryResult(InventoryResultEvent event) {
        if ("InventoryReserved".equals(event.eventType())) {
            System.out.printf(
                    "[notification-service] EMAIL to %s: Your order %s for %s is confirmed!%n",
                    event.customerId(), event.orderId(), event.sku());
        } else {
            System.out.printf(
                    "[notification-service] EMAIL to %s: Sorry, order %s failed (%s).%n",
                    event.customerId(), event.orderId(), event.reason());
        }
    }
}
