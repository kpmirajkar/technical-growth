package com.technicalgrowth.orderservice;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * One row per domain event, written in the SAME transaction as the business
 * change it describes (see OrderController). OutboxPublisher relays pending
 * rows to Kafka afterwards — the two writes that can't be atomic across
 * DB + broker become one DB write plus an at-least-once relay.
 */
@Entity
@Table(name = "outbox")
public class OutboxEvent {

    @Id
    @Column(name = "event_id")
    private String eventId;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    // Kafka message key (customer_id) — preserved so the relay keeps the
    // same per-customer partition affinity as a direct publish would.
    @Column(name = "event_key", nullable = false)
    private String eventKey;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxEvent() {
    }

    public OutboxEvent(String eventId, String aggregateId, String eventType, String eventKey,
                       String payload, Instant createdAt) {
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.eventKey = eventKey;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventKey() {
        return eventKey;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public void setPublishedAt(Instant publishedAt) {
        this.publishedAt = publishedAt;
    }
}
