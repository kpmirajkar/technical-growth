package com.technicalgrowth.orderservice;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEvent, String> {

    // created_at order keeps the relay roughly FIFO; within one poller thread
    // that preserves per-key publish order.
    List<OutboxEvent> findTop50ByPublishedAtIsNullOrderByCreatedAtAsc();
}
