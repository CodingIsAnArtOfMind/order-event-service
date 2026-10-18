package io.raza.orderevent.outbox;

public enum OutboxStatus {
    PENDING,
    PROCESSING,
    PUBLISHED
}