package io.raza.orderevent.outbox;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(
        name = "outbox_events",
        schema = "order_event",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "uk_outbox_event_id",
                        columnNames = "event_id"
                )
        }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "aggregate_id", nullable = false)
    private Long aggregateId;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "topic", nullable = false)
    private String topic;

    @Column(name = "event_key", nullable = false)
    private Long eventKey;

    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private OutboxStatus status;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "published_at")
    private LocalDateTime publishedAt;

    @Column(name = "claimed_at")
    private LocalDateTime claimedAt;

    @Column(name = "claimed_by")
    private String claimedBy;
}