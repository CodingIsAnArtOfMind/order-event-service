package io.raza.orderevent.repository;

import io.raza.orderevent.outbox.OutboxEventEntity;
import io.raza.orderevent.outbox.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface OutboxEventRepository
        extends JpaRepository<OutboxEventEntity, Long> {

    List<OutboxEventEntity>
            findTop50ByStatusOrderByCreatedAtAsc(
                    OutboxStatus status
            );

    @Query(
            value = """
                SELECT *
                FROM order_event.outbox_events
                WHERE status = 'PENDING'
                ORDER BY created_at
                LIMIT 10
                FOR UPDATE SKIP LOCKED
                """,
            nativeQuery = true
    )
    List<OutboxEventEntity> findPendingAndLock();

    @Query(
            value = """
                SELECT *
                FROM order_event.outbox_events
                WHERE status = 'PENDING'
                   OR (
                        status = 'PROCESSING'
                        AND claimed_at < :staleBefore
                   )
                ORDER BY created_at
                LIMIT 10
                FOR UPDATE SKIP LOCKED
                """,
            nativeQuery = true
    )
    List<OutboxEventEntity> findClaimableAndLock(
            @Param("staleBefore") LocalDateTime staleBefore
    );

}