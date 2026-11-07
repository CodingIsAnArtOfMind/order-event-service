package io.raza.orderevent.idempotency;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.Optional;

public interface ApiIdempotencyRepository
        extends JpaRepository<ApiIdempotencyEntity, Long> {

    Optional<ApiIdempotencyEntity> findByIdempotencyKey(
            String idempotencyKey
    );

    @Modifying
    @Query(
            value = """
                    INSERT INTO order_event.api_idempotency
                        (idempotency_key, status, created_at)
                    VALUES
                        (:key, 'PROCESSING', :createdAt)
                    ON CONFLICT (idempotency_key)
                    DO NOTHING
                    """,
            nativeQuery = true
    )
    int tryClaim(
            @Param("key") String key,
            @Param("createdAt") LocalDateTime createdAt
    );
}