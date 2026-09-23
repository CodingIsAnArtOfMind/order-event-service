package io.raza.orderevent.event;

import java.time.LocalDateTime;

public record OrderPlacedEvent(
        Long orderId,
        Long customerId,
        Long productId,
        Integer quantity,
        LocalDateTime placedAt
) {
}