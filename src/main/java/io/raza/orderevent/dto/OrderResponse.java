package io.raza.orderevent.dto;

import java.time.LocalDateTime;

public record OrderResponse(
        Long id,
        Long customerId,
        Long productId,
        Integer quantity,
        String status,
        LocalDateTime createdAt
) {
}