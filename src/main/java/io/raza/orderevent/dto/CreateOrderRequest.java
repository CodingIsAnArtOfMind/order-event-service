package io.raza.orderevent.dto;

public record CreateOrderRequest(
        Long customerId,
        Long productId,
        Integer quantity
) {
}