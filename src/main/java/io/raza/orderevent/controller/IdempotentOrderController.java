package io.raza.orderevent.controller;

import io.raza.orderevent.dto.CreateOrderRequest;
import io.raza.orderevent.dto.OrderResponse;
import io.raza.orderevent.idempotency.IdempotentOrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/idempotent/orders")
@RequiredArgsConstructor
public class IdempotentOrderController {

    private final IdempotentOrderService idempotentOrderService;

    @PostMapping
    public OrderResponse createOrder(
            @RequestHeader("Idempotency-Key")
            String idempotencyKey,

            @Valid
            @RequestBody
            CreateOrderRequest request
    ) {

        return idempotentOrderService.createOrder(
                idempotencyKey,
                request
        );
    }
}