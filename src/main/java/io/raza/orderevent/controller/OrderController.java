package io.raza.orderevent.controller;

import io.raza.orderevent.dto.CreateOrderRequest;
import io.raza.orderevent.dto.OrderResponse;
import io.raza.orderevent.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderResponse createOrder(
            @RequestBody CreateOrderRequest request) {

        return orderService.createOrder(request);
    }
}