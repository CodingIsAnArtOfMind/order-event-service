package io.raza.orderevent.idempotency;

import io.raza.orderevent.dto.CreateOrderRequest;
import io.raza.orderevent.dto.OrderResponse;
import io.raza.orderevent.entity.OrderEntity;
import io.raza.orderevent.repository.OrderRepository;
import io.raza.orderevent.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class IdempotentOrderService {

    private final ApiIdempotencyRepository idempotencyRepository;
    private final OrderService orderService;
    private final OrderRepository orderRepository;

    @Transactional
    public OrderResponse createOrder(
            String idempotencyKey,
            CreateOrderRequest request
    ) {

        int claimed = idempotencyRepository.tryClaim(
                idempotencyKey,
                LocalDateTime.now()
        );

        // We are NOT the first request.
        if (claimed == 0) {

            ApiIdempotencyEntity existing =
                    idempotencyRepository
                            .findByIdempotencyKey(idempotencyKey)
                            .orElseThrow();

            if (existing.getStatus() == IdempotencyStatus.COMPLETED
                    && existing.getOrderId() != null) {

                System.out.println(
                        "IDEMPOTENT DUPLICATE"
                                + " | key=" + idempotencyKey
                                + " | returningOrderId="
                                + existing.getOrderId()
                );

                OrderEntity order =
                        orderRepository
                                .findById(existing.getOrderId())
                                .orElseThrow();

                return toResponse(order);
            }

            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "Request with this Idempotency-Key is already processing"
            );
        }

        System.out.println(
                "IDEMPOTENCY KEY CLAIMED"
                        + " | key=" + idempotencyKey
        );

        // Existing OrderService still performs:
        //
        // Order + Outbox
        // in its existing transaction.
        //
        // Because this outer method is also @Transactional,
        // Spring's default REQUIRED propagation means it joins
        // this same DB transaction.
        OrderResponse response =
                orderService.createOrder(request);

        ApiIdempotencyEntity idempotency =
                idempotencyRepository
                        .findByIdempotencyKey(idempotencyKey)
                        .orElseThrow();

        idempotency.setOrderId(response.id());
        idempotency.setStatus(IdempotencyStatus.COMPLETED);
        idempotency.setCompletedAt(LocalDateTime.now());

        idempotencyRepository.save(idempotency);

        System.out.println(
                "IDEMPOTENT REQUEST COMPLETED"
                        + " | key=" + idempotencyKey
                        + " | orderId=" + response.id()
        );

        return response;
    }

    private OrderResponse toResponse(OrderEntity order) {

        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getProductId(),
                order.getQuantity(),
                order.getStatus(),
                order.getCreatedAt()
        );
    }
}