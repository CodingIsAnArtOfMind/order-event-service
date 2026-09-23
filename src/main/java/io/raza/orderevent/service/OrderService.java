package io.raza.orderevent.service;

import io.raza.orderevent.dto.CreateOrderRequest;
import io.raza.orderevent.event.OrderPlacedEvent;
import io.raza.orderevent.dto.OrderResponse;
import io.raza.orderevent.entity.OrderEntity;
import io.raza.orderevent.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<Long, OrderPlacedEvent> kafkaTemplate;

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {

        OrderEntity order = OrderEntity.builder()
                .customerId(request.customerId())
                .productId(request.productId())
                .quantity(request.quantity())
                .status("PLACED")
                .createdAt(LocalDateTime.now())
                .build();

        OrderEntity savedOrder = orderRepository.save(order);
        OrderPlacedEvent event = new OrderPlacedEvent(
                savedOrder.getId(),
                savedOrder.getCustomerId(),
                savedOrder.getProductId(),
                savedOrder.getQuantity(),
                savedOrder.getCreatedAt()
        );

        kafkaTemplate.send(
                "order-events",
                savedOrder.getId(),
                event
        );


        return new OrderResponse(
                savedOrder.getId(),
                savedOrder.getCustomerId(),
                savedOrder.getProductId(),
                savedOrder.getQuantity(),
                savedOrder.getStatus(),
                savedOrder.getCreatedAt()
        );


    }
}