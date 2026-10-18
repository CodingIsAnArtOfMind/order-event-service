package io.raza.orderevent.service;

import io.raza.orderevent.dto.CreateOrderRequest;
import io.raza.orderevent.event.OrderPlacedEvent;
import io.raza.orderevent.dto.OrderResponse;
import io.raza.orderevent.entity.OrderEntity;
import io.raza.orderevent.outbox.OutboxEventEntity;
import io.raza.orderevent.outbox.OutboxStatus;
import io.raza.orderevent.repository.OrderRepository;
import io.raza.orderevent.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {

    private static final String ORDER_EVENTS_TOPIC = "order-events";
    private static final String ORDER_PLACED = "ORDER_PLACED";
    private static final String ORDER = "ORDER";

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

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
        UUID eventId = UUID.randomUUID();

        OrderPlacedEvent event =
                new OrderPlacedEvent(
                        eventId,
                        savedOrder.getId(),
                        savedOrder.getCustomerId(),
                        savedOrder.getProductId(),
                        savedOrder.getQuantity(),
                        savedOrder.getCreatedAt()
                );

        String payload = serializeEvent(event);

        OutboxEventEntity outboxEvent =
                OutboxEventEntity.builder()
                        .eventId(eventId)
                        .aggregateId(savedOrder.getId())
                        .aggregateType(ORDER)
                        .eventType(ORDER_PLACED)
                        .topic(ORDER_EVENTS_TOPIC)
                        .eventKey(savedOrder.getId())
                        .payload(payload)
                        .status(OutboxStatus.PENDING)
                        .createdAt(LocalDateTime.now())
                        .build();

        outboxEventRepository.save(outboxEvent);

        System.out.println(
                "ORDER + OUTBOX SAVED"
                        + " | orderId=" + savedOrder.getId()
                        + " | eventId=" + eventId
                        + " | outboxStatus=PENDING"
        );

        return toResponse(savedOrder);
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

    private String serializeEvent(OrderPlacedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize event", e);
        }
    }

}