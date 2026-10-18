package io.raza.orderevent.outbox;

import io.raza.orderevent.event.OrderPlacedEvent;
import io.raza.orderevent.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class OutboxBatchProcessor {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<Long, OrderPlacedEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // Keep this only for our old 2222 POC simulation.
    private final Set<UUID> crashAfterAckOnce =
            ConcurrentHashMap.newKeySet();

    @Transactional
    public void publishBatch() {

        List<OutboxEventEntity> events =
                outboxEventRepository.findPendingAndLock();

        if (events.isEmpty()) {
            return;
        }

        System.out.println(
                "LOCKED OUTBOX BATCH"
                        + " | size=" + events.size()
        );

        for (OutboxEventEntity outbox : events) {

            try {

                OrderPlacedEvent event =
                        objectMapper.readValue(
                                outbox.getPayload(),
                                OrderPlacedEvent.class
                        );

                System.out.println(
                        "OUTBOX PUBLISH ATTEMPT"
                                + " | eventId=" + outbox.getEventId()
                                + " | orderId=" + outbox.getAggregateId()
                );

                // Existing multi-instance race simulation.
                if (event.productId().equals(1111L)) {

                    System.out.println(
                            "SIMULATING SLOW OUTBOX PUBLISH"
                                    + " | eventId="
                                    + outbox.getEventId()
                    );

                    Thread.sleep(5000);
                }

                kafkaTemplate
                        .send(
                                outbox.getTopic(),
                                outbox.getEventKey(),
                                event
                        )
                        .get(10, TimeUnit.SECONDS);

                System.out.println(
                        "KAFKA ACK RECEIVED"
                                + " | eventId="
                                + outbox.getEventId()
                );

                // Keep old 2222 failure simulation.
                if (event.productId().equals(2222L)
                        && crashAfterAckOnce.add(
                                outbox.getEventId())) {

                    System.out.println(
                            "SIMULATED CRASH AFTER KAFKA ACK"
                                    + " | BEFORE OUTBOX PUBLISHED"
                                    + " | eventId="
                                    + outbox.getEventId()
                    );

                    throw new RuntimeException(
                            "Simulated crash after Kafka ACK before DB update"
                    );
                }

                outbox.setStatus(OutboxStatus.PUBLISHED);
                outbox.setPublishedAt(LocalDateTime.now());

                /*
                 * Because the entity is managed inside this transaction,
                 * Hibernate dirty checking will update it on commit.
                 *
                 * save() is not strictly required here.
                 */
                System.out.println(
                        "OUTBOX PUBLISHED"
                                + " | eventId="
                                + outbox.getEventId()
                                + " | orderId="
                                + outbox.getAggregateId()
                );

            } catch (Exception e) {

                System.out.println(
                        "OUTBOX PUBLISH FAILED"
                                + " | eventId="
                                + outbox.getEventId()
                                + " | reason="
                                + e.getMessage()
                );
            }
        }
    }
}