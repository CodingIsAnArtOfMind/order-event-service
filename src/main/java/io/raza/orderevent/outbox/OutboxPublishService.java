package io.raza.orderevent.outbox;

import io.raza.orderevent.event.OrderPlacedEvent;
import io.raza.orderevent.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class OutboxPublishService {

    private final KafkaTemplate<Long, OrderPlacedEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxEventRepository outboxEventRepository;
    private final OutboxStatusService outboxStatusService;

    public void publish(OutboxEventEntity outbox) {

        try {

            OrderPlacedEvent event =
                    objectMapper.readValue(
                            outbox.getPayload(),
                            OrderPlacedEvent.class
                    );

            System.out.println(
                    "OUTBOX PUBLISH ATTEMPT"
                            + " | eventId=" + outbox.getEventId()
                            + " | claimedBy=" + outbox.getClaimedBy()
            );

            if (event.productId().equals(1111L)) {

                System.out.println(
                        "SIMULATING SLOW KAFKA PUBLISH"
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

            outboxStatusService.markPublished(outbox.getId());

        } catch (Exception e) {

            System.out.println(
                    "OUTBOX PUBLISH FAILED"
                            + " | eventId="
                            + outbox.getEventId()
                            + " | reason="
                            + e.getMessage()
            );

            /*
             * IMPORTANT:
             *
             * For now we intentionally leave:
             *
             * status = PROCESSING
             *
             * We will solve stale PROCESSING recovery next.
             */
        }
    }

}