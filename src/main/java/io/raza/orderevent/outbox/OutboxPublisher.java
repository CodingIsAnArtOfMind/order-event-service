package io.raza.orderevent.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxClaimService outboxClaimService;
    private final OutboxPublishService outboxPublishService;

    @Scheduled(fixedDelay = 2000)
    public void publishPendingEvents() {

        List<OutboxEventEntity> claimedEvents =
                outboxClaimService.claimBatch();

        for (OutboxEventEntity event : claimedEvents) {
            outboxPublishService.publish(event);
        }
    }
}