package io.raza.orderevent.outbox;

import io.raza.orderevent.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OutboxClaimService {

    private static final Duration CLAIM_LEASE =
            Duration.ofSeconds(30);

    private final OutboxEventRepository outboxEventRepository;

    private final String workerId =
            UUID.randomUUID().toString();

    @Transactional
    public List<OutboxEventEntity> claimBatch() {

        LocalDateTime now = LocalDateTime.now();

        LocalDateTime staleBefore =
                now.minus(CLAIM_LEASE);

        List<OutboxEventEntity> events =
                outboxEventRepository
                        .findClaimableAndLock(staleBefore);

        if (events.isEmpty()) {
            return List.of();
        }

        for (OutboxEventEntity event : events) {

            boolean reclaimed =
                    event.getStatus()
                            == OutboxStatus.PROCESSING;

            event.setStatus(OutboxStatus.PROCESSING);
            event.setClaimedAt(now);
            event.setClaimedBy(workerId);

            if (reclaimed) {

                System.out.println(
                        "OUTBOX RECLAIMED"
                                + " | worker=" + workerId
                                + " | eventId="
                                + event.getEventId()
                );

            } else {

                System.out.println(
                        "OUTBOX CLAIMED"
                                + " | worker=" + workerId
                                + " | eventId="
                                + event.getEventId()
                );
            }
        }

        return events;
    }
}