package io.raza.orderevent.outbox;

import io.raza.orderevent.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class OutboxStatusService {

    private final OutboxEventRepository outboxEventRepository;

    @Transactional
    public void markPublished(Long id) {

        OutboxEventEntity outbox =
                outboxEventRepository.findById(id)
                        .orElseThrow();

        outbox.setStatus(OutboxStatus.PUBLISHED);
        outbox.setPublishedAt(LocalDateTime.now());

        System.out.println(
                "OUTBOX PUBLISHED"
                        + " | eventId=" + outbox.getEventId()
        );
    }
}