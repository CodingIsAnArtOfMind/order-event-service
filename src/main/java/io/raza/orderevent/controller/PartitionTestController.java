package io.raza.orderevent.controller;

import io.raza.orderevent.event.OrderPlacedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/test/partitions")
@RequiredArgsConstructor
public class PartitionTestController {

    private final KafkaTemplate<Long, OrderPlacedEvent> kafkaTemplate;

    @PostMapping("/{key}")
    public List<String> sendSameKey(
            @PathVariable Long key,
            @RequestParam(defaultValue = "3") int count
    ) throws Exception {

        List<String> results = new ArrayList<>();

        for (int i = 1; i <= count; i++) {

            OrderPlacedEvent event = new OrderPlacedEvent(
                    UUID.randomUUID(),
                    key,           // test orderId
                    999L,          // customerId
                    5005L,         // normal productId
                    i,
                    LocalDateTime.now()
            );

            SendResult<Long, OrderPlacedEvent> result =
                    kafkaTemplate
                            .send(
                                    "order-events",
                                    key,
                                    event
                            )
                            .get(10, TimeUnit.SECONDS);

            int partition =
                    result.getRecordMetadata().partition();

            long offset =
                    result.getRecordMetadata().offset();

            String output =
                    "key=" + key
                            + " | eventId=" + event.eventId()
                            + " | partition=" + partition
                            + " | offset=" + offset;

            System.out.println(
                    "PARTITION TEST | " + output
            );

            results.add(output);
        }

        return results;
    }
}