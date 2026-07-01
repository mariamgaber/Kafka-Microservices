package com.kafka.notification.consumers;

import com.kafka.notification.dtos.ProductCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

@Component
@Slf4j
@RequiredArgsConstructor
public class ProductConsumer {
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "product-created-events-topic", autoStartup = "true")
    public void handle(byte [] message) throws IOException {
            ProductCreatedEvent event = objectMapper.readValue(message, ProductCreatedEvent.class);
            log.info("Received Event: {}", event);

        }
}
