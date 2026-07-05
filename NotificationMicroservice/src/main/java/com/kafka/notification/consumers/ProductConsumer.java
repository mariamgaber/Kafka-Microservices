package com.kafka.notification.consumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.notification.dtos.ProductCreatedEvent;
import com.kafka.notification.error.NotRetryableException;
import com.kafka.notification.error.RetryableException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

@Component
@Slf4j
@RequiredArgsConstructor
public class ProductConsumer {
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "product-created-events-topic")
    public void handle(@Payload byte[] message, @Header("messageId") String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {
        try {
            ProductCreatedEvent event = objectMapper.readValue(message, ProductCreatedEvent.class);
            log.info("Received Event: {} with Key: {} and MessageId: {}", event, messageKey, messageId);

        } catch (JsonProcessingException e) {
            // if json message is wrong no way or retries can make it correct so it's not NotRetryable
            throw new NotRetryableException(e);
        } catch (IOException e) {
            throw new RetryableException(e);
        }
    }
}
