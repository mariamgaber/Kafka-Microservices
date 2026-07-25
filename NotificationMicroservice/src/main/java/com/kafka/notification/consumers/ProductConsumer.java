package com.kafka.notification.consumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.notification.dtos.ProductCreatedEvent;
import com.kafka.notification.entities.ProcessedEventEntity;
import com.kafka.notification.error.NotRetryableException;
import com.kafka.notification.error.RetryableException;
import com.kafka.notification.repos.ProcessedEventRepo;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;


@Component
@Slf4j
@RequiredArgsConstructor
public class ProductConsumer {
    private final ObjectMapper objectMapper;
    private final ProcessedEventRepo processedEventRepo;
    private final RestTemplate  restTemplate;
    @Value("${get-product-url}")
    private String productUrl;

    @KafkaListener(topics = "product-created-events-topic")
    @Transactional
    public void handle(@Payload byte[] message, @Header("messageId") String messageId, @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {
        if (processedEventRepo.existsByMessageId(messageId)) {
            log.info("Duplicate message ignored. MessageId: {}", messageId);
            return;
        }
        try {
            ProductCreatedEvent event = objectMapper.readValue(message, ProductCreatedEvent.class);
            getProduct(event);
            processedEventRepo.save(new ProcessedEventEntity(messageId, messageKey));

        } catch (JsonProcessingException e) {
            throw new NotRetryableException("Invalid JSON message", e);
        } catch (DataIntegrityViolationException e) {
            throw new NotRetryableException("Duplicate message id or database constraint violation", e);
        } catch (RestClientException e) {
            throw new RetryableException("Remote service failed", e);
        } catch (Exception e) {
            throw new RetryableException("Unexpected error", e);
        }
    }

    private void getProduct(ProductCreatedEvent event) {
        String url = productUrl + event.getProductId();
        ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
        if(response.getStatusCode().value() != HttpStatus.OK.value()){
            throw new RetryableException("Remote service returned non-success status");
        }
        log.info("Received response from remote service: {}", response.getBody());
    }
}
