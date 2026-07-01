package com.kafka.products.service;

import java.util.UUID;

import com.kafka.products.dtos.ProductCreatedEvent;
import com.kafka.products.dtos.CreateProductRestModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ProductServiceImpl implements ProductService {

	KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
	public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@Override
	public String createProduct(CreateProductRestModel productRestModel) throws Exception {
		
		String productId = UUID.randomUUID().toString();
		ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
				productRestModel.getTitle(), productRestModel.getPrice(), 
				productRestModel.getQuantity());
		
		log.info("Before publishing a ProductCreatedEvent");
		
		SendResult<String, ProductCreatedEvent> result = 
				kafkaTemplate.send("product-created-events-topic",productId, productCreatedEvent).get();

        log.info("Partition: {}", result.getRecordMetadata().partition());
        log.info("Topic: {}", result.getRecordMetadata().topic());
        log.info("Offset: {}", result.getRecordMetadata().offset());
		
		log.info("***** Returning product id");
		
		return productId;
	}

}
