package com.kafka.products.service;

import java.util.UUID;

import com.kafka.products.dtos.ProductCreatedEvent;
import com.kafka.products.dtos.CreateProductRestModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ProductServiceImpl implements ProductService {
	private static final String TOPIC = "product-created-events-topic";
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
		ProducerRecord<String, ProductCreatedEvent> producerRecord = new ProducerRecord<>(TOPIC, productId, productCreatedEvent);
	    // for idempotency
		// add new header for learning, but we can use the product id as a message id cause its unique id
		producerRecord.headers().add("messageId", "505".getBytes());
		log.info("Before sending a ProductCreatedEvent");
		SendResult<String, ProductCreatedEvent> result = kafkaTemplate.send(producerRecord).get();
        log.info("Successfully Sent a Message with Key: {} in Topic: {} Partition: {} and Offset: {}",
				productId,result.getRecordMetadata().topic(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset() );
		return productId;
	}

}
