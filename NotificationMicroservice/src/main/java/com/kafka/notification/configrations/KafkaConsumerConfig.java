package com.kafka.notification.configrations;

import com.kafka.notification.error.NotRetryableException;
import com.kafka.notification.error.RetryableException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, byte[]> consumerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    //ياخد الـ ConsumerFactory ويستخدمه عشان يعمل Listener Container.
    // والـ Listener Container هو اللي:
    //يشغل الـ Consumer.
    //يقعد يسمع الـ Topic.
    //أول ما رسالة تيجي، ينادي:
    // @KafkaListener//public void handle(...) {}




    //→ consumer يستقبل value كـ byte[]
    //→ ObjectMapper يفشل في التحويل
    //→ DefaultErrorHandler يلقط الخطأ
    //→ DeadLetterPublishingRecoverer يستخدم KafkaTemplate
    //→ يرسل نفس الـ byte[] للـ DLT
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]>
    kafkaListenerContainerFactory(ConsumerFactory<String, byte[]> consumerFactory, KafkaTemplate<String, byte[]> kafkaTemplate) {
        //Container Factory object that used to configure the behavior of kafka listener
        // using the config of consumer above
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        // default dead letter topic name is topicName-dlt
        // interval = num of seconds tp wait before try again, maxAttempts = max num of retries
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate), new FixedBackOff(500, 3));
        errorHandler.addNotRetryableExceptions(NotRetryableException.class);
        errorHandler.addRetryableExceptions(RetryableException.class);
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }



    @Bean
    public ProducerFactory<String, byte[]> byteProducerFactory() {
        Map<String, Object>config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }
    @Bean
    public KafkaTemplate<String, byte[]> byteKafkaTemplate(ProducerFactory<String, byte[]> byteProducerFactory) {
        return new KafkaTemplate<>(byteProducerFactory);
    }
}
