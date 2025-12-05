package com.example.common.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaCommonConfig {

    @Value("${custom.config.kafka.acks}")
    private String ack;

    @Value("${custom.config.kafka.max-retries}")
    private Integer maxRetries;

    @Value("${custom.config.kafka.retry-delay-ms}")
    private Integer retryDelayMs;

    @Value("${custom.config.kafka.linger-ms}")
    private Integer lingerMs;

    @Value("${custom.config.kafka.batch-size}")
    private Integer batchSize;

    @Value("${custom.config.kafka.buffer-memory}")
    private Integer bufferMemory;

    @Value("${custom.config.kafka.in-flight-requests}")
    private Integer inFlightRequests;

    @Value("${custom.config.kafka.enable-idempotence}")
    private Boolean enableIdempotence;

    // Producer Configuration
    @Bean
    public ProducerFactory<String, Object> producerFactory(KafkaProperties properties) {
        Map<String, Object> config = new HashMap<>(properties.buildProducerProperties());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, ack);
//        config.put(ProducerConfig.RETRIES_CONFIG, maxRetries);
//        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryDelayMs);
//        config.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
//        config.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
//        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
//        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, inFlightRequests);
//        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        return new DefaultKafkaProducerFactory<>(config);
    }

    // Consumer Configuration
    @Bean
    public ConsumerFactory<String, Object> consumerFactory(KafkaProperties properties) {
        Map<String, Object> config = new HashMap<>(properties.buildConsumerProperties());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
//        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
//        config.put(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> factory) {
        return new KafkaTemplate<>(factory);
    }

    // DLQ Configuration

}
