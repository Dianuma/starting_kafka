package com.example.kafkadeserializertest;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class PerfConsumerJsonTest {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); // 그룹 아이디 랜덤생성 안하면 같은 그룹으로 묶여서 처리되서 오프셋 때문에 꼬임
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, KafkaEventEnvelope.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        KafkaConsumer<String, KafkaEventEnvelope> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("test-topic"));

        long start = System.nanoTime();
        long count = 0;

        while (count < 100_000) {
            ConsumerRecords<String, KafkaEventEnvelope> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, KafkaEventEnvelope> r : records) {
                try {
                    Class<?> payloadClass = Class.forName(r.value().payloadType());
                    Object payload = r.value().payload();
                } catch ( Exception e ) {
                    e.printStackTrace();
                }
                count++;
                if (count % 10000 == 0) {
                    System.out.println(r.value());
                }
            }

        }

        long end = System.nanoTime();
        double elapsedMs = (end - start) / 1_000_000.0;

        System.out.println("Processed: " + count);
        System.out.println("Elapsed: " + elapsedMs + " ms");

    }
}
