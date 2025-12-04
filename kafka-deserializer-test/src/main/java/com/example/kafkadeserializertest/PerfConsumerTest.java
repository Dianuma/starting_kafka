package com.example.kafkadeserializertest;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class PerfConsumerTest {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EnvelopeDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EnvelopeStreamDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, KafkaEventEnvelope<?>> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("test-topic"));

        long start = System.nanoTime();
        long count = 0;

        while (count < 100_000) {
            ConsumerRecords<String, KafkaEventEnvelope<?>> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, KafkaEventEnvelope<?>> r : records) {
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
