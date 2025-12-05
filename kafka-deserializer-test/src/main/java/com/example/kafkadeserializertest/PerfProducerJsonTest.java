package com.example.kafkadeserializertest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class PerfProducerJsonTest {
    private static final ObjectMapper mapper = new ObjectMapper();


    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");      // 성능 우선
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0"); // 지연 없이 바로 전송
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int total = 100_000;

        long start = System.nanoTime();

        for (int i = 0; i < total; i++) {

            // JSON Envelope 생성

            ObjectNode payload = mapper.createObjectNode();
            payload.put("value", i);
            payload.put("text", "This is a test message number " + i);

            String json = mapper.writeValueAsString(payload);

            producer.send(new ProducerRecord<>("test-topic-json", json));
        }

        producer.flush();
        producer.close();

        long end = System.nanoTime();
        double elapsedMs = (end - start) / 1_000_000.0;

        System.out.println("Produced " + total + " messages");
        System.out.println("Elapsed: " + elapsedMs + " ms");
    }

}
