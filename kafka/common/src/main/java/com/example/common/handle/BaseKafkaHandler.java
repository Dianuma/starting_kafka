package com.example.common.handle;

import com.example.common.model.kafka.dto.DeadLetterMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
//import io.micrometer.core.instrument.Counter;
//import io.micrometer.core.instrument.MeterRegistry;

@Slf4j
@Component
@RequiredArgsConstructor
public abstract class BaseKafkaHandler {

    private final KafkaTemplate<String, Object> kafkaTemplate;
//    private final MeterRegistry meterRegistry;

    @Value("${custom.config.kafka.dlq-suffix:.dlq}")
    private String dlqSuffix;

    // 공통 메시지 발행
    protected void sendMessage(String topic, String key, Object payload) {
        kafkaTemplate.send(topic, key, payload)
                .whenCompleteAsync((result, err) -> {
                    if (err == null) {
                        log.info("[Kafka Produce] topic={}, key={}, offset={}, partition={}",
                                topic, key,
                                result.getRecordMetadata().offset(),
                                result.getRecordMetadata().partition()
                        );
                    } else {
                        log.error("[Kafka Error] Failed to send message. topic={}, key={}, error={}",
                                topic, key, err.getMessage(), err);
                        handleError(topic, key, payload, err);
                    }
                });
    }

    // Kafka 메시지 수신 처리 (공통)
    protected void handleRecord(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        try {
            log.info("[Kafka Consume] topic={}, key={}, value={}",
                    record.topic(), record.key(), record.value());
            processMessage(record);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[Kafka Consume Error] topic={}, key={}, error={}",
                    record.topic(), record.key(), e.getMessage(), e);
            handleError(record, e);
        }
    }

    // 실제 도메인별 처리 로직
    protected abstract void processMessage(ConsumerRecord<String, Object> record) throws Exception;

    // Kafka 전송 실패 시 DLQ 처리
    private void handleError(String topic, String key, Object payload, Throwable ex) {
        try {
            String dlqTopic = topic + dlqSuffix;
            DeadLetterMessage dlqPayload = new DeadLetterMessage(topic, key, payload, ex.getMessage(), System.currentTimeMillis());
            kafkaTemplate.send(dlqTopic, key, dlqPayload)
                    .whenCompleteAsync((res, err) -> {
                        if (err == null) {
                            log.warn("[DLQ] Sent to {} for key={}", dlqTopic, key);
                            // TODO: Monitor DLQ sends
//                            Counter.builder("kafka_dlq_sent_total") Monitoring DLQ sends
//                                    .tag("topic", topic)
//                                    .register(meterRegistry)
//                                    .increment();
                        } else {
                            log.error("[DLQ Error] Failed to send to DLQ topic={}, key={}, error={}",
                                    dlqTopic, key, err.getMessage());
                            writeToLocalBackup(topic, key, payload, err);
                        }
                    });
        } catch (Exception e) {
            log.error("[DLQ Fatal] Could not process DLQ for topic={}, key={}", topic, key, e);
            writeToLocalBackup(topic, key, payload, e);
        }
    }

    private void writeToLocalBackup(String topic, String key, Object payload, Throwable e) {
        log.warn("⚠️ [DLQ Backup Placeholder] topic={}, key={}, error={}",
                topic, key, e.getMessage());
        // TODO: Implement Redis or DB fallback
    }

    private void handleError(ConsumerRecord<String, Object> record, Exception e) {
        handleError(record.topic(), record.key(), record.value(), e);
    }
}
