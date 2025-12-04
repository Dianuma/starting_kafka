package com.example.common.model.kafka.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeadLetterMessage {
    private String originalTopic;
    private String key;
    private Object payload;
    private String errorMessage;
    private Long timestamp;
}

