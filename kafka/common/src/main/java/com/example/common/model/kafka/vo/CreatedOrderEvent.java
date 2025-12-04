package com.example.common.model.kafka.vo;

public record CreatedOrderEvent (
        Long orderId,
        Long productId,
        String productName,
        int quantity,
        Long price
) {
}
