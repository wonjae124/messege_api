package com.example.mocksend.api;

public record MockSendFailResponse(
        String result,
        String requestId,
        String channelType,
        String errorCode,
        String message,
        String processedAt,
        long latencyMs
) {
}
