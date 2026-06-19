package com.alarm.config

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Configuration

/**
 * [파일명] KafkaClientIdConfig.kt
 * [기능] Kafka Client ID 구성을 확인하고 로깅하는 설정 클래스
 */
@Configuration
class KafkaClientIdConfig {
    private val log = LoggerFactory.getLogger(KafkaClientIdConfig::class.java)

    init {
        log.info("Kafka client.id initialized: KAFKA_CLIENT_ID='{}', KAFKA_PRODUCER_CLIENT_ID='{}'",
            System.getProperty("KAFKA_CLIENT_ID") ?: "default",
            System.getProperty("KAFKA_PRODUCER_CLIENT_ID") ?: "default"
        )
    }
}
