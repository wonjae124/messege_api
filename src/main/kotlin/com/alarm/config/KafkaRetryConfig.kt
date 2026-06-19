package com.alarm.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.retrytopic.RetryTopicConfiguration
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder

@Configuration
class KafkaRetryConfig(
    @Value("\${spring.kafka.retry.initial-interval:3000}") private val initialInterval: Long,
    @Value("\${spring.kafka.retry.multiplier:2.0}") private val multiplier: Double,
    @Value("\${spring.kafka.retry.max-interval:30000}") private val maxInterval: Long,
    @Value("\${spring.kafka.retry.max-attempts:4}") private val maxAttempts: Int
) {

    @Bean
    fun retryTopicConfig(template: KafkaTemplate<String, String>): RetryTopicConfiguration {
        return RetryTopicConfigurationBuilder.newInstance()
            .exponentialBackoff(initialInterval, multiplier, maxInterval)
            .maxAttempts(maxAttempts)
            .notRetryOn( // 비즈니스 예외(예: IllegalArgumentException) 등 재시도할 필요가 없는 오류는 바로 DLT로 넘기기
                listOf(
                    IllegalArgumentException::class.java,
                    IllegalStateException::class.java
                )
            )
            .dltHandlerMethod("dltProcessor", "handle")
            .create(template)
    }
}
