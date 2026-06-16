package com.alarm.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.retrytopic.RetryTopicConfiguration
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder

@Configuration
class KafkaRetryConfig {

    @Bean
    fun retryTopicConfig(template: KafkaTemplate<String, String>): RetryTopicConfiguration {
        return RetryTopicConfigurationBuilder.newInstance()
            // 지수 백오프: 3s -> 6s -> 12s -> 24s (최대 30초)
            .exponentialBackoff(3000, 2.0, 30000)
            .maxAttempts(4) // 최초 1회 + 재시도 3회
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
