package com.alarm.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.retrytopic.RetryTopicConfiguration
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder

@Configuration
class KafkaRetryConfig(
    // 재시도 초기 대기 시간(ms). 설정이 없으면 3000ms(3초) 사용
    @Value("\${spring.kafka.retry.initial-interval:3000}") private val initialInterval: Long,
    // 재시도 시 대기 시간을 증가시킬 배수. 기본값 2.0 (두 배씩 증가)
    @Value("\${spring.kafka.retry.multiplier:2.0}") private val multiplier: Double,
    // 대기 시간의 최대 상한(ms). 기본값 30000ms(30초)
    @Value("\${spring.kafka.retry.max-interval:30000}") private val maxInterval: Long,
    // 재시도 허용 최대 횟수. 기본값 4번(최초 시도 제외, 총 5번 시도)
    @Value("\${spring.kafka.retry.max-attempts:4}") private val maxAttempts: Int,
    @Value("\${spring.kafka.topic.partitions:3}") private val partitions: Int,
    @Value("\${spring.kafka.topic.replication-factor:3}") private val replicas: Short
) {

@Bean
    fun retryTopicConfig(template: KafkaTemplate<String, String>): RetryTopicConfiguration {
        // Builder를 사용해 재시도 토픽 구성을 정의합니다.
        return RetryTopicConfigurationBuilder.newInstance()
            .autoCreateTopicsWith(partitions, replicas)
            // 지수 백오프: initialInterval → * multiplier → maxInterval 까지 증가
            .exponentialBackoff(initialInterval, multiplier, maxInterval)
            // 최대 재시도 횟수 지정
            .maxAttempts(maxAttempts)
            // 특정 예외는 재시도하지 않고 즉시 Dead‑Letter‑Topic(DLT) 으로 보냅니다.
            .notRetryOn(
                // 예: 비즈니스 로직 오류
                listOf(
                    IllegalArgumentException::class.java,
                    IllegalStateException::class.java
                )
            )
            // DLT 로 전송될 때 호출될 메서드 지정 (bean 이름, 메서드명)
            .dltHandlerMethod("dltProcessor", "handle")
            // 완성된 구성을 주입된 KafkaTemplate 으로 실제 Bean 생성
            .create(template)
    }
}
