package com.alarm.config

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

/**
 * [파일명] Resilience4jConfig.kt
 * [기능] Resilience4j 서킷 브레이커 설정 및 빈 등록
 */
@Configuration
class Resilience4jConfig(
    @Value("\${alarm.circuit-breaker.failure-rate-threshold:50}") private val failureRateThreshold: Float,
    @Value("\${alarm.circuit-breaker.sliding-window-size:10}") private val slidingWindowSize: Int,
    @Value("\${alarm.circuit-breaker.wait-duration-in-open-state:30s}") private val waitDuration: String,
    @Value("\${alarm.circuit-breaker.slow-call-duration-threshold:3s}") private val slowCallDuration: String,
    @Value("\${alarm.circuit-breaker.slow-call-rate-threshold:80}") private val slowCallRate: Float
) {

    @Bean
    fun externalApiCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker {
        val parsedWaitDuration = parseDuration(waitDuration)
        val parsedSlowCallDuration = parseDuration(slowCallDuration)

        val config = CircuitBreakerConfig.custom()
            .failureRateThreshold(failureRateThreshold)
            .slidingWindowSize(slidingWindowSize)
            .waitDurationInOpenState(parsedWaitDuration)
            .slowCallDurationThreshold(parsedSlowCallDuration)
            .slowCallRateThreshold(slowCallRate)
            .permittedNumberOfCallsInHalfOpenState(3)
            .build()

        return registry.circuitBreaker("externalApi", config)
    }

    private fun parseDuration(value: String): Duration {
        val cleaned = value.trim().lowercase()
        return when {
            cleaned.endsWith("ms") -> Duration.ofMillis(cleaned.removeSuffix("ms").toLong())
            cleaned.endsWith("s") -> Duration.ofSeconds(cleaned.removeSuffix("s").toLong())
            cleaned.endsWith("m") -> Duration.ofMinutes(cleaned.removeSuffix("m").toLong())
            else -> Duration.ofSeconds(cleaned.toLong())
        }
    }
}
