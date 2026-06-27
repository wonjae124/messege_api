package com.alarm.config

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import io.github.resilience4j.retry.RetryRegistry
import io.netty.handler.timeout.ReadTimeoutException
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.client.WebClientResponseException
import java.time.Duration
import java.util.concurrent.TimeoutException
import org.slf4j.LoggerFactory

/**
 * [파일명] Resilience4jConfig.kt
 * [기능] Resilience4j 서킷 브레이커 및 재시도 설정 빈 등록
 */
@Configuration
class Resilience4jConfig(
    @Value("\${alarm.circuit-breaker.failure-rate-threshold:50}") private val failureRateThreshold: Float,
    @Value("\${alarm.circuit-breaker.sliding-window-size:10}") private val slidingWindowSize: Int,
    @Value("\${alarm.circuit-breaker.sliding-window-type:COUNT_BASED}") private val slidingWindowType: String,
    @Value("\${alarm.circuit-breaker.minimum-number-of-calls:10}") private val minimumNumberOfCalls: Int,
    @Value("\${alarm.circuit-breaker.permitted-number-of-calls-in-half-open-state:10}") private val permittedNumberOfCallsInHalfOpenState: Int,
    @Value("\${alarm.circuit-breaker.wait-duration-in-open-state:30s}") private val waitDuration: String,
    @Value("\${alarm.circuit-breaker.automatic-transition-from-open-to-half-open-enabled:true}") private val automaticTransitionEnabled: Boolean,
    @Value("\${alarm.circuit-breaker.slow-call-duration-threshold:3s}") private val slowCallDuration: String,
    @Value("\${alarm.circuit-breaker.slow-call-rate-threshold:80}") private val slowCallRate: Float,
    @Value("\${mock.api.retry.max-attempts:3}") private val retryMaxAttempts: Int,
    @Value("\${mock.api.retry.min-backoff-seconds:1}") private val retryMinBackoffSeconds: Long,
    @Value("\${mock.api.retry.max-backoff-seconds:10}") private val retryMaxBackoffSeconds: Long,
    @Value("\${mock.api.retry.jitter:0.3}") private val retryJitter: Double
) {

    @Bean
    fun externalApiCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker {
        val parsedWaitDuration = parseDuration(waitDuration)
        val parsedSlowCallDuration = parseDuration(slowCallDuration)

        val config = CircuitBreakerConfig.custom()
            .failureRateThreshold(failureRateThreshold)
            .slidingWindowType(io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType.valueOf(slidingWindowType))
            .slidingWindowSize(slidingWindowSize)
            .minimumNumberOfCalls(minimumNumberOfCalls)
            .waitDurationInOpenState(parsedWaitDuration)
            .automaticTransitionFromOpenToHalfOpenEnabled(automaticTransitionEnabled)
            .slowCallDurationThreshold(parsedSlowCallDuration)
            .slowCallRateThreshold(slowCallRate)
            .permittedNumberOfCallsInHalfOpenState(permittedNumberOfCallsInHalfOpenState)
            .build()

        val circuitBreaker = registry.circuitBreaker("externalApi", config)

        circuitBreaker.eventPublisher.onStateTransition { event ->
            LoggerFactory.getLogger(Resilience4jConfig::class.java)
                .warn("서킷 브레이커 상태 변경 [{}]: {} -> {}", event.circuitBreakerName, event.stateTransition.fromState, event.stateTransition.toState)
        }.onError { event ->
            LoggerFactory.getLogger(Resilience4jConfig::class.java)
                .error("서킷 브레이커 에러 발생 [{}]: {}", event.circuitBreakerName, event.throwable.message)
        }

        return circuitBreaker
    }

    @Bean
    fun externalApiRetry(registry: RetryRegistry): Retry {
        val config = RetryConfig.custom<Any>()
            .maxAttempts(retryMaxAttempts)
            .intervalFunction(
                IntervalFunction.ofExponentialRandomBackoff(
                    Duration.ofSeconds(retryMinBackoffSeconds), // 최초 대기 시간
                    2.0,
                    retryJitter, // 주문 버튼을 누른 수만 명의 스마트폰(대다수의 클라이언트) 화면에 일제히 "주문 실패" 에러 발생한 상황(외부 API 불안정). 모두 같은 시간 동안 대기 한 후 재요청 하면 동시에 요청을 받은 서버는 터지기 마련임. 따라서 랜덤한 시간으로 요청을 보내게 만듦.
                    Duration.ofSeconds(retryMaxBackoffSeconds)  // 최대 대기 시간 상한 적용
                )
            )
            .retryOnException { throwable ->
                (throwable is WebClientResponseException && (throwable.statusCode.value() == 429 || throwable.statusCode.is5xxServerError)) ||
                (throwable is TimeoutException || throwable is ReadTimeoutException)
            }
            .build()

        val retry = registry.retry("externalApi", config)
        retry.eventPublisher.onRetry { event ->
            LoggerFactory.getLogger(Resilience4jConfig::class.java)
                .info("MockApi 재시도 발생 - 시도 횟수: {}, 원인: {}", event.numberOfRetryAttempts, event.lastThrowable?.message)
        }

        return retry
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
