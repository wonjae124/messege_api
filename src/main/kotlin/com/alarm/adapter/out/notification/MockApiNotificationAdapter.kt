package com.alarm.adapter.out.notification

import com.alarm.application.port.out.AlarmBatchItem
import com.alarm.application.port.out.NotificationPort
import com.alarm.exception.CircuitBreakerOpenException
import com.alarm.exception.ExternalApiException
import com.alarm.exception.ReadTimeoutExternalException
import io.netty.handler.timeout.ReadTimeoutException
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import java.time.Duration
import java.util.concurrent.TimeoutException

/**
 * [파일명] MockApiNotificationAdapter.kt
 * [수정일자] 2026-06-23
 * [기능] 외부 알림 발송 API 서버 연동 및 재시도, 서킷 브레이커 제어
 * [관점] Outbound Adapter: NotificationPort 구체 클래스로서, WebClient를 사용하여 외부 API를 호출하며, 429 및 5xx 에러에 대한 지수 백오프 재시도 규칙 및 Resilience4j 서킷 브레이커 차단을 통제합니다.
 */
@Component
class MockApiNotificationAdapter(
    @Value("\${mock.api.url:http://localhost:8081}") private val mockApiUrl: String,
    @Value("\${mock.api.timeout.connect-millis:5000}") private val connectTimeoutMillis: Int,
    @Value("\${mock.api.timeout.response-seconds:5}") private val responseTimeoutSeconds: Long,
    @Value("\${mock.api.timeout.read-seconds:5}") private val readTimeoutSeconds: Int,
    @Value("\${mock.api.timeout.write-seconds:5}") private val writeTimeoutSeconds: Int,
    @Value("\${mock.api.http-pool.max-connections:100}") private val maxConnections: Int,
    @Value("\${mock.api.http-pool.pending-acquire-timeout-seconds:5}") private val pendingAcquireTimeoutSeconds: Long,
    @Value("\${mock.api.http-pool.max-idle-time-seconds:30}") private val maxIdleTimeSeconds: Long,
    @Value("\${mock.api.http-pool.max-life-time-seconds:300}") private val maxLifeTimeSeconds: Long,
    @Value("\${mock.api.http-pool.evict-interval-seconds:60}") private val evictIntervalSeconds: Long,
    @Value("\${mock.api.http-pool.metrics-enabled:true}") private val metricsEnabled: Boolean,
    private val externalApiRetry: io.github.resilience4j.retry.Retry
) : NotificationPort {

    private val logger = LoggerFactory.getLogger(MockApiNotificationAdapter::class.java)

    /**
     * 커넥션 풀 설정 (ConnectionProvider)
     * - maxConnections:          동일 목적지(host:port)에 동시에 열 수 있는 최대 커넥션 수 (기본값: CPU×2, 최소 16)
     * - pendingAcquireTimeout:   풀에서 커넥션을 빌리기까지 최대 대기 시간 (기본값: 45초 → 풀 고갈 시 앱 hang 방지를 위해 5초로 단축)
     * - maxIdleTime:             유휴 커넥션이 풀에서 제거되기까지 대기 시간 (기본값: 무제한 → stale connection 방지를 위해 30초)
     * - maxLifeTime:             커넥션 최대 수명 (기본값: 무제한 → DNS 변경 반영 및 오래된 커넥션 교체를 위해 5분)
     * - evictInBackground:       백그라운드에서 주기적으로 유휴/만료 커넥션을 정리 (60초 주기)
     * - metrics:                 Micrometer를 통해 커넥션 풀 상태를 Prometheus/Grafana에 노출
     */
    private val connectionProvider = ConnectionProvider.builder("mock-api")
        .maxConnections(maxConnections)
        .pendingAcquireTimeout(Duration.ofSeconds(pendingAcquireTimeoutSeconds))
        .maxIdleTime(Duration.ofSeconds(maxIdleTimeSeconds))
        .maxLifeTime(Duration.ofSeconds(maxLifeTimeSeconds))
        .evictInBackground(Duration.ofSeconds(evictIntervalSeconds))
        .metrics(metricsEnabled) // 커넥션 풀 메트릭 (active, pending 등)
        .build()

    private val httpClient = HttpClient.create(connectionProvider)
        .metrics(true, { uri -> uri })
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
        .responseTimeout(Duration.ofSeconds(responseTimeoutSeconds))
        .doOnConnected { conn ->
            conn.addHandlerLast(ReadTimeoutHandler(readTimeoutSeconds))
                .addHandlerLast(WriteTimeoutHandler(writeTimeoutSeconds))
        }

    private val webClient = WebClient.builder()
        .baseUrl(mockApiUrl)
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .build()

    data class SendMockRequest(
        val requestId: String,
        val channelType: String,
        val receiver: String,
        val message: String
    )

    @CircuitBreaker(name = "externalApi", fallbackMethod = "sendNotificationFallback")
    override fun sendNotification(item: AlarmBatchItem): Mono<Void> {
        val request = SendMockRequest(
            requestId = item.uuidClean,
            channelType = item.channelType,
            receiver = item.receiverId,
            message = item.payload ?: ""
        )
        
        logger.info("MockApi 요청 시작 - requestId: {}, receiver: {}, channel: {}", request.requestId, request.receiver, request.channelType)
        
        return webClient.post()
            .uri("/mock/send")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .retrieve()
            .bodyToMono(Void::class.java)
            .timeout(Duration.ofSeconds(responseTimeoutSeconds))
            .doOnSuccess {
                logger.info("MockApi 요청 성공 - requestId: {}", request.requestId)
            }
            .transformDeferred(io.github.resilience4j.reactor.retry.RetryOperator.of(externalApiRetry))
            .onErrorMap { throwable ->
                when (throwable) {
                    is ReadTimeoutException -> ReadTimeoutExternalException(
                        "Read timeout after ${readTimeoutSeconds}s"
                    )
                    is WebClientResponseException -> ExternalApiException(
                        throwable.statusCode.value(),
                        throwable.message ?: "External API failure"
                    )
                    else -> throwable
                }
            }
            .doOnError { throwable ->
                when (throwable) {
                    is ReadTimeoutException -> {
                        logger.error(
                            "READ_TIMEOUT | requestId: {} | uri: {} | cause: {}",
                            request.requestId, "/mock/send", throwable.message, throwable
                        )
                    }
                    else -> logger.error(
                        "MockApi 최종 실패 - requestId: {}, 원인: {}",
                        request.requestId, throwable.message, throwable
                    )
                }
            }
    }

    /**
     * 서킷 브레이커가 OPEN 상태일 때 호출 제한(CallNotPermittedException)을 가로채서 커스텀 예외로 변환하는 Fallback 메서드
     */
    fun sendNotificationFallback(item: AlarmBatchItem, ex: CallNotPermittedException): Mono<Void> {
        logger.info("Circuit breaker is OPEN. Blocking external API call for item {}.", item.uuidClean)
        return Mono.error(CircuitBreakerOpenException("Circuit breaker is OPEN. Calls to external notification API are blocked."))
    }
}
