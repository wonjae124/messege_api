package com.alarm.adapter.out.notification

import com.alarm.application.port.out.AlarmBatchItem
import com.alarm.application.port.out.NotificationPort
import com.alarm.exception.CircuitBreakerOpenException
import com.alarm.exception.ExternalApiException
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
import reactor.util.retry.Retry
import java.time.Duration

/**
 * [파일명] MockApiNotificationAdapter.kt
 * [수정일자] 2026-06-15
 * [기능] 외부 알림 발송 API 서버 연동 및 재시도, 서킷 브레이커 제어
 * [관점] Outbound Adapter: NotificationPort 구체 클래스로서, WebClient를 사용하여 외부 API를 호출하며, 429 및 5xx 에러에 대한 지수 백오프 재시도 규칙 및 Resilience4j 서킷 브레이커 차단을 통제합니다.
 */
@Component
class MockApiNotificationAdapter(
    @Value("\${mock.api.url:http://localhost:8081}") private val mockApiUrl: String,
    @Value("\${mock.api.timeout.connect-millis:5000}") private val connectTimeoutMillis: Int,
    @Value("\${mock.api.timeout.response-seconds:5}") private val responseTimeoutSeconds: Long,
    @Value("\${mock.api.timeout.read-seconds:5}") private val readTimeoutSeconds: Int,
    @Value("\${mock.api.timeout.write-seconds:5}") private val writeTimeoutSeconds: Int
) : NotificationPort {

    private val logger = LoggerFactory.getLogger(MockApiNotificationAdapter::class.java)

    private val httpClient = HttpClient.create()
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
    override fun sendNotification(items: List<AlarmBatchItem>): Mono<Void> {
        if (items.isEmpty()) return Mono.empty()
        
        return Flux.fromIterable(items)
            .flatMap { item ->
                val request = SendMockRequest(
                    requestId = item.uuidClean,
                    channelType = item.channelType,
                    receiver = item.receiverId,
                    message = item.payload ?: ""
                )
                webClient.post()
                    .uri("/mock/send")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(Void::class.java)
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .filter { throwable ->
                            throwable is WebClientResponseException &&
                            (throwable.statusCode.value() == 429 || throwable.statusCode.is5xxServerError)
                        }
                    )
                    .onErrorMap { throwable ->
                        if (throwable is WebClientResponseException) {
                            ExternalApiException(throwable.statusCode.value(), throwable.message ?: "External API failure")
                        } else {
                            throwable
                        }
                    }
            }
            .then()
    }

    /**
     * 서킷 브레이커가 OPEN 상태일 때 호출 제한(CallNotPermittedException)을 가로채서 커스텀 예외로 변환하는 Fallback 메서드
     */
    fun sendNotificationFallback(items: List<AlarmBatchItem>, ex: CallNotPermittedException): Mono<Void> {
        logger.warn("Circuit breaker is OPEN. Blocking external API call for {} items.", items.size)
        return Mono.error(CircuitBreakerOpenException("Circuit breaker is OPEN. Calls to external notification API are blocked."))
    }
}
