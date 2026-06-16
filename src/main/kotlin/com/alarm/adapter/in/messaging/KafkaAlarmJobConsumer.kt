package com.alarm.adapter.`in`.messaging

import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.NotificationPort
import com.alarm.application.port.out.AlarmBatchItem
import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.domain.AlarmExecutionLog
import com.alarm.domain.AlarmStatus
import com.alarm.exception.ExternalApiException
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.retrytopic.RetryTopicHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.CompletableFuture
import reactor.core.publisher.Mono

/**
 * [파일명] KafkaAlarmJobConsumer.kt
 * [기능] Kafka 비동기 알람 메시지 배치 소비 및 전송 처리, 실제 비즈니스 로직 수행함.
 * [관점] Inbound Adapter: 카프카 큐 이벤트 스트림(Inbound Event)을 구독하여 외부 API 일괄 호출 및 DB 벌크/배치 업데이트(JdbcTemplate)를 연동하고 멱등 처리를 보장합니다.
 */
@Component
class KafkaAlarmJobConsumer(
    private val alarmRepositoryPort: AlarmRepositoryPort,
    private val notificationPort: NotificationPort,
    private val alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort,
    private val redisTemplate: StringRedisTemplate,
    private val redisRateLimiter: RedisRateLimiter,
    private val objectMapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(KafkaAlarmJobConsumer::class.java)

    data class AlarmJobPayload(
        val alarmId: String,
        val senderId: String,
        val receiverId: String,
        val uuid: String,
        val requestId: String? = null,
        val traceId: String? = null
    )

    @KafkaListener(
        topics = ["alarm-jobs"],
        groupId = "\${spring.kafka.consumer.group-id:alarm_api_consumer_group}",
        concurrency = "3"
    )
    fun consumeAlarmJob(
        payloadStr: String,
        @Header(name = RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, required = false) attempts: Int?
    ): CompletableFuture<Void?> {
        val attempt = attempts ?: 1
        logger.info("Processing alarm job event: attempt={}", attempt)

        val parsedPayload: AlarmJobPayload
        try {
            parsedPayload = objectMapper.readValue(payloadStr, AlarmJobPayload::class.java)
        } catch (e: Exception) {
            logger.error("Unexpected error parsing payload", e)
            val future = CompletableFuture<Void?>()
            future.completeExceptionally(e)
            return future
        }

        MDC.put("requestId", parsedPayload.requestId ?: "unknown")
        val procKey = "proc:${parsedPayload.senderId}${parsedPayload.uuid}"

        // 1. 소비 단계 멱등성 체크 (Redis proc:)
        val isFirstProcessing = redisTemplate.opsForValue().setIfAbsent(procKey, "PROCESSING", Duration.ofMinutes(3))
        if (isFirstProcessing != true) {
            val status = redisTemplate.opsForValue().get(procKey)
            if (status == "DONE" || status == "PROCESSING") {
                logger.info("Duplicate Kafka job detected and ignored: alarmId = {}", parsedPayload.alarmId)
                return CompletableFuture.completedFuture(null)
            }
            redisTemplate.opsForValue().set(procKey, "PROCESSING", Duration.ofMinutes(3))
        }

        // 2. 외부 알림 발송 전 레이트 리밋 제어
        while (!redisRateLimiter.tryAcquire(1)) {
            try {
                Thread.sleep(100)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                val future = CompletableFuture<Void?>()
                future.completeExceptionally(e)
                return future
            }
        }

        val alarmIdLong = parsedPayload.alarmId.toLong()
        val alarm = alarmRepositoryPort.findById(alarmIdLong)
        if (alarm == null) {
            redisTemplate.delete(procKey)
            val future = CompletableFuture<Void?>()
            future.completeExceptionally(com.alarm.exception.AlarmNotFoundException("알람을 찾을 수 없습니다: $alarmIdLong"))
            return future
        }

        alarm.processing()
        alarmRepositoryPort.save(alarm)

        val batchItem = AlarmBatchItem(
            alarmId = alarm.id,
            senderId = alarm.senderId,
            receiverId = alarm.receiverId,
            channelType = alarm.channelType,
            uuidClean = parsedPayload.uuid,
            payload = alarm.payload
        )

        val startTime = System.currentTimeMillis()

        // 3. 비동기 리액티브 파이프라인 연동 (Non-blocking)
        val future = notificationPort.sendNotification(listOf(batchItem))
            .doOnSuccess {
                val elapsedTime = System.currentTimeMillis() - startTime
                // 성공 시 Redis 및 DB 상태 갱신
                redisTemplate.opsForValue().set(procKey, "DONE", Duration.ofMinutes(3))
                alarmRepositoryPort.updateStatusesInBatch(listOf(alarmIdLong), AlarmStatus.SENT)

                alarmExecutionLogRepositoryPort.save(AlarmExecutionLog.record(
                    alarmId = parsedPayload.alarmId,
                    status = "SUCCESS",
                    requestId = parsedPayload.requestId ?: "unknown",
                    traceId = parsedPayload.traceId,
                    channel = "KAFKA",
                    receiver = parsedPayload.receiverId,
                    retryCount = attempt - 1,
                    externalApiStatusCode = 200,
                    elapsedTimeMs = elapsedTime,
                    resultStatus = "SUCCESS",
                    lastRetryAt = if (attempt > 1) LocalDateTime.now() else null,
                    maxRetryExceeded = false
                ))
            }
            .onErrorResume { e ->
                val elapsedTime = System.currentTimeMillis() - startTime
                logger.error("Failed to execute API call for alarm", e)

                // 실패 시 procKey 삭제하여 다음 재시도 가능하게 함
                redisTemplate.delete(procKey)

                val statusCode = if (e is ExternalApiException) e.statusCode else 500
                val is4xxClientError = e is ExternalApiException && e.statusCode in 400..499
                val isMaxExceeded = attempt > 3

                val (targetStatus, logStatus, logResultStatus) = if (is4xxClientError) {
                    Triple(AlarmStatus.FAILED, "FAILED", "FAILED")
                } else if (isMaxExceeded) {
                    Triple(AlarmStatus.DEAD, "DEAD", "DEAD")
                } else {
                    Triple(AlarmStatus.RETRY_WAIT, "RETRY_WAIT", "RETRY_WAIT")
                }

                alarmRepositoryPort.updateStatusesInBatch(listOf(alarmIdLong), targetStatus)

                alarmExecutionLogRepositoryPort.save(AlarmExecutionLog.record(
                    alarmId = parsedPayload.alarmId,
                    status = logStatus,
                    errorMessage = e.message,
                    requestId = parsedPayload.requestId ?: "unknown",
                    traceId = parsedPayload.traceId,
                    channel = "KAFKA",
                    receiver = parsedPayload.receiverId,
                    retryCount = attempt - 1,
                    externalApiStatusCode = statusCode,
                    elapsedTimeMs = elapsedTime,
                    resultStatus = logResultStatus,
                    lastRetryAt = LocalDateTime.now(),
                    maxRetryExceeded = isMaxExceeded || is4xxClientError
                ))

                if (is4xxClientError) {
                    // 4xx client 에러인 경우 재시도하지 않도록 Mono.empty() 반환 (성공 처리로 흐름 종결)
                    Mono.empty()
                } else {
                    Mono.error(e)
                }
            }
            .toFuture()

        // 4. 비동기 리턴 및 MDC 정리
        return future.handle { _, throwable ->
            MDC.clear()
            if (throwable != null) {
                throw throwable
            }
            null
        }
    }
}
