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
    ) {
        val attempt = attempts ?: 1
        logger.info("Processing alarm job event: attempt={}", attempt)

        var parsedPayload: AlarmJobPayload? = null
        try {
            parsedPayload = objectMapper.readValue(payloadStr, AlarmJobPayload::class.java)
            MDC.put("requestId", parsedPayload.requestId ?: "unknown")

            val procKey = "proc:${parsedPayload.senderId}${parsedPayload.uuid}"

            // 1. 소비 단계 멱등성 체크 (Redis proc:)
            val isFirstProcessing = redisTemplate.opsForValue().setIfAbsent(procKey, "PROCESSING", Duration.ofMinutes(3))
            if (isFirstProcessing != true) {
                val status = redisTemplate.opsForValue().get(procKey)
                if (status == "DONE") {
                    logger.info("Duplicate Kafka job detected and ignored: alarmId = {}", parsedPayload.alarmId)
                    return // 이미 성공한 건은 무시
                }
                // PROCESSING 중이거나 다른 상태라면, 다시 처리 시도
                redisTemplate.opsForValue().set(procKey, "PROCESSING", Duration.ofMinutes(3))
            }

            // 2. 외부 알림 발송
            while (!redisRateLimiter.tryAcquire(1)) {
                Thread.sleep(100)
            }

            val batchItem = AlarmBatchItem(
                alarmId = parsedPayload.alarmId,
                senderId = parsedPayload.senderId,
                uuidClean = parsedPayload.uuid,
                payload = parsedPayload.alarmId
            )

            val startTime = System.currentTimeMillis()
            try {
                notificationPort.sendNotification(listOf(batchItem)).block()
                val elapsedTime = System.currentTimeMillis() - startTime

                // 성공 시
                redisTemplate.opsForValue().set(procKey, "DONE", Duration.ofMinutes(3))

                alarmRepositoryPort.updateStatusesInBatch(listOf(parsedPayload.alarmId), AlarmStatus.SENT)

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

            } catch (e: Exception) {
                val elapsedTime = System.currentTimeMillis() - startTime
                logger.error("Failed to execute API call for alarm", e)

                // 실패 시 procKey 삭제하여 다음 재시도 가능하게 함
                redisTemplate.delete(procKey)

                val statusCode = if (e is ExternalApiException) e.statusCode else 500

                val isMaxExceeded = attempt > 3 // 4번째 시도에서 실패 시 (maxAttempts = 4)
                if (isMaxExceeded) {
                    alarmRepositoryPort.updateStatusesInBatch(listOf(parsedPayload.alarmId), AlarmStatus.FAILED)
                }

                alarmExecutionLogRepositoryPort.save(AlarmExecutionLog.record(
                    alarmId = parsedPayload.alarmId,
                    status = "FAILED",
                    errorMessage = e.message,
                    requestId = parsedPayload.requestId ?: "unknown",
                    traceId = parsedPayload.traceId,
                    channel = "KAFKA",
                    receiver = parsedPayload.receiverId,
                    retryCount = attempt - 1,
                    externalApiStatusCode = statusCode,
                    elapsedTimeMs = elapsedTime,
                    resultStatus = "FAILED",
                    lastRetryAt = LocalDateTime.now(),
                    maxRetryExceeded = isMaxExceeded
                ))

                // 재시도를 위해 예외를 밖으로 던짐 (Spring Kafka Retry Topic이 처리)
                throw e
            }

        } catch (e: Exception) {
            logger.error("Unexpected error parsing or processing payload", e)
            throw e
        } finally {
            MDC.clear()
        }
    }
}
