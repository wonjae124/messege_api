package com.alarm.adapter.`in`.messaging

import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.NotificationPort
import com.alarm.application.port.out.AlarmBatchItem
import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.domain.AlarmExecutionLog
import com.alarm.domain.AlarmStatus
import com.alarm.exception.ExternalApiException
import com.alarm.exception.AlarmNotFoundException
import com.alarm.exception.ConcurrentProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.retrytopic.RetryTopicHeaders
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.CompletableFuture
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

/**
 * [파일명] KafkaAlarmJobConsumer.kt
 * [기능] Kafka 비동기 알람 메시지 소비 및 전송 처리, 실제 비즈니스 로직 수행함.
 * [관점] Inbound Adapter: 카프카 큐 이벤트를 구독하여 외부 API 호출 및 DB 업데이트를 연동하고 멱등 처리를 보장합니다.
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

    // 리액티브 논블로킹 속도 제한 획득 헬퍼 함수
    private fun acquireTokenReactive(retries: Int = 50): Mono<Void> {
        return Mono.fromCallable {
            redisRateLimiter.tryAcquire(1)
        }
        .subscribeOn(Schedulers.boundedElastic())
        .flatMap { acquired ->
            if (acquired) {
                Mono.empty<Void>()
            } else {
                if (retries <= 0) {
                    Mono.error(RuntimeException("Rate limit token acquisition timeout"))
                } else {
                    Mono.delay(Duration.ofMillis(100))
                        .then(Mono.defer { acquireTokenReactive(retries - 1) })
                }
            }
        }
    }

    @KafkaListener(
        topics = ["alarm-jobs"],
        groupId = "\${spring.kafka.consumer.group-id:alarm_api_consumer_group}",
        concurrency = "\${spring.kafka.consumer.concurrency:3}"
    )
    fun consumeAlarmJob(
        payloadStr: String,
        @Header(name = RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, required = false) attempts: Int?,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int = 0,
        @Header(KafkaHeaders.OFFSET) offset: Long = 0,
        @Header(KafkaHeaders.RECEIVED_KEY) key: String? = null
    ): CompletableFuture<Void?> {
        val attempt = attempts ?: 1
        logger.info("Kafka message received - key={}, partition={}, offset={}, attempt={}", key, partition, offset, attempt)
        logger.info("Processing alarm job event: attempt={}", attempt)

        val parsedPayload: AlarmJobPayload
        val alarmIdLong: Long
        try {
            parsedPayload = objectMapper.readValue(payloadStr, AlarmJobPayload::class.java)
            alarmIdLong = parsedPayload.alarmId.toLong()
        } catch (e: Exception) {
            logger.error("Unexpected error parsing payload or invalid alarmId: {}", payloadStr, e)
            val future = CompletableFuture<Void?>()
            future.completeExceptionally(e)
            return future
        }

        val requestId = parsedPayload.requestId ?: "unknown"
        MDC.put("requestId", requestId)
        val procKey = "proc:${parsedPayload.senderId}:${parsedPayload.uuid}"

        // 1. 소비 단계 1차 RDB 멱등성 검증 (Double-Checked Locking) - Redis 락 경합 최소화
        val alarmInit = alarmRepositoryPort.findById(alarmIdLong)
        if (alarmInit != null && alarmInit.isFinalStatus()) {
            logger.info("DCL (RDB) 1st duplicate check triggered: alarm {} is already in final status {}. Skipping.", alarmIdLong, alarmInit.status)
            redisTemplate.opsForValue().set(procKey, "DONE", Duration.ofMinutes(3))
            MDC.clear()
            return CompletableFuture.completedFuture(null)
        }

        // 2. 소비 단계 분산 락 선점 (Redis proc:)
        val isFirstProcessing = redisTemplate.opsForValue().setIfAbsent(procKey, "PROCESSING", Duration.ofMinutes(3))
        if (isFirstProcessing != true) {
            val status = redisTemplate.opsForValue().get(procKey)
            if (status == "DONE") {
                logger.info("Duplicate Kafka job detected and ignored: alarmId = {}", alarmIdLong)
                MDC.clear()
                return CompletableFuture.completedFuture(null)
            }
            // PROCESSING 이거나 락이 점유된 경우
            logger.info("Job currently processing or lock held: alarmId = {}", alarmIdLong)
            MDC.clear()
            val future = CompletableFuture<Void?>()
            future.completeExceptionally(ConcurrentProcessingException("Alarm job is already processing or lock is held: $alarmIdLong"))
            return future
        }

        // 락 선점 이후, 동기 구간을 try-catch로 감싸 비동기 체인 위임 전 예외 시 락 해제 및 MDC 클리어 보장
        val batchItem: AlarmBatchItem
        try {
            // 3. 소비 단계 2차 RDB 멱등성 검증 (Double-Checked Locking)
            val alarm = alarmRepositoryPort.findById(alarmIdLong)
            if (alarm == null) {
                throw AlarmNotFoundException("알람을 찾을 수 없습니다: $alarmIdLong")
            }
            if (alarm.isFinalStatus()) {
                logger.info("DCL (RDB) 2nd duplicate check triggered: alarm {} is already in final status {}. Skipping.", alarmIdLong, alarm.status)
                redisTemplate.opsForValue().set(procKey, "DONE", Duration.ofMinutes(3))
                MDC.clear()
                return CompletableFuture.completedFuture(null)
            }

            alarm.processing()
            alarmRepositoryPort.save(alarm)

            batchItem = AlarmBatchItem(
                alarmId = alarm.id,
                senderId = alarm.senderId,
                receiverId = alarm.receiverId,
                channelType = alarm.channelType,
                uuidClean = parsedPayload.uuid,
                payload = alarm.payload
            )
        } catch (e: Exception) {
            redisTemplate.delete(procKey)
            MDC.clear()
            val future = CompletableFuture<Void?>()
            future.completeExceptionally(e)
            return future
        }

        val startTime = System.currentTimeMillis()

        // 4. 비동기 리액티브 파이프라인 연동 (Non-blocking 토큰 획득 -> 발송 -> boundedElastic 스케줄러 전환 -> 결과 저장)
        val future = acquireTokenReactive()
            .then(Mono.defer {
                notificationPort.sendNotification(batchItem)
            })
            .publishOn(Schedulers.boundedElastic()) // 블로킹 RDB I/O 격리
            .then(Mono.defer {
                val elapsedTime = System.currentTimeMillis() - startTime
                redisTemplate.opsForValue().set(procKey, "DONE", Duration.ofMinutes(3))
                
                alarmRepositoryPort.findById(alarmIdLong)?.let { a ->
                    a.sent()
                    alarmRepositoryPort.save(a)
                }

                alarmExecutionLogRepositoryPort.save(AlarmExecutionLog.record(
                    alarmId = alarmIdLong.toString(),
                    status = "SUCCESS",
                    requestId = requestId,
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
                Mono.empty<Void>()
            })
            .onErrorResume { e ->
                val elapsedTime = System.currentTimeMillis() - startTime
                // 리액터 스레드 임시 MDC 로깅 설정
                MDC.put("requestId", requestId)
                logger.error("Failed to execute API call for alarm: alarmId = {}", alarmIdLong, e)
                MDC.clear()

                // 실패 시 락을 해제하여 다음 재시도를 가능하게 함
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

                val failureLocation = if (targetStatus == AlarmStatus.FAILED || targetStatus == AlarmStatus.DEAD) "CONSUMER" else null

                // boundedElastic에서 DB 작업 수행
                alarmRepositoryPort.findById(alarmIdLong)?.let { a ->
                    a.status = targetStatus
                    a.location = failureLocation
                    a.updatedAt = LocalDateTime.now()
                    alarmRepositoryPort.save(a)
                }
                
                alarmExecutionLogRepositoryPort.save(AlarmExecutionLog.record(
                    alarmId = alarmIdLong.toString(),
                    status = logStatus,
                    errorMessage = e.message,
                    requestId = requestId,
                    traceId = parsedPayload.traceId,
                    channel = "KAFKA",
                    receiver = parsedPayload.receiverId,
                    retryCount = attempt - 1,
                    externalApiStatusCode = statusCode,
                    elapsedTimeMs = elapsedTime,
                    resultStatus = logResultStatus,
                    lastRetryAt = LocalDateTime.now(),
                    maxRetryExceeded = isMaxExceeded
                ))

                if (is4xxClientError || isMaxExceeded) {
                    // 재시도가 불가능한 최종 상태인 경우, 예외를 던지지 않고 정상 완료하여 Kafka 추가 재시도 방지
                    Mono.empty()
                } else {
                    Mono.error(e)
                }
            }
            .toFuture()

        // 5. 비동기 리턴 및 카프카 스레드 MDC 정리
        MDC.clear()
        return future.handle { _, throwable ->
            if (throwable != null) {
                throw throwable
            }
            null
        }
    }
}
