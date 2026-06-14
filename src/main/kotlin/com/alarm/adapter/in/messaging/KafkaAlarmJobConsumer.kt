package com.alarm.adapter.`in`.messaging

import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.NotificationPort
import com.alarm.application.port.out.AlarmBatchItem
import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.domain.AlarmExecutionLog
import com.alarm.domain.AlarmStatus
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import java.time.Duration

/**
 * [파일명] KafkaAlarmJobConsumer.kt
 * [수정일자] 2026-06-11
 * [기능] Kafka 비동기 알람 메시지 배치 소비 및 전송 처리
 * [관점] Inbound Adapter: 카프카 큐 이벤트 스트림(Inbound Event)을 구독하여 외부 API 일괄 호출 및 DB 벌크/배치 업데이트(JdbcTemplate)를 연동하고 멱등 처리를 보장합니다.
 */
@Component
class KafkaAlarmJobConsumer(
    private val alarmRepositoryPort: AlarmRepositoryPort,
    private val notificationPort: NotificationPort,
    private val alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort,
    private val redisTemplate: StringRedisTemplate,
    private val redisRateLimiter: RedisRateLimiter,
    private val objectMapper: ObjectMapper,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    private val logger = LoggerFactory.getLogger(KafkaAlarmJobConsumer::class.java)

    data class AlarmJobPayload(
        val alarmId: String,
        val senderId: String,
        val receiverId: String,
        val uuid: String,
        val retryCount: Int = 0
    )

    @KafkaListener(
        topics = ["alarm-jobs"],
        groupId = "alarm-group",
        concurrency = "3",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun consumeAlarmJobs(payloads: List<String>, ack: Acknowledgment) {
        logger.info("Received batch of alarm job events from Kafka: size = {}", payloads.size)
        
        val parsedPayloads = payloads.mapNotNull { 
            try { objectMapper.readValue(it, AlarmJobPayload::class.java) } 
            catch (e: Exception) { logger.error("Parse error", e); null } 
        }
        if (parsedPayloads.isEmpty()) {
            ack.acknowledge()
            return
        }

        // 1. 1차 DB 상태 조회 (Source of Truth)
        val alarmIds = parsedPayloads.map { it.alarmId }.distinct()
        val initialAlarms = alarmRepositoryPort.findAllByIds(alarmIds).associateBy { it.id }

        // 이미 종결된 상태(SENT, FAILED, DEAD) 필터링
        val validPayloads = parsedPayloads.filter { payload ->
            val status = initialAlarms[payload.alarmId]?.status
            status != AlarmStatus.SENT && status != AlarmStatus.FAILED && status != AlarmStatus.DEAD
        }

        if (validPayloads.isEmpty()) {
            ack.acknowledge()
            return
        }

        // 2. Redis Lock 획득 시도 (단기 중복/동시성 방어)
        val lockedPayloads = mutableListOf<AlarmJobPayload>()
        val lockKeys = mutableListOf<String>()

        for (payload in validPayloads) {
            val procKey = "proc:${payload.senderId}${payload.uuid}"
            val acquired = redisTemplate.opsForValue().setIfAbsent(procKey, "PROCESSING", Duration.ofMinutes(3))
            if (acquired == true) {
                lockedPayloads.add(payload)
                lockKeys.add(procKey)
            } else {
                logger.info("Lock acquisition failed (already processing): alarmId = {}", payload.alarmId)
            }
        }

        if (lockedPayloads.isEmpty()) {
            ack.acknowledge()
            return
        }

        try {
            // 3. 2차 DB 상태 조회 (Double-Checked Locking)
            val lockedAlarmIds = lockedPayloads.map { it.alarmId }.distinct()
            val finalAlarms = alarmRepositoryPort.findAllByIds(lockedAlarmIds).associateBy { it.id }

            val finalPayloadsToProcess = lockedPayloads.filter { payload ->
                val status = finalAlarms[payload.alarmId]?.status
                status != AlarmStatus.SENT && status != AlarmStatus.FAILED && status != AlarmStatus.DEAD
            }

            if (finalPayloadsToProcess.isEmpty()) return

            // 4. 상태 전이 (SENDING)
            val processingIds = finalPayloadsToProcess.map { it.alarmId }
            alarmRepositoryPort.updateStatusesInBatch(processingIds, AlarmStatus.SENDING)

            // 5. 배치 단위 API 호출 (Rate Limit 적용)
            val chunks = finalPayloadsToProcess.chunked(200)
            val successfulIds = mutableListOf<String>()
            val failedIds = mutableListOf<String>()
            val retryPayloads = mutableListOf<AlarmJobPayload>()
            val deadIds = mutableListOf<String>()
            val logs = mutableListOf<AlarmExecutionLog>()

            for (chunk in chunks) {
                while (!redisRateLimiter.tryAcquire(1)) { Thread.sleep(100) }

                val batchItems = chunk.map {
                    AlarmBatchItem(alarmId = it.alarmId, senderId = it.senderId, uuidClean = it.uuid, payload = it.alarmId)
                }

                try {
                    notificationPort.sendNotification(batchItems).block()
                    chunk.forEach { 
                        successfulIds.add(it.alarmId)
                        logs.add(AlarmExecutionLog.record(it.alarmId, "SUCCESS"))
                    }
                } catch (e: Exception) {
                    logger.error("API call failed", e)
                    // 에러 응답 분류 로직 (400 계열 vs 500/Timeout)
                    val isClientError = e is WebClientResponseException && e.statusCode.is4xxClientError
                    
                    for (item in chunk) {
                        if (isClientError) {
                            // 400 계열: 복구 불가능 에러 -> FAILED
                            failedIds.add(item.alarmId)
                            logs.add(AlarmExecutionLog.record(item.alarmId, "FAILED", e.message))
                        } else {
                            // 500 계열 / Timeout / 429: 재시도 대상
                            val nextRetry = item.retryCount + 1
                            if (nextRetry > 3) {
                                deadIds.add(item.alarmId)
                                logs.add(AlarmExecutionLog.record(item.alarmId, "DEAD", "Max retries exceeded: ${e.message}"))
                            } else {
                                retryPayloads.add(item.copy(retryCount = nextRetry))
                                logs.add(AlarmExecutionLog.record(item.alarmId, "RETRY", "Scheduled for retry ${nextRetry}/3"))
                            }
                        }
                    }
                }
            }

            // 6. DB 결과 저장 및 재시도 큐 발행
            if (successfulIds.isNotEmpty()) alarmRepositoryPort.updateStatusesInBatch(successfulIds, AlarmStatus.SENT)
            if (failedIds.isNotEmpty()) alarmRepositoryPort.updateStatusesInBatch(failedIds, AlarmStatus.FAILED)
            if (deadIds.isNotEmpty()) alarmRepositoryPort.updateStatusesInBatch(deadIds, AlarmStatus.DEAD)
            
            // 재시도 대상은 상태를 다시 PUBLISHED로 돌려놓고(또는 그대로 두되) Retry 토픽으로 발행
            if (retryPayloads.isNotEmpty()) {
                alarmRepositoryPort.updateStatusesInBatch(retryPayloads.map { it.alarmId }, AlarmStatus.PUBLISHED)
                for (p in retryPayloads) {
                    val payloadStr = objectMapper.writeValueAsString(p)
                    kafkaTemplate.send("alarm-jobs-retry", payloadStr)
                }
            }

            if (logs.isNotEmpty()) alarmExecutionLogRepositoryPort.saveAll(logs)

        } finally {
            // 7. 완료 후 무조건 Redis Lock 해제 (다른 인스턴스 재시도 가능하도록)
            if (lockKeys.isNotEmpty()) {
                redisTemplate.delete(lockKeys)
            }
            // 8. Offset Commit
            ack.acknowledge()
            logger.info("Successfully acknowledged Kafka batch of size = {}", payloads.size)
        }
    }

    @KafkaListener(
        topics = ["alarm-jobs-retry"],
        groupId = "alarm-retry-group"
    )
    fun consumeRetry(record: String) {
        logger.info("Received failed alarm event to retry after 1 minute delay: {}", record)
        Thread.sleep(60000) // 1분 대기
        kafkaTemplate.send("alarm-jobs", record)
    }
}
