package com.alarm.adapter.out.messaging

import com.alarm.application.port.out.AlarmOutboxEventRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.MessageQueuePort
import com.alarm.domain.AlarmStatus
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate
import java.time.LocalDateTime
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll


/**
 * [파일명] OutboxEventPublisher.kt
 * [기능] Outbox 이벤트 폴링 후 Kafka 발행 처리 스케줄러 (Tx 분리형)
 * [관점] Outbound Adapter (Scheduler): 1초마다 RDB에서 PENDING 상태 이벤트를 조회하여
 *        Kafka 브로커에 발행합니다. 발행 시 DB 커넥션을 점유하지 않도록 트랜잭션을 분리했습니다.
 */
@Component
class OutboxEventPublisher(
    private val alarmOutboxEventRepositoryPort: AlarmOutboxEventRepositoryPort,
    private val alarmRepositoryPort: AlarmRepositoryPort,
    private val messageQueuePort: MessageQueuePort,
    private val transactionTemplate: TransactionTemplate,
    @Value("\${alarm.outbox.batch-size:100}") private val batchSize: Int
) {
    private val logger = LoggerFactory.getLogger(OutboxEventPublisher::class.java)

    @Scheduled(fixedDelay = 500)
    fun publishOutboxEvents() {
        val now = LocalDateTime.now()
        
        // 1. Tx1: 조회 및 PROCESSING 마킹 후 즉시 커밋 (DB 락 및 커넥션 해제)
        val pendingEvents = transactionTemplate.execute {
            val events = alarmOutboxEventRepositoryPort.findPendingEventsForUpdate(now, batchSize)
            events.forEach { 
                it.markProcessing()
                alarmOutboxEventRepositoryPort.save(it)
            }
            events
        } ?: emptyList()

        if (pendingEvents.isEmpty()) return

        logger.info("Outbox: {} PENDING 이벤트 발행 시작", pendingEvents.size)

        // 2. 비동기 카프카 발행: 루프를 돌며 send만 호출 (내부적으로 백그라운드 배칭 처리됨)
        val futures = pendingEvents.map { event ->
            event to messageQueuePort.publishAlarmJobFromOutbox(event)
        }

        val successEvents = mutableListOf<com.alarm.domain.AlarmOutboxEvent>()
        val failedEvents = mutableListOf<com.alarm.domain.AlarmOutboxEvent>()

        // 3. 동기 대기: 이미 비동기로 쏘아진 메시지들의 ACK를 기다림
        for ((event, future) in futures) {
            try {
                future.get(5, java.util.concurrent.TimeUnit.SECONDS)
                event.markPublished()
                successEvents.add(event)
                logger.debug("Outbox: alarmId={} Kafka 발행 완료", event.alarmId)
            } catch (e: Exception) {
                val isFinalFail = event.recordFailure() // 지수 백오프 알고리즘에 따라 실패 횟수 기록, 대기 시간(5초 -> 10초 -> 20초)
                failedEvents.add(event)
                if (isFinalFail) {
                    transactionTemplate.executeWithoutResult {
                        alarmRepositoryPort.findById(event.alarmId.toLong())?.let { a ->
                            a.fail("PUBLISHER")
                            alarmRepositoryPort.save(a)
                        }
                    }
                    logger.error("Outbox: alarmId={} Kafka 발행 최종 실패 → FAILED", event.alarmId, e)
                } else {
                    logger.warn("Outbox: alarmId={} Kafka 발행 일시 실패. 재시도: {}", event.alarmId, event.retryCount, e)
                }
            }
        }

        // 4. 단일 DB 트랜잭션: 성공 및 실패 건들을 한 방에 일괄 업데이트 (Batch Update)
        if (successEvents.isNotEmpty() || failedEvents.isNotEmpty()) {
            transactionTemplate.executeWithoutResult {
                alarmOutboxEventRepositoryPort.saveAll(successEvents + failedEvents)
            }
        }
    }
}
