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

    @Scheduled(fixedDelay = 1000)
    fun publishPendingEvents() {
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

        // 2. Tx 외부: 루프 돌며 Kafka 네트워크 I/O 수행 (DB 커넥션 없음)
        for (event in pendingEvents) {
            try {
                // 브로커 동기 대기가 포함된 발행 메서드 호출
                messageQueuePort.publishAlarmJobFromOutbox(event)
                
                // 3-A. Tx2 (성공): 개별 트랜잭션으로 상태 업데이트
                transactionTemplate.executeWithoutResult {
                    event.markPublished()
                    alarmOutboxEventRepositoryPort.save(event)
                }
                logger.debug("Outbox: alarmId={} Kafka 발행 완료", event.alarmId)
            } catch (e: Exception) {
                // 3-B. Tx2 (실패): 개별 트랜잭션으로 상태 업데이트
                transactionTemplate.executeWithoutResult {
                    val isFinalFail = event.recordFailure()
                    alarmOutboxEventRepositoryPort.save(event)
                    if (isFinalFail) {
                        alarmRepositoryPort.updateStatusesInBatch(listOf(event.alarmId.toLong()), AlarmStatus.FAILED)
                        logger.error("Outbox: alarmId={} Kafka 발행 최종 실패 → FAILED", event.alarmId, e)
                    } else {
                        logger.warn("Outbox: alarmId={} Kafka 발행 일시 실패. 재시도 횟수: {}, 다음 재시도: {}", event.alarmId, event.retryCount, event.nextRetryAt, e)
                    }
                }
            }
        }
    }
}
