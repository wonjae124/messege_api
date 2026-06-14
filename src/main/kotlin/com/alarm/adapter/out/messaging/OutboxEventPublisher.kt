package com.alarm.adapter.out.messaging

import com.alarm.application.port.out.AlarmOutboxEventRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.MessageQueuePort
import com.alarm.domain.AlarmStatus
import com.alarm.domain.OutboxStatus
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

/**
 * [파일명] OutboxEventPublisher.kt
 * [수정일자] 2026-06-14
 * [기능] Outbox 이벤트 폴링 후 Kafka 발행 처리 스케줄러
 * [관점] Outbound Adapter (Scheduler): 1초마다 RDB에서 PENDING 상태 Outbox 이벤트를 조회하여
 *        Kafka 브로커에 발행하고, 성공 시 PUBLISHED, 실패 시 FAILED로 상태를 갱신합니다.
 *        알람 저장(트랜잭션)과 Kafka 발행을 분리하여 메시지 유실 없는 At-least-once 발행을 보장합니다.
 */
@Component
class OutboxEventPublisher(
    private val alarmOutboxEventRepositoryPort: AlarmOutboxEventRepositoryPort,
    private val alarmRepositoryPort: AlarmRepositoryPort,
    private val messageQueuePort: MessageQueuePort
) {
    private val logger = LoggerFactory.getLogger(OutboxEventPublisher::class.java)

    @Transactional
    @Scheduled(fixedDelay = 1000) // 1초마다 PENDING 이벤트 폴링
    fun publishPendingEvents() {
        val now = LocalDateTime.now()
        // FOR UPDATE SKIP LOCKED 쿼리로 이미 다른 인스턴스가 락을 잡은 행은 스킵하고 가져옴 (지수 백오프 시간 도래 건만)
        val pendingEvents = alarmOutboxEventRepositoryPort.findPendingEventsForUpdate(now)
        if (pendingEvents.isEmpty()) return

        logger.info("Outbox: {} PENDING 이벤트 발행 시작", pendingEvents.size)

        for (event in pendingEvents) {
            // 메모리 상에서 상태 변경 (트랜잭션 커밋 시 반영됨)
            event.markProcessing()

            try {
                messageQueuePort.publishAlarmJobFromOutbox(event)
                event.markPublished()
                // 원본 알람 테이블에도 상태 동기화 (발행 성공)
                alarmRepositoryPort.updateStatusesInBatch(listOf(event.alarmId), AlarmStatus.PUBLISHED)
                alarmOutboxEventRepositoryPort.save(event)
                logger.info("Outbox: alarmId={} Kafka 발행 완료 → PUBLISHED", event.alarmId)
            } catch (e: Exception) {
                val isFinalFail = event.recordFailure()
                alarmOutboxEventRepositoryPort.save(event)
                if (isFinalFail) {
                    // 최대 재시도 초과 시 원본 알람 테이블에도 최종 실패 마킹
                    alarmRepositoryPort.updateStatusesInBatch(listOf(event.alarmId), AlarmStatus.PUBLISH_FAILED)
                    logger.error("Outbox: alarmId={} Kafka 발행 최종 실패 → PUBLISH_FAILED", event.alarmId, e)
                } else {
                    logger.warn("Outbox: alarmId={} Kafka 발행 일시 실패. 재시도 횟수: {}, 다음 재시도: {}", event.alarmId, event.retryCount, event.nextRetryAt, e)
                }
            }
        }
    }
}
