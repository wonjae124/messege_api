package com.alarm.domain

import java.time.LocalDateTime
import java.util.UUID

/**
 * [파일명] AlarmOutboxEvent.kt
 * [수정일자] 2026-06-14
 * [기능] Outbox Pattern 이벤트 도메인 엔티티 정의
 * [관점] Domain Entity: 알람 생성 시 Kafka 발행 전 RDB에 먼저 저장되는 Outbox 이벤트입니다.
 *        알람 저장과 동일 트랜잭션 내에서 저장되어, Kafka 발행 실패 시에도 이벤트 유실을 방지합니다.
 *        별도 스케줄러(OutboxEventPublisher)가 PENDING 상태 이벤트를 읽어 Kafka에 발행합니다.
 */
class AlarmOutboxEvent private constructor(
    val id: String,
    val alarmId: String,
    val senderId: String,
    val receiverId: String,
    val uuid: String,
    var status: OutboxStatus,
    val createdAt: LocalDateTime,
    var publishedAt: LocalDateTime?,
    var retryCount: Int = 0,
    var nextRetryAt: LocalDateTime
) {
    companion object {
        fun create(alarm: Alarm): AlarmOutboxEvent {
            val uuid = alarm.id.replace("-", "")
            return AlarmOutboxEvent(
                id = UUID.randomUUID().toString(),
                alarmId = alarm.id,
                senderId = alarm.senderId,
                receiverId = alarm.receiverId,
                uuid = uuid,
                status = OutboxStatus.PENDING,
                createdAt = LocalDateTime.now(),
                publishedAt = null,
                retryCount = 0,
                nextRetryAt = LocalDateTime.now()
            )
        }

        fun reconstruct(
            id: String,
            alarmId: String,
            senderId: String,
            receiverId: String,
            uuid: String,
            status: OutboxStatus,
            createdAt: LocalDateTime,
            publishedAt: LocalDateTime?,
            retryCount: Int,
            nextRetryAt: LocalDateTime
        ): AlarmOutboxEvent = AlarmOutboxEvent(id, alarmId, senderId, receiverId, uuid, status, createdAt, publishedAt, retryCount, nextRetryAt)
    }

    fun markProcessing() {
        this.status = OutboxStatus.PROCESSING
    }

    fun markPublished() {
        this.status = OutboxStatus.PUBLISHED
        this.publishedAt = LocalDateTime.now()
    }

    fun markPublishFailed() {
        this.status = OutboxStatus.PUBLISH_FAILED
    }

    /**
     * 카프카 발행 실패 시 재시도 횟수를 증가시키고, 다음 재시도 시간을 지수 백오프로 설정합니다.
     * @return 최대 재시도 횟수(3회)를 초과하여 완전히 실패했는지 여부
     */
    fun recordFailure(): Boolean {
        this.retryCount++
        if (this.retryCount > 3) {
            markPublishFailed()
            return true
        }

        // 지수 백오프: 1회 실패시 5초, 2회시 10초, 3회시 20초 대기
        val backoffSeconds = 5L * (1L shl (this.retryCount - 1)) // 5 * 2^(retryCount-1)
        this.nextRetryAt = LocalDateTime.now().plusSeconds(backoffSeconds)
        return false
    }
}
