package com.alarm.adapter.out.persistence.entity

import com.alarm.domain.AlarmOutboxEvent
import com.alarm.domain.OutboxStatus
import jakarta.persistence.*
import java.time.LocalDateTime

/**
 * [파일명] AlarmOutboxEventEntity.kt
 * [수정일자] 2026-06-14
 * [기능] Outbox 이벤트 도메인과 매핑되는 JPA RDB 테이블 엔티티 정의
 * [관점] Outbound Persistence Entity: alarm_outbox_events 테이블 스키마와 대응하며
 *        도메인과 DB 간 상호 매핑(reconstruct/fromDomain) 변환 로직을 가집니다.
 */
@Entity
@Table(name = "alarm_outbox_events")
class AlarmOutboxEventEntity(
    @Id
    val id: Long,

    @Column(name = "alarm_id", nullable = false)
    val alarmId: String,

    @Column(name = "sender_id", nullable = false)
    val senderId: String,

    @Column(name = "receiver_id", nullable = false)
    val receiverId: String,

    @Column(nullable = false)
    val uuid: String,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    val status: OutboxStatus,

    @Column(name = "created_at", nullable = false, updatable = false)
    val createdAt: LocalDateTime,

    @Column(name = "published_at")
    val publishedAt: LocalDateTime?,

    @Column(name = "retry_count", nullable = false)
    val retryCount: Int,

    @Column(name = "next_retry_at", nullable = false)
    val nextRetryAt: LocalDateTime
) {
    companion object {
        fun fromDomain(event: AlarmOutboxEvent): AlarmOutboxEventEntity {
            return AlarmOutboxEventEntity(
                id = event.id,
                alarmId = event.alarmId,
                senderId = event.senderId,
                receiverId = event.receiverId,
                uuid = event.uuid,
                status = event.status,
                createdAt = event.createdAt,
                publishedAt = event.publishedAt,
                retryCount = event.retryCount,
                nextRetryAt = event.nextRetryAt
            )
        }
    }

    fun toDomain(): AlarmOutboxEvent {
        return AlarmOutboxEvent.reconstruct(
            id = id,
            alarmId = alarmId,
            senderId = senderId,
            receiverId = receiverId,
            uuid = uuid,
            status = status,
            createdAt = createdAt,
            publishedAt = publishedAt,
            retryCount = retryCount,
            nextRetryAt = nextRetryAt
        )
    }
}
