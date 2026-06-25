package com.alarm.adapter.out.persistence.entity

import com.alarm.domain.Alarm
import com.alarm.domain.AlarmStatus
import jakarta.persistence.*
import java.time.LocalDateTime

/**
 * [파일명] AlarmEntity.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 도메인 엔티티와 매핑되는 JPA RDB 테이블 엔티티 정의
 * [관점] Outbound persistence Entity: RDB 스키마 구조와 대응하며 도메인과 DB 간의 상호 매핑(reconstruct/fromDomain) 변환 로직을 가집니다.
 */
@Entity
/**
 * [Index 설계 전략 (Read vs Write Trade-off)]
 * 알림 서비스(alarms)는 초당 수천 건의 INSERT(쓰기)가 발생하는 극단적인 쓰기 위주의 시스템입니다.
 * 인덱스가 추가될 때마다 쓰기 성능이 기하급수적으로 하락하므로, 최소한의 필수 인덱스만 유지해야 합니다.
 * 
 * 1. idx_alarm_history: 발송자 기준의 커서 페이징(빠른 조회)을 위한 복합 인덱스
 * 2. idx_alarm_receiver_history: 수신자 기준의 커서 페이징을 위한 복합 인덱스
 * 
 * *주의*: status, channel_type 등의 단순 검색을 위한 인덱스 추가는 INSERT 성능을 위해 의도적으로 배제했습니다.
 */
@Table(
    name = "alarms",
    indexes = [
        Index(name = "idx_alarm_history", columnList = "sender_id, created_at DESC"),
        Index(name = "idx_alarm_receiver_history", columnList = "receiver_id, created_at DESC")
    ]
)
class AlarmEntity(
    @Id
    @Column(name = "id", nullable = false)
    val id: Long,

    @Column(name = "sender_id", nullable = false)
    val senderId: String,

    @Column(name = "receiver_id", nullable = false)
    val receiverId: String,

    @Column(name = "channel_type", nullable = false)
    val channelType: String,

    @Column(nullable = false)
    val name: String,

    @Convert(converter = AlarmStatusConverter::class)
    @Column(nullable = false, columnDefinition = "SMALLINT")
    val status: AlarmStatus,

    @org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    val payload: String?,

    @Column
    val location: String?,

    @Column(name = "created_at", nullable = false, updatable = false)
    val createdAt: LocalDateTime,

    @Column(name = "updated_at", nullable = false)
    val updatedAt: LocalDateTime
) {
    companion object {
        fun fromDomain(alarm: Alarm): AlarmEntity {
            return AlarmEntity(
                id = alarm.id,
                senderId = alarm.senderId,
                receiverId = alarm.receiverId,
                channelType = alarm.channelType,
                name = alarm.name,
                status = alarm.status,
                payload = alarm.payload,
                location = alarm.location,
                createdAt = alarm.createdAt,
                updatedAt = alarm.updatedAt
            )
        }
    }

    fun toDomain(): Alarm {
        return Alarm.reconstruct(
            id = id,
            senderId = senderId,
            receiverId = receiverId,
            channelType = channelType,
            name = name,
            status = status,
            payload = payload,
            location = location,
            createdAt = createdAt,
            updatedAt = updatedAt
        )
    }
}
