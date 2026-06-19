package com.alarm.adapter.out.persistence.entity

import com.alarm.domain.AlarmDlq
import jakarta.persistence.*
import java.time.LocalDateTime

/**
 * [파일명] AlarmDlqEntity.kt
 * [기능] 데드레터 큐(DLQ) 테이블 구조와 JPA 매핑
 */
@Entity
@Table(name = "alarm_dlq")
class AlarmDlqEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(name = "alarm_id", nullable = false)
    val alarmId: Long,

    @Column(name = "original_payload", columnDefinition = "TEXT")
    val originalPayload: String?,

    @Column(name = "exception_message", columnDefinition = "TEXT")
    val exceptionMessage: String?,

    @Column(name = "exception_stack_trace", columnDefinition = "TEXT")
    val exceptionStackTrace: String?,

    @Column(nullable = false)
    var resolved: Boolean = false,

    @Column(name = "created_at", nullable = false, updatable = false)
    val createdAt: LocalDateTime,

    @Column(name = "resolved_at")
    var resolvedAt: LocalDateTime? = null
) {
    companion object {
        fun fromDomain(domain: AlarmDlq): AlarmDlqEntity {
            return AlarmDlqEntity(
                id = domain.id,
                alarmId = domain.alarmId,
                originalPayload = domain.originalPayload,
                exceptionMessage = domain.exceptionMessage,
                exceptionStackTrace = domain.exceptionStackTrace,
                resolved = domain.resolved,
                createdAt = domain.createdAt,
                resolvedAt = domain.resolvedAt
            )
        }
    }

    fun toDomain(): AlarmDlq {
        return AlarmDlq.reconstruct(
            id = id ?: 0L,
            alarmId = alarmId,
            originalPayload = originalPayload,
            exceptionMessage = exceptionMessage,
            exceptionStackTrace = exceptionStackTrace,
            resolved = resolved,
            createdAt = createdAt,
            resolvedAt = resolvedAt
        )
    }
}
