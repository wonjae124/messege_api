package com.alarm.adapter.out.persistence.entity

import com.alarm.domain.Alarm
import com.alarm.domain.AlarmStatus
import jakarta.persistence.*
import java.time.LocalDateTime
import java.util.UUID

@Entity
@Table(name = "alarms")
class AlarmEntity(
    @Id
    val id: UUID,

    @Column(nullable = false)
    val name: String,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    val status: AlarmStatus,

    @Column(columnDefinition = "TEXT")
    val payload: String?,

    @Column(name = "created_at", nullable = false, updatable = false)
    val createdAt: LocalDateTime,

    @Column(name = "updated_at", nullable = false)
    val updatedAt: LocalDateTime
) {
    companion object {
        fun fromDomain(alarm: Alarm): AlarmEntity {
            return AlarmEntity(
                id = alarm.id,
                name = alarm.name,
                status = alarm.status,
                payload = alarm.payload,
                createdAt = alarm.createdAt,
                updatedAt = alarm.updatedAt
            )
        }
    }

    fun toDomain(): Alarm {
        return Alarm.reconstruct(
            id = id,
            name = name,
            status = status,
            payload = payload,
            createdAt = createdAt,
            updatedAt = updatedAt
        )
    }
}
