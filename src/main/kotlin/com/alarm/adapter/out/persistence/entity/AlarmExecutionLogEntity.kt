package com.alarm.adapter.out.persistence.entity

import com.alarm.domain.AlarmExecutionLog
import jakarta.persistence.*
import java.time.LocalDateTime
import java.util.UUID

@Entity
@Table(name = "alarm_execution_logs")
class AlarmExecutionLogEntity(
    @Id
    val id: UUID,

    @Column(name = "alarm_id", nullable = false)
    val alarmId: UUID,

    @Column(name = "triggered_at", nullable = false)
    val triggeredAt: LocalDateTime,

    @Column(nullable = false)
    val status: String, // SUCCESS, FAILED

    @Column(name = "error_message", columnDefinition = "TEXT")
    val errorMessage: String?
) {
    companion object {
        fun fromDomain(log: AlarmExecutionLog): AlarmExecutionLogEntity {
            return AlarmExecutionLogEntity(
                id = log.id,
                alarmId = log.alarmId,
                triggeredAt = log.triggeredAt,
                status = log.status,
                errorMessage = log.errorMessage
            )
        }
    }

    fun toDomain(): AlarmExecutionLog {
        return AlarmExecutionLog.reconstruct(
            id = id,
            alarmId = alarmId,
            triggeredAt = triggeredAt,
            status = status,
            errorMessage = errorMessage
        )
    }
}
