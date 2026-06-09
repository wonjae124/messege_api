package com.alarm.domain

import java.time.LocalDateTime
import java.util.UUID

class AlarmExecutionLog private constructor(
    val id: UUID,
    val alarmId: UUID,
    val triggeredAt: LocalDateTime,
    val status: String, // SUCCESS, FAILED
    val errorMessage: String?
) {
    companion object {
        fun record(
            alarmId: UUID,
            status: String,
            errorMessage: String? = null
        ): AlarmExecutionLog {
            return AlarmExecutionLog(
                id = UUID.randomUUID(),
                alarmId = alarmId,
                triggeredAt = LocalDateTime.now(),
                status = status,
                errorMessage = errorMessage
            )
        }

        fun reconstruct(
            id: UUID,
            alarmId: UUID,
            triggeredAt: LocalDateTime,
            status: String,
            errorMessage: String?
        ): AlarmExecutionLog {
            return AlarmExecutionLog(id, alarmId, triggeredAt, status, errorMessage)
        }
    }
}
