package com.alarm.application.port.`in`

import com.alarm.domain.Alarm
import com.alarm.domain.AlarmStatus
import java.time.LocalDateTime
import java.util.UUID

interface UpdateAlarmUseCase {
    fun updateAlarm(command: UpdateAlarmCommand): Alarm

    data class UpdateAlarmCommand(
        val id: UUID,
        val name: String?,
        val status: AlarmStatus?,
        val payload: String?
    )
}
