package com.alarm.application.port.`in`

import com.alarm.domain.Alarm
import java.time.LocalDateTime

interface CreateAlarmUseCase {
    fun createAlarm(command: CreateAlarmCommand): Alarm

    data class CreateAlarmCommand(
        val name: String,
        val payload: String?
    )
}
