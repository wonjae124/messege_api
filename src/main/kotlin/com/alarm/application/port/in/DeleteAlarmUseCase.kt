package com.alarm.application.port.`in`

import java.util.UUID

interface DeleteAlarmUseCase {
    fun deleteAlarm(id: UUID)
}
