package com.alarm.application.port.`in`

import java.util.UUID

interface TriggerAlarmUseCase {
    fun triggerAlarm(id: UUID)
}
