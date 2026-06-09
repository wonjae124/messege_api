package com.alarm.application.port.out

import java.util.UUID

interface NotificationPort {
    fun sendNotification(alarmId: UUID, payload: String?)
}
