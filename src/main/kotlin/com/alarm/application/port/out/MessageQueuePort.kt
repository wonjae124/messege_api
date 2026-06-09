package com.alarm.application.port.out

import com.alarm.domain.Alarm

interface MessageQueuePort {
    fun publishAlarmJob(alarm: Alarm)
}
