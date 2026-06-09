package com.alarm.application.port.`in`

import com.alarm.domain.Alarm
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import java.util.UUID

interface GetAlarmQuery {
    fun getAlarm(id: UUID): Alarm
    fun listAlarms(pageable: Pageable): Page<Alarm>
}
