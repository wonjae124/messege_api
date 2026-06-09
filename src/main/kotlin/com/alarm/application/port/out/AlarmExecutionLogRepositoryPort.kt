package com.alarm.application.port.out

import com.alarm.domain.AlarmExecutionLog
import java.util.UUID

interface AlarmExecutionLogRepositoryPort {
    fun save(log: AlarmExecutionLog): AlarmExecutionLog
    fun findByAlarmId(alarmId: UUID): List<AlarmExecutionLog>
}
