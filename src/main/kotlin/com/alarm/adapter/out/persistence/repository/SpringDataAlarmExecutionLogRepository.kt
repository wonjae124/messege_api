package com.alarm.adapter.out.persistence.repository

import com.alarm.adapter.out.persistence.entity.AlarmExecutionLogEntity
import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

interface SpringDataAlarmExecutionLogRepository : JpaRepository<AlarmExecutionLogEntity, UUID> {
    fun findByAlarmId(alarmId: UUID): List<AlarmExecutionLogEntity>
}
