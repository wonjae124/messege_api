package com.alarm.adapter.out.persistence

import com.alarm.adapter.out.persistence.entity.AlarmExecutionLogEntity
import com.alarm.adapter.out.persistence.repository.SpringDataAlarmExecutionLogRepository
import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.domain.AlarmExecutionLog
import org.springframework.stereotype.Component
import java.util.UUID

@Component
class JpaAlarmExecutionLogRepositoryAdapter(
    private val springDataAlarmExecutionLogRepository: SpringDataAlarmExecutionLogRepository
) : AlarmExecutionLogRepositoryPort {

    override fun save(log: AlarmExecutionLog): AlarmExecutionLog {
        val entity = AlarmExecutionLogEntity.fromDomain(log)
        val savedEntity = springDataAlarmExecutionLogRepository.save(entity)
        return savedEntity.toDomain()
    }

    override fun findByAlarmId(alarmId: UUID): List<AlarmExecutionLog> {
        return springDataAlarmExecutionLogRepository.findByAlarmId(alarmId)
            .map { it.toDomain() }
    }
}
