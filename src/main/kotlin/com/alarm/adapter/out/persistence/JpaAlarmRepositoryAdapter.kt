package com.alarm.adapter.out.persistence

import com.alarm.adapter.out.persistence.entity.AlarmEntity
import com.alarm.adapter.out.persistence.repository.SpringDataAlarmRepository
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.domain.Alarm
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.stereotype.Component
import java.util.UUID

@Component
class JpaAlarmRepositoryAdapter(
    private val springDataAlarmRepository: SpringDataAlarmRepository
) : AlarmRepositoryPort {

    override fun save(alarm: Alarm): Alarm {
        val entity = AlarmEntity.fromDomain(alarm)
        val savedEntity = springDataAlarmRepository.save(entity)
        return savedEntity.toDomain()
    }

    override fun findById(id: UUID): Alarm? {
        return springDataAlarmRepository.findById(id)
            .map { it.toDomain() }
            .orElse(null)
    }

    override fun findAll(pageable: Pageable): Page<Alarm> {
        return springDataAlarmRepository.findAll(pageable)
            .map { it.toDomain() }
    }

    override fun delete(alarm: Alarm) {
        val entity = AlarmEntity.fromDomain(alarm)
        springDataAlarmRepository.delete(entity)
    }
}
