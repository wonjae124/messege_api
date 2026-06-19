package com.alarm.adapter.out.persistence

import com.alarm.application.port.out.AlarmDlqRepositoryPort
import com.alarm.domain.AlarmDlq
import com.alarm.adapter.out.persistence.entity.AlarmDlqEntity
import org.springframework.stereotype.Repository

/**
 * [파일명] JpaAlarmDlqRepositoryAdapter.kt
 * [기능] 포트를 구현하여 실제 DB에 DLQ 데이터를 저장
 */
@Repository
class JpaAlarmDlqRepositoryAdapter(
    private val jpaAlarmDlqRepository: JpaAlarmDlqRepository
) : AlarmDlqRepositoryPort {

    override fun save(alarmDlq: AlarmDlq): AlarmDlq {
        val entity = AlarmDlqEntity.fromDomain(alarmDlq)
        val savedEntity = jpaAlarmDlqRepository.save(entity)
        return savedEntity.toDomain()
    }
}
