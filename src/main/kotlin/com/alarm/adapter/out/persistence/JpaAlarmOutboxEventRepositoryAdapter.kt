package com.alarm.adapter.out.persistence

import com.alarm.adapter.out.persistence.entity.AlarmOutboxEventEntity
import com.alarm.adapter.out.persistence.repository.SpringDataAlarmOutboxEventRepository
import com.alarm.application.port.out.AlarmOutboxEventRepositoryPort
import com.alarm.domain.AlarmOutboxEvent
import org.springframework.stereotype.Component

/**
 * [파일명] JpaAlarmOutboxEventRepositoryAdapter.kt
 * [수정일자] 2026-06-14
 * [기능] Outbox 이벤트 데이터베이스 저장 및 조회
 * [관점] Outbound Adapter: AlarmOutboxEventRepositoryPort 구체 클래스로서, JPA를 통해 Outbox 이벤트를 영속화합니다.
 */
@Component
class JpaAlarmOutboxEventRepositoryAdapter(
    private val springDataAlarmOutboxEventRepository: SpringDataAlarmOutboxEventRepository
) : AlarmOutboxEventRepositoryPort {

    override fun save(event: AlarmOutboxEvent): AlarmOutboxEvent {
        val entity = AlarmOutboxEventEntity.fromDomain(event)
        return springDataAlarmOutboxEventRepository.save(entity).toDomain()
    }

    override fun findPendingEventsForUpdate(now: java.time.LocalDateTime, limit: Int): List<AlarmOutboxEvent> {
        return springDataAlarmOutboxEventRepository.findPendingEventsForUpdate(now, limit)
            .map { it.toDomain() }
    }
}
