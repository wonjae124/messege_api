package com.alarm.adapter.out.persistence.repository

import com.alarm.adapter.out.persistence.entity.AlarmOutboxEventEntity
import com.alarm.domain.OutboxStatus
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param

/**
 * [파일명] SpringDataAlarmOutboxEventRepository.kt
 * [수정일자] 2026-06-14
 * [기능] Spring Data JPA 기반 Outbox 이벤트 테이블 데이터 액세스 인터페이스
 * [관점] Outbound Infrastructure: PENDING 상태 이벤트 목록 조회 등 Outbox 폴러가 필요로 하는 쿼리를 제공합니다.
 */
interface SpringDataAlarmOutboxEventRepository : JpaRepository<AlarmOutboxEventEntity, Long> {
    fun findById(id: String): AlarmOutboxEventEntity?
    fun findAllByStatus(status: OutboxStatus): List<AlarmOutboxEventEntity>

    @Query(
        value = "SELECT * FROM alarm_outbox_events WHERE status = 'PENDING' AND next_retry_at <= :now FOR UPDATE SKIP LOCKED",
        nativeQuery = true
    )
    fun findPendingEventsForUpdate(@Param("now") now: java.time.LocalDateTime): List<AlarmOutboxEventEntity>
}
