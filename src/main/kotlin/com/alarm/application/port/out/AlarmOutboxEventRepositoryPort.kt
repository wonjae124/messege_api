package com.alarm.application.port.out

import com.alarm.domain.AlarmOutboxEvent
import com.alarm.domain.OutboxStatus

/**
 * [파일명] AlarmOutboxEventRepositoryPort.kt
 * [수정일자] 2026-06-14
 * [기능] Outbox 이벤트 저장/조회 아웃바운드 포트 정의
 * [관점] Outbound Port: 도메인/애플리케이션 레이어가 Outbox 이벤트 영속성에 의존하기 위한 추상 인터페이스입니다.
 */
interface AlarmOutboxEventRepositoryPort {
    fun save(event: AlarmOutboxEvent): AlarmOutboxEvent
    fun findPendingEventsForUpdate(now: java.time.LocalDateTime, limit: Int): List<AlarmOutboxEvent>
}
