package com.alarm.application.port.out

import com.alarm.domain.AlarmOutboxEvent

/**
 * [파일명] MessageQueuePort.kt
 * [수정일자] 2026-06-18
 * [기능] 비동기 메시지 큐 발행을 위한 아웃바운드 포트 정의
 * [관점] Outbound Port: Outbox 폴러(OutboxEventPublisher)가 Kafka 브로커로 이벤트를 발행할 때 사용하는 포트 계약입니다.
 *        AlarmService는 Outbox 테이블에만 저장하며 이 포트를 직접 호출하지 않습니다(이중 쓰기 방지).
 */
interface MessageQueuePort {
    fun publishAlarmJobFromOutbox(event: AlarmOutboxEvent)
}
