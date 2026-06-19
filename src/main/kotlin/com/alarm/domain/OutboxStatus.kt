package com.alarm.domain

/**
 * [파일명] OutboxStatus.kt
 * [수정일자] 2026-06-14
 * [기능] Outbox 이벤트 발행 상태 열거형 정의
 * [관점] Domain Value Object: Outbox 이벤트가 Kafka 발행 대기(PENDING), 발행 완료(PUBLISHED), 발행 실패(FAILED) 중 어느 상태인지를 나타내는 도메인 레이어의 불변 상태 값 집합입니다.
 */
enum class OutboxStatus {
    PENDING,        // RDB 저장 완료, Kafka 발행 대기 중
    PROCESSING,     // (스케줄러 락 대기 대신 메모리 상징적 상태, 또는 DB 업데이트)
    PUBLISHED,      // Kafka 브로커 발행 확인 완료
    PUBLISH_FAILED  // 재시도 횟수 초과 등 최종 발행 실패
}
