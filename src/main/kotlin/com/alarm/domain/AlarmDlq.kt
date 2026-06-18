package com.alarm.domain

import java.time.LocalDateTime

/**
 * [파일명] AlarmDlq.kt
 * [기능] 데드레터 큐(DLQ)에 저장될 실패 내역 도메인 모델
 * [관점] Domain Entity: 시스템 오류로 인해 최종적으로 전송에 실패한 메시지의 상세 정보(예외, 페이로드)를 보관하여 추후 재처리가 가능하도록 돕습니다.
 */
class AlarmDlq private constructor(
    val id: Long? = null, // DB 자동 생성 식별자
    val alarmId: Long,
    val originalPayload: String?,
    val exceptionMessage: String?,
    val exceptionStackTrace: String?,
    var resolved: Boolean,
    val createdAt: LocalDateTime,
    var resolvedAt: LocalDateTime? = null
) {
    companion object {
        fun create(
            alarmId: Long,
            originalPayload: String?,
            exceptionMessage: String?,
            exceptionStackTrace: String?
        ): AlarmDlq {
            return AlarmDlq(
                alarmId = alarmId,
                originalPayload = originalPayload,
                exceptionMessage = exceptionMessage,
                exceptionStackTrace = exceptionStackTrace,
                resolved = false,
                createdAt = LocalDateTime.now()
            )
        }

        fun reconstruct(
            id: Long,
            alarmId: Long,
            originalPayload: String?,
            exceptionMessage: String?,
            exceptionStackTrace: String?,
            resolved: Boolean,
            createdAt: LocalDateTime,
            resolvedAt: LocalDateTime?
        ): AlarmDlq {
            return AlarmDlq(
                id = id,
                alarmId = alarmId,
                originalPayload = originalPayload,
                exceptionMessage = exceptionMessage,
                exceptionStackTrace = exceptionStackTrace,
                resolved = resolved,
                createdAt = createdAt,
                resolvedAt = resolvedAt
            )
        }
    }

    fun markResolved() {
        this.resolved = true
        this.resolvedAt = LocalDateTime.now()
    }
}
