package com.alarm.domain

import com.github.f4b6a3.uuid.UuidCreator
import java.time.LocalDateTime
import java.util.UUID

/**
 * [파일명] AlarmExecutionLog.kt
 * [기능] 알람 전송 실행 이력 로그 도메인 엔티티 정의
 * [관점] Domain Entity: 알람이 실제로 발송되었거나 실패한 이력 로그를 비즈니스 규칙에 맞게 팩토리 메서드(record, reconstruct)로 캡슐화한 불변 성격의 도메인 엔티티입니다.
 */
class AlarmExecutionLog private constructor(
    val id: UUID,
    val alarmId: String,
    val triggeredAt: LocalDateTime,
    val status: String, // SUCCESS, FAILED
    val errorMessage: String?,
    val requestId: String,
    val traceId: String?,
    val channel: String?,
    val receiver: String?,
    val retryCount: Int,
    val externalApiStatusCode: Int?,
    val elapsedTimeMs: Long?,
    val resultStatus: String,
    val lastRetryAt: LocalDateTime?,
    val maxRetryExceeded: Boolean
) {
    companion object {
        @JvmOverloads
        fun record(
            alarmId: String,
            status: String,
            errorMessage: String? = null,
            requestId: String = org.slf4j.MDC.get("requestId") ?: "unknown",
            traceId: String? = io.opentelemetry.api.trace.Span.current().spanContext.let { if (it.isValid) it.traceId else null },
            channel: String? = null,
            receiver: String? = null,
            retryCount: Int = 0,
            externalApiStatusCode: Int? = null,
            elapsedTimeMs: Long? = null,
            resultStatus: String = status,
            lastRetryAt: LocalDateTime? = null,
            maxRetryExceeded: Boolean = false
        ): AlarmExecutionLog {
            return AlarmExecutionLog(
                id = UuidCreator.getTimeOrderedEpoch(),
                alarmId = alarmId,
                triggeredAt = LocalDateTime.now(),
                status = status,
                errorMessage = errorMessage,
                requestId = requestId,
                traceId = traceId,
                channel = channel,
                receiver = receiver,
                retryCount = retryCount,
                externalApiStatusCode = externalApiStatusCode,
                elapsedTimeMs = elapsedTimeMs,
                resultStatus = resultStatus,
                lastRetryAt = lastRetryAt,
                maxRetryExceeded = maxRetryExceeded
            )
        }

        @JvmOverloads
        fun reconstruct(
            id: UUID,
            alarmId: String,
            triggeredAt: LocalDateTime,
            status: String,
            errorMessage: String?,
            requestId: String = "unknown",
            traceId: String? = null,
            channel: String? = null,
            receiver: String? = null,
            retryCount: Int = 0,
            externalApiStatusCode: Int? = null,
            elapsedTimeMs: Long? = null,
            resultStatus: String = status,
            lastRetryAt: LocalDateTime? = null,
            maxRetryExceeded: Boolean = false
        ): AlarmExecutionLog {
            return AlarmExecutionLog(
                id = id,
                alarmId = alarmId,
                triggeredAt = triggeredAt,
                status = status,
                errorMessage = errorMessage,
                requestId = requestId,
                traceId = traceId,
                channel = channel,
                receiver = receiver,
                retryCount = retryCount,
                externalApiStatusCode = externalApiStatusCode,
                elapsedTimeMs = elapsedTimeMs,
                resultStatus = resultStatus,
                lastRetryAt = lastRetryAt,
                maxRetryExceeded = maxRetryExceeded
            )
        }
    }
}
