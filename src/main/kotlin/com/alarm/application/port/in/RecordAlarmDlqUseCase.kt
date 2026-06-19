package com.alarm.application.port.`in`

/**
 * [파일명] RecordAlarmDlqUseCase.kt
 * [기능] 실패한 알람 메시지를 DLQ에 기록하는 유즈케이스 포트
 */
interface RecordAlarmDlqUseCase {
    fun recordDlq(alarmId: Long, payload: String?, exceptionMessage: String?, stackTrace: String?)
}
