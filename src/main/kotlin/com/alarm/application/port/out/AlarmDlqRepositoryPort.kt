package com.alarm.application.port.out

import com.alarm.domain.AlarmDlq

/**
 * [파일명] AlarmDlqRepositoryPort.kt
 * [기능] 알람 DLQ 엔티티의 저장 처리를 담당하는 아웃바운드 포트
 */
interface AlarmDlqRepositoryPort {
    fun save(alarmDlq: AlarmDlq): AlarmDlq
}
