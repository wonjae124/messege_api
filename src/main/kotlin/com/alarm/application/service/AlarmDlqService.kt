package com.alarm.application.service

import com.alarm.application.port.`in`.RecordAlarmDlqUseCase
import com.alarm.application.port.out.AlarmDlqRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.domain.AlarmDlq
import com.alarm.exception.AlarmNotFoundException
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

/**
 * [파일명] AlarmDlqService.kt
 * [기능] DLQ 메시지를 데이터베이스에 저장하고, 원본 알람의 상태를 DEAD로 업데이트하는 애플리케이션 서비스
 */
@Service
class AlarmDlqService(
    private val alarmDlqRepositoryPort: AlarmDlqRepositoryPort,
    private val alarmRepositoryPort: AlarmRepositoryPort
) : RecordAlarmDlqUseCase {

    private val logger = LoggerFactory.getLogger(AlarmDlqService::class.java)

    @Transactional
    override fun recordDlq(alarmId: Long, payload: String?, exceptionMessage: String?, stackTrace: String?) {
        logger.info("Recording DLQ for alarmId: {}", alarmId)
        
        // 1. 원본 알람 조회 및 DEAD 상태로 업데이트
        val alarm = alarmRepositoryPort.findById(alarmId)
        if (alarm != null) {
            alarm.dead(location = "CONSUMER_DLQ")
            alarmRepositoryPort.save(alarm)
        } else {
            logger.warn("Original alarm not found for DLQ recording: {}", alarmId)
        }

        // 2. DLQ 내역 저장
        val alarmDlq = AlarmDlq.create(
            alarmId = alarmId,
            originalPayload = payload,
            exceptionMessage = exceptionMessage,
            exceptionStackTrace = stackTrace
        )
        alarmDlqRepositoryPort.save(alarmDlq)
    }
}
