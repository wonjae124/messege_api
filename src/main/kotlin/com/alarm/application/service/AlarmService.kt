package com.alarm.application.service

import com.alarm.application.port.`in`.*
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.LockTemplate
import com.alarm.application.port.out.MessageQueuePort
import com.alarm.domain.Alarm
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

@Service
@Transactional(readOnly = true)
class AlarmService(
    private val alarmRepositoryPort: AlarmRepositoryPort,
    private val alarmExecutionLogRepositoryPort: com.alarm.application.port.out.AlarmExecutionLogRepositoryPort,
    private val notificationPort: com.alarm.application.port.out.NotificationPort,
    private val lockTemplate: LockTemplate,
    private val messageQueuePort: MessageQueuePort
) : CreateAlarmUseCase, GetAlarmQuery, UpdateAlarmUseCase, DeleteAlarmUseCase, TriggerAlarmUseCase {

    @Transactional
    override fun createAlarm(command: CreateAlarmUseCase.CreateAlarmCommand): Alarm {
        // 정적 팩토리 메서드를 사용하여 의도 명확화 및 검증 로직 강제화
        val alarm = Alarm.create(
            name = command.name,
            payload = command.payload
        )
        val savedAlarm = alarmRepositoryPort.save(alarm)
        messageQueuePort.publishAlarmJob(savedAlarm)
        return savedAlarm
    }

    override fun getAlarm(id: UUID): Alarm {
        return alarmRepositoryPort.findById(id)
            ?: throw NoSuchElementException("알람을 찾을 수 없습니다: $id")
    }

    override fun listAlarms(pageable: Pageable): Page<Alarm> {
        return alarmRepositoryPort.findAll(pageable)
    }

    @Transactional
    override fun updateAlarm(command: UpdateAlarmUseCase.UpdateAlarmCommand): Alarm {
        // 템플릿/콜백 패턴 기반의 동시성 자원 제어
        val lockKey = "alarm-lock-${command.id}"
        return lockTemplate.executeWithLock(lockKey) {
            val alarm = alarmRepositoryPort.findById(command.id)
                ?: throw NoSuchElementException("알람을 찾을 수 없습니다: ${command.id}")

            // 도메인 내부 메서드에 행위와 상태 수정 위임
            alarm.update(
                name = command.name,
                payload = command.payload
            )
            command.status?.let {
                when (it) {
                    com.alarm.domain.AlarmStatus.ACTIVE -> alarm.activate()
                    com.alarm.domain.AlarmStatus.INACTIVE -> alarm.deactivate()
                    com.alarm.domain.AlarmStatus.TRIGGERED -> alarm.trigger()
                    com.alarm.domain.AlarmStatus.FAILED -> alarm.fail()
                }
            }
            alarmRepositoryPort.save(alarm)
        }
    }

    @Transactional
    override fun deleteAlarm(id: UUID) {
        val lockKey = "alarm-lock-$id"
        lockTemplate.executeWithLock(lockKey) {
            val alarm = alarmRepositoryPort.findById(id)
                ?: throw NoSuchElementException("알람을 찾을 수 없습니다: $id")
            alarmRepositoryPort.delete(alarm)
        }
    }

    @Transactional
    override fun triggerAlarm(id: UUID) {
        val lockKey = "alarm-lock-$id"
        lockTemplate.executeWithLock(lockKey) {
            val alarm = alarmRepositoryPort.findById(id)
                ?: throw NoSuchElementException("알람을 찾을 수 없습니다: $id")

            if (alarm.status == com.alarm.domain.AlarmStatus.TRIGGERED) {
                // Idempotency: skip if already triggered
                return@executeWithLock
            }
            try {
                // 비즈니스 행위: 알람 발생 상태 변경
                alarm.trigger()
                alarmRepositoryPort.save(alarm)

                // 외부 API 호출 전송
                notificationPort.sendNotification(alarm.id, alarm.payload)

                // 실행 이력 기록 (성공)
                val log = com.alarm.domain.AlarmExecutionLog.record(alarm.id, "SUCCESS")
                alarmExecutionLogRepositoryPort.save(log)
            } catch (e: Exception) {
                alarm.fail()
                alarmRepositoryPort.save(alarm)

                // 실행 이력 기록 (실패)
                val log = com.alarm.domain.AlarmExecutionLog.record(alarm.id, "FAILED", e.message)
                alarmExecutionLogRepositoryPort.save(log)
                throw e
            }
        }
    }
}
