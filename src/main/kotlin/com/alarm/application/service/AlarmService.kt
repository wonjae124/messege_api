package com.alarm.application.service

import com.alarm.application.port.`in`.*
import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmOutboxEventRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.MessageQueuePort
import com.alarm.application.port.out.NotificationPort
import com.alarm.application.port.out.AlarmBatchItem
import com.alarm.domain.Alarm
import com.alarm.domain.AlarmExecutionLog
import com.alarm.domain.AlarmOutboxEvent
import com.alarm.domain.AlarmStatus
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.server.ResponseStatusException
import java.time.Duration

/**
 * [파일명] AlarmService.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 비즈니스 유스케이스 처리 구현체
 * [관점] Application Service: 인바운드 포트들을 구현하여 트랜잭션 경계를 설정하고, 멱등성 검증, 도메인 엔티티(Alarm) 상태 변환 제어, 아웃바운드 포트(Repository, MessageQueue, Notification) 조율 작업을 총괄합니다.
 */
@Service
@Transactional(readOnly = true)
class AlarmService(
    private val alarmRepositoryPort: AlarmRepositoryPort,
    private val alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort,
    private val notificationPort: NotificationPort,
    private val messageQueuePort: MessageQueuePort,
    private val alarmOutboxEventRepositoryPort: AlarmOutboxEventRepositoryPort,
    private val redisTemplate: StringRedisTemplate
) : CreateAlarmUseCase, GetAlarmQuery, UpdateAlarmUseCase, DeleteAlarmUseCase, TriggerAlarmUseCase {

    @Transactional
    override fun createAlarm(command: CreateAlarmUseCase.CreateAlarmCommand): Alarm {
        // API 레벨 멱등성 체크
        val uuidClean = command.id.replace("-", "")
        val idemKey = "idem:${command.senderId}$uuidClean"

        // 1. 단기 동시성 방어 (Redis 락 선점)
        val isFirstRequest = redisTemplate.opsForValue().setIfAbsent(
            idemKey,
            "PROCESSING",
            Duration.ofMinutes(5)
        )

        if (isFirstRequest != true) {
            // Redis 키가 이미 존재함: 과거에 성공했거나, 다른 스레드가 현재 처리 중임
            // 2. 재처리 상태 확인: DB에 이미 반영되었는지 확인 (Source of Truth)
            val existingAlarm = alarmRepositoryPort.findById(command.id)
            if (existingAlarm != null) {
                return existingAlarm // 이미 성공한 요청이므로 기존 결과 반환 (멱등성 보장)
            }
            // 아직 DB에 없다면 현재 다른 스레드가 트랜잭션을 잡고 처리 중인 상태임
            throw ResponseStatusException(
                HttpStatus.CONFLICT,
                "Request is currently being processed"
            )
        }

        try {
            // 3. 장기 멱등성 방어: Redis 키가 만료된 이후의 중복 요청 방어
            val existingAlarm = alarmRepositoryPort.findById(command.id)
            if (existingAlarm != null) {
                return existingAlarm
            }

            // 정적 팩토리 메서드를 사용하여 의도 명확화 및 검증 로직 강제화
            val alarm = Alarm.create(
                id = command.id,
                senderId = command.senderId,
                receiverId = command.receiverId,
                name = command.name,
                payload = command.payload
                // default status : PENDING
            )

            val savedAlarm = alarmRepositoryPort.save(alarm)

            // [Outbox Pattern] Kafka 직접 발행 대신, 같은 트랜잭션 내에 Outbox 이벤트 저장
            // OutboxEventPublisher 스케줄러가 PENDING 이벤트를 읽어 Kafka에 발행
            val outboxEvent = AlarmOutboxEvent.create(savedAlarm)
            alarmOutboxEventRepositoryPort.save(outboxEvent)

            return savedAlarm
        } catch (e: Exception) {
            // 🚨 [장애 복구 로직] DB 저장이 실패했다면 클라이언트가 즉시 재시도할 수 있도록 Redis 키 삭제
            redisTemplate.delete(idemKey)
            throw e
        }
    }

    override fun getAlarm(id: String): Alarm {
        return alarmRepositoryPort.findById(id)
            ?: throw NoSuchElementException("알람을 찾을 수 없습니다: $id")
    }

    override fun listAlarms(pageable: Pageable): Page<Alarm> {
        return alarmRepositoryPort.findAll(pageable)
    }

    @Transactional
    override fun updateAlarm(command: UpdateAlarmUseCase.UpdateAlarmCommand): Alarm {
        val alarm = alarmRepositoryPort.findById(command.id)
            ?: throw NoSuchElementException("알람을 찾을 수 없습니다: ${command.id}")

        // 도메인 내부 메서드에 행위와 상태 수정 위임
        alarm.update(
            name = command.name,
            payload = command.payload
        )
        command.status?.let {
            when (it) {
                AlarmStatus.PENDING -> alarm.markPending()
                AlarmStatus.SENT -> alarm.sent()
                AlarmStatus.FAILED -> alarm.fail()
                else -> { /* no-op or handle other statuses if needed */ }
            }
        }
        return alarmRepositoryPort.save(alarm)
    }

    @Transactional
    override fun deleteAlarm(id: String) {
        val alarm = alarmRepositoryPort.findById(id)
            ?: throw NoSuchElementException("알람을 찾을 수 없습니다: $id")
        alarmRepositoryPort.delete(alarm)
    }

    @Transactional
    override fun triggerAlarm(id: String) {
        val alarm = alarmRepositoryPort.findById(id)
            ?: throw NoSuchElementException("알람을 찾을 수 없습니다: $id")

        if (alarm.status == AlarmStatus.SENT) {
            // Idempotency: skip if already sent
            return
        }
        try {
            // 비즈니스 행위: 알람 발생 상태 변경
            alarm.sent()
            alarmRepositoryPort.save(alarm)

            // 외부 API 호출 전송
            val batchItem = AlarmBatchItem(
                alarmId = alarm.id,
                senderId = alarm.senderId,
                uuidClean = alarm.id.replace("-", ""),
                payload = alarm.payload
            )
            notificationPort.sendNotification(listOf(batchItem)).block()

            // 실행 이력 기록 (성공)
            val log = AlarmExecutionLog.record(alarm.id, "SUCCESS")
            alarmExecutionLogRepositoryPort.save(log)
        } catch (e: Exception) {
            alarm.fail()
            alarmRepositoryPort.save(alarm)

            // 실행 이력 기록 (실패)
            val log = AlarmExecutionLog.record(alarm.id, "FAILED", e.message)
            alarmExecutionLogRepositoryPort.save(log)
            throw e
        }
    }
}
