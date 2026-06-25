package com.alarm.application.service

import com.alarm.application.port.`in`.*
import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmOutboxEventRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.config.idempotencyKeyToLong
import com.alarm.domain.Alarm
import com.alarm.domain.AlarmExecutionLog
import com.alarm.domain.AlarmOutboxEvent
import com.alarm.domain.AlarmStatus
import com.alarm.exception.AlarmNotFoundException
import com.alarm.exception.DuplicateRequestException
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Duration

/**
 * [파일명] AlarmService.kt
 * [수정일자] 2026-06-16
 * [기능] 알람 비즈니스 유스케이스 처리 구현체
 * [관점] Application Service: 인바운드 포트들을 구현하여 트랜잭션 경계를 설정하고, 멱등성 검증,
 *        도메인 엔티티(Alarm) 상태 변환 제어, 아웃바운드 포트(Repository) 조율 작업을 총괄합니다.
 *        Kafka 발행은 반드시 Outbox 테이블을 통해 수행되며, 직접 발행(이중 쓰기)을 하지 않습니다.
 */
@Service
@Transactional(readOnly = true)
class AlarmService(
    private val alarmRepositoryPort: AlarmRepositoryPort,
    private val alarmOutboxEventRepositoryPort: AlarmOutboxEventRepositoryPort,
    private val alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort,
    private val redisTemplate: StringRedisTemplate
) : CreateAlarmUseCase, GetAlarmQuery, UpdateAlarmUseCase, DeleteAlarmUseCase {

    private val logger = LoggerFactory.getLogger(AlarmService::class.java)

    /**
     * id 문자열을 Long으로 변환하는 헬퍼 메서드.
     * 숫자 문자열이면 toLong(), 아니면 idempotencyKeyToLong 해시로 변환합니다.
     */
    private fun resolveId(id: String): Long =
        id.toLongOrNull() ?: idempotencyKeyToLong(id)

    @Transactional
    override fun createAlarm(command: CreateAlarmUseCase.CreateAlarmCommand): Alarm {
        // ① API 레벨 멱등성 체크 (senderId:id 구분자 포함으로 키 충돌 방지)
        val idemKey = "idem:${command.senderId}:${command.id}"
        val isFirstRequest = redisTemplate.opsForValue().setIfAbsent(
            idemKey,
            "PENDING",
            Duration.ofMinutes(5)
        )
        if (isFirstRequest != true) {
            val existingAlarm = alarmRepositoryPort.findById(command.id)
            if (existingAlarm != null) {
                return existingAlarm
            }
            throw DuplicateRequestException("Duplicate request based on Idempotency-Key: processing")
        }

        try {
            // ② 알람 저장 + Outbox 이벤트 저장 (같은 트랜잭션 — Outbox 패턴)
            val alarm = Alarm.create(
                id = command.id,
                senderId = command.senderId,
                receiverId = command.receiverId,
                channelType = command.channelType,
                name = command.name,
                payload = command.payload
            )
            val savedAlarm = alarmRepositoryPort.save(alarm)

            // Outbox 테이블에만 저장 (Kafka 직접 발행 금지 — 이중 쓰기 방지)
            // 스케줄러(OutboxEventPublisher)가 PENDING 이벤트를 읽어 Kafka에 발행합니다.
            val outboxEvent = AlarmOutboxEvent.create(savedAlarm)
            alarmOutboxEventRepositoryPort.save(outboxEvent)

            return savedAlarm
        } catch (e: Exception) {
            // ③ DB 저장 실패 시 Redis 멱등 키를 즉시 해제 → 클라이언트 즉시 재시도 가능
            redisTemplate.delete(idemKey)
            logger.error("Alarm creation failed, releasing idem key: {}", idemKey, e)
            throw e
        }
    }

    override fun getAlarm(id: String): Alarm {
        return alarmRepositoryPort.findById(resolveId(id))
            ?: throw AlarmNotFoundException("알람을 찾을 수 없습니다: $id")
    }

    override fun listAlarms(pageable: Pageable): Page<Alarm> {
        return alarmRepositoryPort.findAll(pageable)
    }

    override fun getAlarmHistory(
        senderId: String?,
        receiverId: String?,
        status: com.alarm.domain.AlarmStatus?,
        cursorCreatedAt: java.time.LocalDateTime?,
        cursorId: Long?,
        limit: Int
    ): com.alarm.adapter.`in`.web.dto.AlarmHistoryResponse {
        val cutoffDate = java.time.LocalDateTime.now().minusDays(7)
        // limit + 1개를 조회하여 hasNext 여부를 확인합니다.
        val alarms = alarmRepositoryPort.findHistoryByCursor(
            senderId, receiverId, status, cutoffDate, cursorCreatedAt, cursorId, limit + 1
        )
        
        val hasNext = alarms.size > limit
        val resultAlarms = if (hasNext) alarms.take(limit) else alarms
        
        val nextCursorCreatedAt = resultAlarms.lastOrNull()?.createdAt
        val nextCursorId = resultAlarms.lastOrNull()?.id
        
        return com.alarm.adapter.`in`.web.dto.AlarmHistoryResponse(
            alarms = resultAlarms.map { com.alarm.adapter.`in`.web.dto.AlarmResponse.fromDomain(it) },
            nextCursorCreatedAt = nextCursorCreatedAt,
            nextCursorId = nextCursorId,
            hasNext = hasNext
        )
    }

    @Transactional
    override fun updateAlarm(command: UpdateAlarmUseCase.UpdateAlarmCommand): Alarm {
        val alarm = alarmRepositoryPort.findById(resolveId(command.id))
            ?: throw AlarmNotFoundException("알람을 찾을 수 없습니다: ${command.id}")

        // 이름/페이로드 수정은 도메인 메서드에 위임
        alarm.update(name = command.name, payload = command.payload)

        // 상태 변경은 허용된 상태만 화이트리스트로 제한 (상태 머신 불변식 보호)
        // PROCESSING, DEAD, RETRY_WAIT 등 내부 전용 상태는 API로 직접 변경 불가
        command.status?.let { requestedStatus ->
            when (requestedStatus) {
                AlarmStatus.PENDING -> alarm.markPending()
                AlarmStatus.SENT    -> alarm.sent()
                AlarmStatus.FAILED  -> alarm.fail()
                else -> throw IllegalArgumentException(
                    "API를 통한 상태 변경이 허용되지 않는 상태입니다: $requestedStatus"
                )
            }
        }

        return alarmRepositoryPort.save(alarm)
    }

    @Transactional
    override fun deleteAlarm(id: String) {
        val alarm = alarmRepositoryPort.findById(resolveId(id))
            ?: throw AlarmNotFoundException("알람을 찾을 수 없습니다: $id")
        alarmRepositoryPort.delete(alarm)
    }
}
