package com.alarm.application.service

import com.alarm.application.port.`in`.CreateAlarmUseCase
import com.alarm.application.port.out.*
import com.alarm.domain.Alarm
import com.alarm.domain.AlarmExecutionLog
import com.alarm.domain.AlarmOutboxEvent
import com.alarm.domain.AlarmStatus
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito
import org.mockito.Mockito.*
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.web.server.ResponseStatusException
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * [대상] AlarmService — 애플리케이션 서비스 레이어
 * [범주] Service Unit Test — Mockito Mock, 외부 의존성 없음
 *
 * createAlarm
 *   ① DB에 PENDING 상태로 저장
 *   ② Kafka 큐(MessageQueue)에 발행
 *   ③ Redis 멱등키 중복 → CONFLICT(409) 예외
 *
 * triggerAlarm
 *   ④ 이미 SENT 상태 → 재발송 Skip
 *   ⑤ 발송 성공 → DB SENT 갱신 + SUCCESS 이력 기록
 *   ⑥ 발송 실패 → DB FAILED 갱신 + FAILED 이력 기록
 */
class AlarmServiceTest {

    private lateinit var alarmRepositoryPort: AlarmRepositoryPort
    private lateinit var alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort
    private lateinit var notificationPort: NotificationPort
    private lateinit var messageQueuePort: MessageQueuePort
    private lateinit var alarmOutboxEventRepositoryPort: AlarmOutboxEventRepositoryPort
    private lateinit var redisTemplate: StringRedisTemplate
    private lateinit var valueOps: ValueOperations<String, String>
    private lateinit var alarmService: AlarmService

    private val dummyAlarm = Alarm.create(
        id = "dummy-id",
        senderId = "sender-1",
        receiverId = "receiver-1",
        name = "Dummy",
        payload = null
    )
    private val dummyLog = AlarmExecutionLog.record("dummy", "SUCCESS")

    @Suppress("UNCHECKED_CAST")
    @BeforeEach
    fun setUp() {
        alarmRepositoryPort = mock(AlarmRepositoryPort::class.java)
        alarmExecutionLogRepositoryPort = mock(AlarmExecutionLogRepositoryPort::class.java)
        notificationPort = mock(NotificationPort::class.java)
        messageQueuePort = mock(MessageQueuePort::class.java)
        alarmOutboxEventRepositoryPort = mock(AlarmOutboxEventRepositoryPort::class.java)
        redisTemplate = mock(StringRedisTemplate::class.java)
        valueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>

        `when`(redisTemplate.opsForValue()).thenReturn(valueOps)

        alarmService = AlarmService(
            alarmRepositoryPort,
            alarmExecutionLogRepositoryPort,
            notificationPort,
            messageQueuePort,
            alarmOutboxEventRepositoryPort,
            redisTemplate
        )
    }

    /**
     * [시나리오 1] 단기 락 획득 성공 시, DB에 알람과 Outbox 이벤트를 정상 저장합니다.
     */
    @Test
    fun `createAlarm - should save alarm and outbox event when lock acquired`() {
        val command = CreateAlarmUseCase.CreateAlarmCommand("test-uuid-123", "sender-1", "receiver-1", "Test", "{}")
        
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        `when`(alarmRepositoryPort.findById("test-uuid-123")).thenReturn(null)
        
        val alarmCaptor = ArgumentCaptor.forClass(Alarm::class.java)
        `when`(alarmRepositoryPort.save(alarmCaptor.capture() ?: dummyAlarm)).thenAnswer { it.getArgument(0) }

        alarmService.createAlarm(command)

        assertNotNull(alarmCaptor.value)
        assertEquals(AlarmStatus.PENDING, alarmCaptor.value.status)
        verify(alarmOutboxEventRepositoryPort).save(any(AlarmOutboxEvent::class.java) ?: mock(AlarmOutboxEvent::class.java))
    }

    /**
     * [시나리오 2] 단기 락 획득 실패(또는 락 만료 후 재진입) 하였으나, DB에 이미 저장된 결과가 있으면 기존 객체를 반환합니다. (멱등성 보장)
     */
    @Test
    fun `createAlarm - should return existing alarm if already saved in DB`() {
        val command = CreateAlarmUseCase.CreateAlarmCommand("test-uuid-123", "sender-1", "receiver-1", "Test", "{}")
        
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(false)
        `when`(alarmRepositoryPort.findById("test-uuid-123")).thenReturn(dummyAlarm)

        val result = alarmService.createAlarm(command)

        assertEquals("dummy-id", result.id) // 기존 객체 반환 확인
        verify(alarmRepositoryPort, never()).save(any(Alarm::class.java) ?: dummyAlarm) // 신규 저장 발생 안 함
    }

    /**
     * [시나리오 3] 단기 락 획득 실패 시, DB에도 값이 없다면 다른 스레드가 방금 락을 잡고 처리 중인 것이므로 409 예외를 반환합니다. (따닥 방어)
     */
    @Test
    fun `createAlarm - should throw CONFLICT if locked and DB is empty (Race Condition)`() {
        val command = CreateAlarmUseCase.CreateAlarmCommand("test-uuid-123", "sender-1", "receiver-1", "Test", "{}")
        
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(false)
        `when`(alarmRepositoryPort.findById("test-uuid-123")).thenReturn(null)

        val ex = assertThrows<ResponseStatusException> {
            alarmService.createAlarm(command)
        }
        
        assertEquals(409, ex.statusCode.value())
    }

    /**
     * [시나리오 4] DB 저장 과정에서 예외가 발생하면, 즉시 재시도 가능하도록 catch 블록에서 Redis 락을 해제합니다. (장애 복구)
     */
    @Test
    fun `createAlarm - should delete Redis key if DB save fails`() {
        val command = CreateAlarmUseCase.CreateAlarmCommand("test-uuid-123", "sender-1", "receiver-1", "Test", "{}")
        val idemKey = "idem:sender-1testuuid123"
        
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        `when`(alarmRepositoryPort.findById("test-uuid-123")).thenReturn(null)
        `when`(alarmRepositoryPort.save(any(Alarm::class.java) ?: dummyAlarm)).thenThrow(RuntimeException("DB Connection Timeout"))

        assertThrows<RuntimeException> {
            alarmService.createAlarm(command)
        }

        // DB 장애 발생 시 즉시 Redis 키가 삭제되는지 검증!
        verify(redisTemplate).delete(idemKey)
    }

    /**
     * [시나리오] 이미 발송 완료(SENT)인 알람은 재발송(Trigger)하지 않고 건너뛰는지 검증합니다.
     * [Given] 상태가 SENT인 알람 준비 및 findById 설정
     * [When] 알람 발송 trigger 호출
     * [Then] 외부 전송 API 호출(sendNotification)이 수행되지 않는지 검증
     */
    @Test
    fun `triggerAlarm - should skip if status is already SENT`() {
        // Given
        val alarm = Alarm.create(
            id = "alarm-123",
            senderId = "sender-1",
            receiverId = "receiver-1",
            name = "Already Sent Alarm",
            payload = "{}"
        )
        alarm.sent()
        `when`(alarmRepositoryPort.findById("alarm-123")).thenReturn(alarm)

        // When
        alarmService.triggerAlarm("alarm-123")

        // Then
        verify(notificationPort, never()).sendNotification(anyList())
    }

    /**
     * [시나리오] 알람 발송 성공 시 DB에 완료(SENT) 상태로 갱신 및 로그 기록을 검증합니다.
     * [Given] PENDING 상태의 알람 준비 및 외부 전송 API 성공 설정
     * [When] 알람 발송 trigger 호출
     * [Then] DB에 알람 상태가 SENT로 업데이트 저장되고, SUCCESS 실행 로그가 남는지 검증
     */
    @Test
    fun `triggerAlarm - should update status to SENT and record SUCCESS log on success`() {
        // Given
        val alarm = Alarm.create(
            id = "alarm-123",
            senderId = "sender-1",
            receiverId = "receiver-1",
            name = "Pending Alarm",
            payload = "{}"
        )
        `when`(alarmRepositoryPort.findById("alarm-123")).thenReturn(alarm)
        `when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.empty())
        
        val alarmCaptor = ArgumentCaptor.forClass(Alarm::class.java)
        `when`(alarmRepositoryPort.save(alarmCaptor.capture() ?: dummyAlarm)).thenAnswer { it.getArgument(0) }

        // When
        alarmService.triggerAlarm("alarm-123")

        // Then
        val savedAlarm = alarmCaptor.value
        assertNotNull(savedAlarm)
        assertEquals(AlarmStatus.SENT, savedAlarm.status) // H2 DB에 SENT(Done) 상태로 업데이트되는지 검증
        
        verify(alarmExecutionLogRepositoryPort).save(any(AlarmExecutionLog::class.java) ?: dummyLog)
    }

    /**
     * [시나리오] 알람 발송 실패 시 DB에 실패(FAILED) 상태 갱신 및 에러 로그 기록을 검증합니다.
     * [Given] PENDING 상태의 알람 준비 및 외부 전송 API 오류 발생 설정
     * [When] 알람 발송 trigger 호출
     * [Then] DB에 알람 상태가 FAILED로 업데이트 저장되고, FAILED 실행 로그가 남는지 검증
     */
    @Test
    fun `triggerAlarm - should update status to FAILED and record FAILED log on failure`() {
        // Given
        val alarm = Alarm.create(
            id = "alarm-123",
            senderId = "sender-1",
            receiverId = "receiver-1",
            name = "Pending Alarm",
            payload = "{}"
        )
        `when`(alarmRepositoryPort.findById("alarm-123")).thenReturn(alarm)
        `when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.error(RuntimeException("Connection timeout")))
        
        val alarmCaptor = ArgumentCaptor.forClass(Alarm::class.java)
        `when`(alarmRepositoryPort.save(alarmCaptor.capture() ?: dummyAlarm)).thenAnswer { it.getArgument(0) }

        // When & Then
        assertThrows<RuntimeException> {
            alarmService.triggerAlarm("alarm-123")
        }

        val savedAlarm = alarmCaptor.value
        assertNotNull(savedAlarm)
        assertEquals(AlarmStatus.FAILED, savedAlarm.status) // H2 DB에 FAILED 상태로 업데이트되는지 검증
        
        verify(alarmExecutionLogRepositoryPort).save(any(AlarmExecutionLog::class.java) ?: dummyLog)
    }
}
