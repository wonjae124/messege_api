package com.alarm.application.service

import com.alarm.application.port.`in`.CreateAlarmUseCase
import com.alarm.application.port.out.*
import com.alarm.domain.Alarm
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
import com.alarm.exception.DuplicateRequestException
import java.time.Duration

/**
 * [대상] AlarmService — 애플리케이션 서비스 레이어
 * [범주] Service Unit Test — Mockito Mock, 외부 의존성 없음
 *
 * createAlarm
 *   ① DB에 PENDING 상태로 저장 + Outbox 이벤트 저장 (Outbox 패턴)
 *   ② Redis 멱등키 중복 → CONFLICT(409) 예외
 */
class AlarmServiceTest {

    private lateinit var alarmRepositoryPort: AlarmRepositoryPort
    private lateinit var alarmOutboxEventRepositoryPort: AlarmOutboxEventRepositoryPort
    private lateinit var alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort
    private lateinit var redisTemplate: StringRedisTemplate
    private lateinit var valueOps: ValueOperations<String, String>
    private lateinit var alarmService: AlarmService

    private val dummyAlarm = Alarm.create(
        id = 99999L,
        senderId = "sender-1",
        receiverId = "receiver-1",
        channelType = "SMS",
        name = "Dummy",
        payload = null
    )

    @Suppress("UNCHECKED_CAST")
    @BeforeEach
    fun setUp() {
        alarmRepositoryPort = mock(AlarmRepositoryPort::class.java)
        alarmOutboxEventRepositoryPort = mock(AlarmOutboxEventRepositoryPort::class.java)
        alarmExecutionLogRepositoryPort = mock(AlarmExecutionLogRepositoryPort::class.java)
        redisTemplate = mock(StringRedisTemplate::class.java)
        valueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>

        `when`(redisTemplate.opsForValue()).thenReturn(valueOps)

        alarmService = AlarmService(
            alarmRepositoryPort,
            alarmOutboxEventRepositoryPort,
            alarmExecutionLogRepositoryPort,
            redisTemplate
        )
    }

    /**
     * [시나리오] 최초 알람 생성 시 DB에 저장되는 알람의 초기 상태를 검증합니다.
     * [Given] 생성 명령 파라미터 준비 및 Redis 중복검사 통과 설정
     * [When] 알람 생성 메서드 호출
     * [Then] DB에 저장(save) 요청된 알람의 상태(status)가 PENDING인지 검증
     */
    @Test
    fun `createAlarm - should save alarm with PENDING status in DB`() {
        // Given
        val command = CreateAlarmUseCase.CreateAlarmCommand(
            id = 12345L,
            senderId = "sender-1",
            receiverId = "receiver-1",
            channelType = "SMS",
            name = "Test Alarm",
            payload = "{}"
        )
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)

        val alarmCaptor = ArgumentCaptor.forClass(Alarm::class.java)
        `when`(alarmRepositoryPort.save(alarmCaptor.capture() ?: dummyAlarm)).thenAnswer { it.getArgument(0) }

        // When
        alarmService.createAlarm(command)

        // Then
        val savedAlarm = alarmCaptor.value
        assertNotNull(savedAlarm)
        assertEquals(AlarmStatus.PENDING, savedAlarm.status) // DB 저장 시 PENDING 상태 검증
    }

    /**
     * [시나리오] Redis에 이미 멱등키가 등록되어 있어 중복 생성 요청인 경우 예외 발생을 검증합니다.
     * [Given] 중복 멱등키 인입 시 Redis setIfAbsent가 false를 반환하도록 설정
     * [When] 알람 생성 메서드 호출
     * [Then] DuplicateRequestException 예외가 발생하는지 검증
     */
    @Test
    fun `createAlarm - should throw CONFLICT exception on duplicate request`() {
        // Given
        val command = CreateAlarmUseCase.CreateAlarmCommand(
            id = 12345L,
            senderId = "sender-1",
            receiverId = "receiver-1",
            channelType = "SMS",
            name = "Test Alarm",
            payload = "{}"
        )
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(false)

        // When & Then
        assertThrows<DuplicateRequestException> {
            alarmService.createAlarm(command)
        }
        verify(alarmRepositoryPort, never()).save(any(Alarm::class.java) ?: dummyAlarm)
    }
}
