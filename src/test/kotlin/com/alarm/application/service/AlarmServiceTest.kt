package com.alarm.application.service

import com.alarm.application.port.`in`.CreateAlarmUseCase
import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmOutboxEventRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.domain.Alarm
import com.alarm.domain.AlarmOutboxEvent
import com.alarm.exception.DuplicateRequestException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.*
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import java.time.Duration

class AlarmServiceTest {

    private lateinit var alarmRepositoryPort: AlarmRepositoryPort
    private lateinit var alarmOutboxEventRepositoryPort: AlarmOutboxEventRepositoryPort
    private lateinit var alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort
    private lateinit var redisTemplate: StringRedisTemplate
    private lateinit var valueOperations: ValueOperations<String, String>

    private lateinit var alarmService: AlarmService

    @BeforeEach
    fun setUp() {
        alarmRepositoryPort = mock<AlarmRepositoryPort>()
        alarmOutboxEventRepositoryPort = mock<AlarmOutboxEventRepositoryPort>()
        alarmExecutionLogRepositoryPort = mock<AlarmExecutionLogRepositoryPort>()
        redisTemplate = mock<StringRedisTemplate>()
        
        @Suppress("UNCHECKED_CAST")
        valueOperations = mock<ValueOperations<String, String>>()
        whenever(redisTemplate.opsForValue()).thenReturn(valueOperations)

        alarmService = AlarmService(
            alarmRepositoryPort,
            alarmOutboxEventRepositoryPort,
            alarmExecutionLogRepositoryPort,
            redisTemplate
        )
    }

    @Test
    fun `createAlarm - 성공적으로 알람을 생성하고 Outbox에 저장한다`() {
        // given
        val command = CreateAlarmUseCase.CreateAlarmCommand(
            id = 1234L,
            senderId = "sender1",
            receiverId = "receiver1",
            channelType = "SMS",
            name = "Test Alarm",
            payload = "Hello"
        )
        val idemKey = "idem:sender1:1234"
        whenever(valueOperations.setIfAbsent(eq(idemKey), eq("PENDING"), any<Duration>())).thenReturn(true)
        
        val expectedAlarm = Alarm.create(1234L, command.senderId, command.receiverId, command.channelType, command.name, command.payload)
        whenever(alarmRepositoryPort.save(any<Alarm>())).thenReturn(expectedAlarm)

        // when
        val result = alarmService.createAlarm(command)

        // then
        assertNotNull(result)
        assertEquals(1234L, result.id)
        verify(alarmRepositoryPort, times(1)).save(any<Alarm>())
        verify(alarmOutboxEventRepositoryPort, times(1)).save(any<AlarmOutboxEvent>())
    }

    @Test
    fun `createAlarm - 멱등성 키가 존재하고 DB에 이미 저장되어 있으면, 기존 알람을 반환한다`() {
        // given
        val command = CreateAlarmUseCase.CreateAlarmCommand(
            id = 1234L,
            senderId = "sender1",
            receiverId = "receiver1",
            channelType = "SMS",
            name = "Test Alarm",
            payload = "Hello"
        )
        val idemKey = "idem:sender1:1234"
        whenever(valueOperations.setIfAbsent(eq(idemKey), eq("PENDING"), any<Duration>())).thenReturn(false)
        
        val existingAlarm = Alarm.create(1234L, command.senderId, command.receiverId, command.channelType, command.name, command.payload)
        whenever(alarmRepositoryPort.findById(1234L)).thenReturn(existingAlarm)

        // when
        val result = alarmService.createAlarm(command)

        // then
        assertEquals(existingAlarm, result)
        verify(alarmRepositoryPort, never()).save(any<Alarm>())
        verify(alarmOutboxEventRepositoryPort, never()).save(any<AlarmOutboxEvent>())
    }

    @Test
    fun `createAlarm - 멱등성 키가 존재하지만 DB에 없으면, DuplicateRequestException을 던진다`() {
        // given
        val command = CreateAlarmUseCase.CreateAlarmCommand(
            id = 1234L,
            senderId = "sender1",
            receiverId = "receiver1",
            channelType = "SMS",
            name = "Test Alarm",
            payload = "Hello"
        )
        val idemKey = "idem:sender1:1234"
        whenever(valueOperations.setIfAbsent(eq(idemKey), eq("PENDING"), any<Duration>())).thenReturn(false)
        whenever(alarmRepositoryPort.findById(1234L)).thenReturn(null)

        // when & then
        assertThrows(DuplicateRequestException::class.java) {
            alarmService.createAlarm(command)
        }
        verify(alarmRepositoryPort, never()).save(any<Alarm>())
    }

    @Test
    fun `createAlarm - DB 저장 실패 시, Redis 멱등 키를 삭제하고 예외를 전파한다`() {
        // given
        val command = CreateAlarmUseCase.CreateAlarmCommand(
            id = 1234L,
            senderId = "sender1",
            receiverId = "receiver1",
            channelType = "SMS",
            name = "Test Alarm",
            payload = "Hello"
        )
        val idemKey = "idem:sender1:1234"
        whenever(valueOperations.setIfAbsent(eq(idemKey), eq("PENDING"), any<Duration>())).thenReturn(true)
        whenever(alarmRepositoryPort.save(any<Alarm>())).thenThrow(RuntimeException("DB save failed"))

        // when & then
        val ex = assertThrows(RuntimeException::class.java) {
            alarmService.createAlarm(command)
        }
        assertEquals("DB save failed", ex.message)
        
        // Redis 키가 삭제되었는지 확인
        verify(redisTemplate, times(1)).delete(idemKey)
    }
}
