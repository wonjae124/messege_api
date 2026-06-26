package com.alarm.adapter.`in`.messaging

import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.NotificationPort
import com.alarm.domain.Alarm
import com.alarm.domain.AlarmStatus
import com.alarm.exception.ConcurrentProcessingException
import com.alarm.exception.ExternalApiException
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.CompletionException

class KafkaAlarmJobConsumerTest {

    private lateinit var alarmRepositoryPort: AlarmRepositoryPort
    private lateinit var notificationPort: NotificationPort
    private lateinit var alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort
    private lateinit var redisTemplate: StringRedisTemplate
    private lateinit var redisRateLimiter: RedisRateLimiter
    private lateinit var valueOperations: ValueOperations<String, String>
    private val objectMapper = jacksonObjectMapper()

    private lateinit var consumer: KafkaAlarmJobConsumer

    @BeforeEach
    fun setUp() {
        alarmRepositoryPort = mock<AlarmRepositoryPort>()
        notificationPort = mock<NotificationPort>()
        alarmExecutionLogRepositoryPort = mock<AlarmExecutionLogRepositoryPort>()
        redisTemplate = mock<StringRedisTemplate>()
        redisRateLimiter = mock<RedisRateLimiter>()
        
        @Suppress("UNCHECKED_CAST")
        valueOperations = mock()
        whenever(redisTemplate.opsForValue()).thenReturn(valueOperations)

        consumer = KafkaAlarmJobConsumer(
            alarmRepositoryPort,
            notificationPort,
            alarmExecutionLogRepositoryPort,
            redisTemplate,
            redisRateLimiter,
            objectMapper
        )
    }

    private fun createPayloadJson(): String {
        return """
            {
                "alarmId": "1",
                "senderId": "sender1",
                "receiverId": "receiver1",
                "uuid": "uuid1234",
                "requestId": "req1"
            }
        """.trimIndent()
    }

    @Test
    fun `consumeAlarmJob - 정상 처리 시 알람을 발송하고 DB 상태를 업데이트한다`() {
        // given
        val payloadStr = createPayloadJson()
        val alarm = Alarm.create(1L, "sender1", "receiver1", "SMS", "Test", "Msg")
        
        // 1차 및 2차 DCL 검증 시 PENDING 상태의 알람 반환
        whenever(alarmRepositoryPort.findById(1L)).thenReturn(alarm)
        
        // Redis 락 획득 성공
        whenever(valueOperations.setIfAbsent(any(), eq("PROCESSING"), any<Duration>())).thenReturn(true)
        
        // RateLimiter 토큰 획득 성공
        whenever(redisRateLimiter.tryAcquire(1)).thenReturn(true)
        
        // Notification API 호출 성공
        whenever(notificationPort.sendNotification(any())).thenReturn(Mono.empty())

        // when
        val future = consumer.consumeAlarmJob(payloadStr, 1)
        future.join() // 대기

        // then
        assertEquals(AlarmStatus.SENT, alarm.status)
        verify(notificationPort, times(1)).sendNotification(any())
        verify(alarmExecutionLogRepositoryPort, times(1)).save(any())
        verify(valueOperations, times(1)).set(any<String>(), eq("DONE"), any<Duration>())
    }

    @Test
    fun `consumeAlarmJob - 이미 최종 상태인 경우 Redis 락 경합 없이 스킵한다 (DCL 1차)`() {
        // given
        val payloadStr = createPayloadJson()
        val alarm = Alarm.create(1L, "s", "r", "c", "n", "p").apply { sent() }
        
        whenever(alarmRepositoryPort.findById(1L)).thenReturn(alarm)

        // when
        val future = consumer.consumeAlarmJob(payloadStr, 1)
        future.join()

        // then
        verify(valueOperations, times(1)).set(any<String>(), eq("DONE"), any<Duration>())
        verify(notificationPort, never()).sendNotification(any())
    }

    @Test
    fun `consumeAlarmJob - Redis 락 획득 실패 시, 기존 상태가 DONE 이면 스킵한다`() {
        // given
        val payloadStr = createPayloadJson()
        val alarm = Alarm.create(1L, "s", "r", "c", "n", "p")
        whenever(alarmRepositoryPort.findById(1L)).thenReturn(alarm)
        
        whenever(valueOperations.setIfAbsent(any(), eq("PROCESSING"), any<Duration>())).thenReturn(false)
        whenever(valueOperations.get(any<String>())).thenReturn("DONE")

        // when
        val future = consumer.consumeAlarmJob(payloadStr, 1)
        future.join()

        // then
        verify(notificationPort, never()).sendNotification(any())
    }

    @Test
    fun `consumeAlarmJob - Redis 락 획득 실패 시, 기존 상태가 PROCESSING 이면 예외 발생`() {
        // given
        val payloadStr = createPayloadJson()
        val alarm = Alarm.create(1L, "s", "r", "c", "n", "p")
        whenever(alarmRepositoryPort.findById(1L)).thenReturn(alarm)
        
        whenever(valueOperations.setIfAbsent(any(), eq("PROCESSING"), any<Duration>())).thenReturn(false)
        whenever(valueOperations.get(any<String>())).thenReturn("PROCESSING")

        // when & then
        val ex = assertThrows(Exception::class.java) {
            consumer.consumeAlarmJob(payloadStr, 1).join()
        }
        val actualCause = ex.cause ?: ex
        assertTrue(actualCause is ConcurrentProcessingException)
        verify(notificationPort, never()).sendNotification(any())
    }

    @Test
    fun `consumeAlarmJob - 외부 API 400 에러 발생 시 알람을 FAILED로 마킹한다`() {
        // given
        val payloadStr = createPayloadJson()
        val alarm = Alarm.create(1L, "s", "r", "c", "n", "p")
        whenever(alarmRepositoryPort.findById(1L)).thenReturn(alarm)
        whenever(valueOperations.setIfAbsent(any(), eq("PROCESSING"), any<Duration>())).thenReturn(true)
        whenever(redisRateLimiter.tryAcquire(1)).thenReturn(true)
        
        // 400 에러 모의 (Client Error)
        whenever(notificationPort.sendNotification(any())).thenReturn(Mono.error(ExternalApiException(400, "Bad Request")))

        // when
        val future = consumer.consumeAlarmJob(payloadStr, 1)
        future.join() // 4xx 에러는 Mono.empty() 로 리턴되므로 에러 전파되지 않음 (재시도 방지)

        // then
        assertEquals(AlarmStatus.FAILED, alarm.status)
        verify(redisTemplate, times(1)).delete(any<String>()) // 실패 시 락 해제
        verify(alarmExecutionLogRepositoryPort, times(1)).save(any())
    }
}
