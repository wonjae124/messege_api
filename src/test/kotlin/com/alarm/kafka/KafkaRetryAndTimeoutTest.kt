package com.alarm.kafka

import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.adapter.`in`.messaging.KafkaAlarmJobConsumer
import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.NotificationPort
import com.alarm.domain.Alarm
import com.alarm.domain.AlarmStatus
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito
import org.mockito.Mockito.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.kafka.test.context.EmbeddedKafka
import reactor.core.publisher.Mono
import java.time.Duration

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
    partitions = 3,
    topics = ["alarm-jobs", "alarm-jobs-retry"],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class KafkaRetryAndTimeoutTest {

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var alarmRepositoryPort: AlarmRepositoryPort

    @Autowired
    lateinit var alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort

    @MockBean
    lateinit var notificationPort: NotificationPort

    @MockBean
    lateinit var redisTemplate: StringRedisTemplate

    @MockBean
    lateinit var redisRateLimiter: RedisRateLimiter

    private lateinit var valueOps: ValueOperations<String, String>

    @BeforeEach
    @Suppress("UNCHECKED_CAST")
    fun setUp() {
        valueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>
        `when`(redisTemplate.opsForValue()).thenReturn(valueOps)
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        `when`(redisRateLimiter.tryAcquire(anyInt())).thenReturn(true)
        `when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.empty())
    }

    @Test
    fun `verify timeout retry and RETRY_WAIT status`() {
        // Given: API 호출 시 일시 에러(Timeout/RuntimeException) 발생 모킹
        `when`(notificationPort.sendNotification(anyList()))
            .thenReturn(Mono.error(RuntimeException("외부 API 연결 시간초과 (Timeout)")))

        val alarm = Alarm.create(
            id = 92001L,
            senderId = "sender-1",
            receiverId = "receiver-1",
            channelType = "SMS",
            name = "Timeout Test Alarm",
            payload = "{}"
        )
        alarmRepositoryPort.save(alarm)

        val consumer = KafkaAlarmJobConsumer(
            alarmRepositoryPort, notificationPort, alarmExecutionLogRepositoryPort,
            redisTemplate, redisRateLimiter, objectMapper
        )

        val payload = """{"alarmId":"92001","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-timeout"}"""

        // When: 첫 번째 시도 (attempt = 1) -> 예외가 다시 카프카 컨테이너로 상향 방출되어야 함
        assertThrows(Exception::class.java) {
            consumer.consumeAlarmJob(payload, 1).join()
        }

        // Then: DB 상태가 RETRY_WAIT로 변경되었는지 확인
        val savedAlarm = alarmRepositoryPort.findById(92001L)
        assertNotNull(savedAlarm)
        assertEquals(AlarmStatus.RETRY_WAIT, savedAlarm?.status)
    }

    @Test
    fun `verify retry attempts up to 3 times`() {
        // Given: 일시적인 전송 실패 시나리오
        `when`(notificationPort.sendNotification(anyList()))
            .thenReturn(Mono.error(RuntimeException("일시적인 네트워크 장애")))

        val alarm = Alarm.create(
            id = 92002L,
            senderId = "sender-1",
            receiverId = "receiver-1",
            channelType = "SMS",
            name = "Retry Test Alarm",
            payload = "{}"
        )
        alarmRepositoryPort.save(alarm)

        val consumer = KafkaAlarmJobConsumer(
            alarmRepositoryPort, notificationPort, alarmExecutionLogRepositoryPort,
            redisTemplate, redisRateLimiter, objectMapper
        )

        val payload = """{"alarmId":"92002","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-retry-3"}"""

        // attempts 1, 2, 3 차례대로 실행하여 매번 RETRY_WAIT 상태 유지 및 예외 방출 검증
        for (attempt in 1..3) {
            assertThrows(Exception::class.java) {
                consumer.consumeAlarmJob(payload, attempt).join()
            }
            val savedAlarm = alarmRepositoryPort.findById(92002L)
            assertEquals(AlarmStatus.RETRY_WAIT, savedAlarm?.status)
        }
    }
}
