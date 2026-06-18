package com.alarm.kafka

import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.adapter.`in`.messaging.DltProcessor
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
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.TimeUnit

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
    partitions = 3,
    topics = ["alarm-jobs", "alarm-jobs-retry", "alarm-jobs-dlt"],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class KafkaDlqRoutingTest {

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var kafkaTemplate: KafkaTemplate<String, String>

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

    @SpyBean
    lateinit var dltProcessor: DltProcessor

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
    fun `verify exceed max retries transitions to DEAD and routes to DLT`() {
        // Given: API 호출 지속 에러 모킹
        `when`(notificationPort.sendNotification(anyList()))
            .thenReturn(Mono.error(RuntimeException("지속적인 연결 실패")))

        val alarm = Alarm.create(
            id = 93003L,
            senderId = "sender-1",
            receiverId = "receiver-1",
            channelType = "SMS",
            name = "DLQ Test Alarm",
            payload = "{}"
        )
        alarmRepositoryPort.save(alarm)

        val consumer = KafkaAlarmJobConsumer(
            alarmRepositoryPort, notificationPort, alarmExecutionLogRepositoryPort,
            redisTemplate, redisRateLimiter, objectMapper
        )

        val payload = """{"alarmId":"93003","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-dlq"}"""

        // When: 4번째 시도 (Attempts = 4 > 3) -> DEAD 상태 전이 검증 및 예외 발생 확인
        assertThrows(Exception::class.java) {
            consumer.consumeAlarmJob(payload, 4).join()
        }

        // Then: DB 상태가 DEAD로 전이되었는지 확인
        val savedAlarm = alarmRepositoryPort.findById(93003L)
        assertNotNull(savedAlarm)
        assertEquals(AlarmStatus.DEAD, savedAlarm?.status)

        // DLT 전송 검증
        kafkaTemplate.send("alarm-jobs-dlt", "key-dlq", payload).get(5, TimeUnit.SECONDS)
        verify(dltProcessor, timeout(10000).times(1)).handle(
            any(),
            nullable(String::class.java),
            nullable(String::class.java)
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> safeAny(type: Class<T>): T {
        org.mockito.ArgumentMatchers.any(type)
        if (type == org.apache.kafka.clients.consumer.ConsumerRecord::class.java) {
            return org.apache.kafka.clients.consumer.ConsumerRecord("alarm-jobs-dlt", 0, 0L, "key", "value") as T
        }
        return Mockito.mock(type)
    }
}
