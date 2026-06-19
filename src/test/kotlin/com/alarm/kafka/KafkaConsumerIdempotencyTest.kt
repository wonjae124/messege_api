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
    topics = ["alarm-jobs"],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class KafkaConsumerIdempotencyTest {

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
    fun `verify RDB status based idempotency check`() {
        // Given: 이미 SENT 완료 상태인 알람 생성
        val alarm = Alarm.create(
            id = 94004L,
            senderId = "sender-1",
            receiverId = "receiver-1",
            channelType = "SMS",
            name = "Already Sent Alarm",
            payload = "{}"
        )
        alarm.sent()
        alarmRepositoryPort.save(alarm)

        val consumer = KafkaAlarmJobConsumer(
            alarmRepositoryPort, notificationPort, alarmExecutionLogRepositoryPort,
            redisTemplate, redisRateLimiter, objectMapper
        )

        val payload = """{"alarmId":"94004","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-rdb-dup"}"""

        // When: 동일한 메시지 소비 시도
        consumer.consumeAlarmJob(payload, 1).join()

        // Then: DB 상태 1차 검사로 외부 API 호출이 완벽히 생략되어야 함
        verify(notificationPort, never()).sendNotification(anyList())
    }

    @Test
    fun `verify Redis lock based concurrency prevention`() {
        // Given: Redis setIfAbsent가 false를 반환하여 락 획득 실패 (이미 처리 중인 상황 모킹)
        val procKey = "proc:sender-1:uuid-redis-dup"
        
        // 1차 DB 조회는 PENDING 상태로 통과하도록 설정
        val alarm = Alarm.create(
            id = 94005L,
            senderId = "sender-1",
            receiverId = "receiver-1",
            channelType = "SMS",
            name = "Redis Locked Alarm",
            payload = "{}"
        )
        alarmRepositoryPort.save(alarm)

        `when`(valueOps.setIfAbsent(eq(procKey), anyString(), any(Duration::class.java))).thenReturn(false)
        `when`(valueOps.get(eq(procKey))).thenReturn("PROCESSING")

        val consumer = KafkaAlarmJobConsumer(
            alarmRepositoryPort, notificationPort, alarmExecutionLogRepositoryPort,
            redisTemplate, redisRateLimiter, objectMapper
        )

        val payload = """{"alarmId":"94005","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-redis-dup"}"""

        // When: 소비 진행 (PROCESSING 상태이므로 ConcurrentProcessingException 발생)
        assertThrows(Exception::class.java) {
            consumer.consumeAlarmJob(payload, 1).join()
        }

        // Then: 락을 획득하지 못했으므로 외부 API 호출이 생략되어야 함
        verify(notificationPort, never()).sendNotification(anyList())
    }
}
