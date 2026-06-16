package com.alarm.kafka

import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.adapter.`in`.messaging.KafkaAlarmJobConsumer
import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.NotificationPort
import com.alarm.domain.Alarm
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
import java.util.concurrent.CompletableFuture

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
    partitions = 3,
    topics = ["alarm-jobs"],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class KafkaTpsPerformanceTest {

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
    fun `verify high TPS with non-blocking asynchronous execution`() {
        // Given: API 호출이 의도적으로 200ms 지연 후 응답하는 리액티브 스트림 정의
        `when`(notificationPort.sendNotification(anyList())).thenAnswer {
            Mono.delay(Duration.ofMillis(200)).then(Mono.empty<Void>())
        }

        val consumer = KafkaAlarmJobConsumer(
            alarmRepositoryPort, notificationPort, alarmExecutionLogRepositoryPort,
            redisTemplate, redisRateLimiter, objectMapper
        )

        // 10개의 알람 데이터를 준비하고 RDB에 저장
        val futures = (1..10).map { i ->
            val alarmId = 95000L + i
            val alarm = Alarm.create(
                id = alarmId,
                senderId = "sender-tps",
                receiverId = "receiver-tps",
                channelType = "SMS",
                name = "TPS Test $i",
                payload = "{}"
            )
            alarmRepositoryPort.save(alarm)

            val payload = """{"alarmId":"$alarmId","senderId":"sender-tps","receiverId":"receiver-tps","uuid":"uuid-tps-$i"}"""
            
            CompletableFuture.supplyAsync {
                consumer.consumeAlarmJob(payload, 1).join()
            }
        }

        val startTime = System.currentTimeMillis()

        // When: 10건의 비동기 작업을 동시에 처리하고 병렬로 완료될 때까지 대기
        CompletableFuture.allOf(*futures.toTypedArray()).join()

        val elapsedTime = System.currentTimeMillis() - startTime
        println("Elapsed time for 10 concurrent requests: $elapsedTime ms")

        // Then: 10건 순차 처리 지연(2000ms) 대비 월등히 빠른 속도로 병렬 처리가 끝나야 함
        assertTrue(elapsedTime < 800, "병렬 비동기 처리는 순차 실행 지연 시간(2000ms)보다 훨씬 빠르게 완료되어야 합니다. (실제 소요: $elapsedTime ms)")
    }
}
