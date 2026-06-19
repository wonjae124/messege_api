package com.alarm.adapter.`in`.messaging

import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.NotificationPort
import com.alarm.application.port.out.AlarmBatchItem
import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.domain.AlarmStatus
import com.alarm.domain.AlarmExecutionLog
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito
import org.mockito.Mockito.*
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * [?€?? KafkaAlarmJobConsumer ??Kafka ë°°ì¹˜ ?Œë¹„??
 * [ë²”ì£¼] Consumer Unit Test ??Mockito Mock, ?¤ì œ Kafka ë¸Œë¡œì»??†ìŒ
 *
 * H2 DB ?íƒœ ê°±ì‹ 
 *   ???•ìƒ ì²˜ë¦¬ ??updateStatusesInBatch(SENT)
 *   ??ë°œì†¡ ?¤íŒ¨ ??updateStatusesInBatch(FAILED) + saveAll(?ëŸ¬ ?´ë ¥)
 *
 * Redis proc: ??ë©±ë“±??
 *   ???™ì¼ uuid 2ê±??¸ì… ??sendNotification ?•í™•??1?Œë§Œ ?¸ì¶œ (ì¤‘ë³µ Skip)
 *
 * Rate Limiter
 *   ??501~502ë²ˆì§¸ ì²?¬ tryAcquire=false ???€ê¸????¬ì‹œ?? ì´?504???¸ì¶œ
 */
class KafkaAlarmJobConsumerTest {

    private lateinit var alarmRepositoryPort: AlarmRepositoryPort
    private lateinit var notificationPort: NotificationPort
    private lateinit var alarmExecutionLogRepositoryPort: AlarmExecutionLogRepositoryPort
    private lateinit var redisTemplate: StringRedisTemplate
    private lateinit var redisRateLimiter: RedisRateLimiter
    private lateinit var objectMapper: ObjectMapper
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>
    private lateinit var valueOps: ValueOperations<String, String>
    private lateinit var ack: Acknowledgment
    private lateinit var consumer: KafkaAlarmJobConsumer

    @Suppress("UNCHECKED_CAST")
    @BeforeEach
    fun setUp() {
        alarmRepositoryPort = mock(AlarmRepositoryPort::class.java)
        notificationPort = mock(NotificationPort::class.java)
        alarmExecutionLogRepositoryPort = mock(AlarmExecutionLogRepositoryPort::class.java)
        redisTemplate = mock(StringRedisTemplate::class.java)
        redisRateLimiter = mock(RedisRateLimiter::class.java)

        // ?µì‹¬ ?˜ì •: KotlinModule???±ë¡?´ì•¼ Kotlin data class ??§?¬í™” ê°€??
        objectMapper = ObjectMapper().registerKotlinModule()

        kafkaTemplate = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>
        valueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>
        ack = mock(Acknowledgment::class.java)

        `when`(redisTemplate.opsForValue()).thenReturn(valueOps)
        `when`(redisRateLimiter.tryAcquire(anyInt())).thenReturn(true)

        // Mockito ?œì? anyList() ë§¤ì²˜??null???„ë‹Œ ë¹?ë¦¬ìŠ¤?¸ë? ë°˜í™˜?˜ì—¬ ?ˆì „??
        `when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.empty())

        val dummyAlarm = com.alarm.domain.Alarm.reconstruct(
            id = 12345L,
            senderId = "sender-1",
            receiverId = "receiver-1",
            channelType = "SMS",
            name = "Test Alarm",
            status = AlarmStatus.PENDING,
            payload = "Test Payload", location = null,
            createdAt = java.time.LocalDateTime.now(),
            updatedAt = java.time.LocalDateTime.now()
        )
        `when`(alarmRepositoryPort.findById(anyLong())).thenReturn(dummyAlarm)

        consumer = KafkaAlarmJobConsumer(
            alarmRepositoryPort,
            notificationPort,
            alarmExecutionLogRepositoryPort,
            redisTemplate,
            redisRateLimiter,
            objectMapper
        )
    }

    /**
     * [?œë‚˜ë¦¬ì˜¤] ì¹´í”„ì¹´ì—???˜ì‹ ???ŒëŒ??ì¤?ì¤‘ë³µ?˜ì? ?Šì? ?ŒëŒ??ìµœì¢…?ìœ¼ë¡?DB(H2)???„ë£Œ(SENT) ?íƒœë¡????€?¥ë˜?”ì? ê²€ì¦í•©?ˆë‹¤.
     * [Given] ì¤‘ë³µ?˜ì? ?Šì? ?ŒëŒ ë©”ì‹œì§€ ë°?Redis ? ì  ?µê³¼ ?¤ì •
     * [When] ?ŒëŒ ?´ë²¤??ë°°ì¹˜ ?Œë¹„ ?¸ì¶œ
     * [Then] alarmRepositoryPort.updateStatusesInBatchê°€ SENT ?íƒœë¡??¸ì¶œ?˜ì—ˆ?”ì? ê²€ì¦?
     */
    @Test
    fun `should update RDB H2 status to SENT on successful processing`() {
        // Given
        val payload = """{"alarmId":"12345","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-ok"}"""
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)

        // When
        consumer.consumeAlarmJob(payload, 1).join()

        // [Then] alarmRepositoryPort.saveê°€ SENT ?íƒœë¡??¸ì¶œ?˜ì—ˆ?”ì? ê²€ì¦?
        // 1. DCL 2ì°?ê²€ì¦ì„ ?„í•œ findById ?¸ì¶œ
        verify(alarmRepositoryPort).findById(12345L)
        verify(alarmRepositoryPort).save(anyObject(com.alarm.domain.Alarm::class.java))
    }

    /**
     * [?œë‚˜ë¦¬ì˜¤] ì¤‘ë³µ ?´ë²¤?¸ê? ?¸ì…?˜ì—ˆ????ì¤‘ë³µ??ê°ì??˜ê³  ë¬´ì‹œ(Skip)?˜ëŠ” ë¡œì§??ê²€ì¦í•©?ˆë‹¤.
     * [Given] ?™ì¼ ë©±ë“±??proc:)ë¥?ê°€ì§?2ê°œì˜ ë©”ì‹œì§€,
     *        ì²?ë²ˆì§¸ ë©”ì‹œì§€??setIfAbsent=true(ìµœì´ˆ ? ì  ?±ê³µ),
     *        ??ë²ˆì§¸ ë©”ì‹œì§€??setIfAbsent=false + Redis ê°?"PROCESSING" ë°˜í™˜(ì¤‘ë³µ ê°ì?)?¼ë¡œ ?¤ì •
     * [When] ?ŒëŒ ?´ë²¤??ë°°ì¹˜ ?Œë¹„ ?¸ì¶œ
     * [Then] ?¸ë? ë°œì†¡ API(sendNotification)ê°€ ?•í™•??1?Œë§Œ ?¸ì¶œ?˜ëŠ”ì§€ ê²€ì¦?(ì¤‘ë³µ ë©”ì‹œì§€ ?œì™¸??
     */
    @Test
    fun `should detect and ignore duplicate Kafka jobs using Redis proc key`() {
        // Given
        val payload1 = """{"alarmId":"12345","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-dup"}"""
        val payload2 = """{"alarmId":"12345","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-dup"}"""
        // ??ë©”ì‹œì§€ê°€ ?™ì¼??proc ?¤ë? ê³µìœ ?? proc:sender-1:uuid-dup
        val procKey = "proc:sender-1:uuid-dup"

        // ì²?ë²ˆì§¸ ? ì  ?œë„: true (?±ê³µ)
        // ??ë²ˆì§¸ ? ì  ?œë„: false (?´ë? ? ì ??= ì¤‘ë³µ)
        `when`(valueOps.setIfAbsent(eq(procKey), eq("PROCESSING"), any(Duration::class.java)))
            .thenReturn(true)
            .thenReturn(false)
        // ??ë²ˆì§¸ ë©”ì‹œì§€ ?„ë‹¬ ??Redis?ì„œ ?„ì¬ ?íƒœ ì¡°íšŒ ??PROCESSING ë°˜í™˜ ??ì¤‘ë³µ?¼ë¡œ ?ì •
        `when`(valueOps.get(eq(procKey))).thenReturn("PROCESSING")

        // When
        consumer.consumeAlarmJob(payload1, 1).join()
        
        // ??ë²ˆì§¸ ì¤‘ë³µ ?‘ì—…?€ ConcurrentProcessingException???˜ì ¸??Kafka ?¬ì‹œ?„ë? ?€????
        org.junit.jupiter.api.Assertions.assertThrows(Exception::class.java) {
            consumer.consumeAlarmJob(payload2, 1).join()
        }

        // Then
        // ArgumentCaptor ìºìŠ¤???†ì´ ?¸ì¶œ ?Ÿìˆ˜(1??ë§?verify ??Kotlin null-safety ë¬¸ì œ ?†ìŒ
        verify(notificationPort, times(1)).sendNotification(anyList())
    }

    /**
     * [?œë‚˜ë¦¬ì˜¤] ë°œì†¡ ?¤ë¥˜ ??DB(H2)???¤íŒ¨(FAILED) ?íƒœ ?…ë°?´íŠ¸?€ ?ëŸ¬ ?´ë ¥ ?€?¥ì„ ê²€ì¦í•©?ˆë‹¤.
     * [Given] ë©”ì‹œì§€ ë°??¸ë? ?„ì†¡ APIê°€ RuntimeException??ë°˜í™˜?˜ë„ë¡??¤ì •
     * [When] ?ŒëŒ ?´ë²¤??ë°°ì¹˜ ?Œë¹„ ?¸ì¶œ
     * [Then] DB H2??FAILED ?…ë°?´íŠ¸ ?”ì²­ ë°??ëŸ¬ ë¡œê·¸ ?¼ê´„(saveAll) ?€?¥ì´ ?¼ì–´?˜ëŠ”ì§€ ê²€ì¦?
     */
    @Test
    fun `should update RDB H2 status to DEAD and record log on max execution failure`() {
        // Given
        val payload = """{"alarmId":"12345","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-fail"}"""
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        `when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.error(RuntimeException("Connection lost")))

        // When
        consumer.consumeAlarmJob(payload, 4).join()

        // Then
        verify(alarmRepositoryPort, times(2)).findById(12345L)
        verify(alarmRepositoryPort).save(anyObject(com.alarm.domain.Alarm::class.java))

        // 3. alarmExecutionLogRepositoryPort.save ê°€ DEAD ?íƒœë¡??€?¥ë˜?ˆëŠ”ì§€ ê²€ì¦?
        verify(alarmExecutionLogRepositoryPort).save(anyObject(AlarmExecutionLog::class.java))
    }

    /**
     * [?œë‚˜ë¦¬ì˜¤] ?¸ë? ?°ë™ Rate Limit ì¡°ê±´(ì´ˆë‹¹ ìµœë? 500???¸ì¶œ)??ì´ˆê³¼??502ë²ˆì§¸ ?”ì²­ ?í™©??ê²€ì¦í•©?ˆë‹¤.
     * [Given] 502ê°œì˜ ë°°ì¹˜ chunkë¥??ì„±?˜ê¸° ?„í•´ ì´?502 * 200 = 100,400ê°œì˜ ?ŒëŒ ?˜ì´ë¡œë“œ ?ì„±
     *        Redis Rate Limiter(tryAcquire)ê°€ 500?Œì§¸ê¹Œì???true(?ˆìš©),
     *        501ë²ˆì§¸?€ 502ë²ˆì§¸ ?œë„??false(ì´ˆê³¼ ì°¨ë‹¨),
     *        503ë²ˆì§¸ ?œë„ë¶€???¤ì‹œ true(?ˆìš©)ë¥?ë°˜í™˜?˜ë„ë¡?Mock ?¤ì •
     * [When] 100,400ê°œì˜ ?€?©ëŸ‰ ?ŒëŒ ?´ë²¤??ë°°ì¹˜ ?Œë¹„ ?¸ì¶œ
     * [Then] tryAcquire ?¸ì¶œ ?Ÿìˆ˜ê°€ ì´?504???±ê³µ 502??+ ?€ê¸°ë¡œ ?¸í•œ ?¬ì‹œ??2???¸ì? ê²€ì¦í•˜??
     *        ì´ˆê³¼ ???€ê¸?ë°??¬ì„ ?ì´ ?•ìƒ ?™ì‘?˜ëŠ”ì§€ ì¦ëª…
     */
    @Test
    fun `should block and wait when rate limit is exceeded`() {
        // Given
        // 3ê°œì˜ ?ŒëŒ ë©”ì‹œì§€ ?ì„±
        val payloads = List(3) { index ->
            """{"alarmId":"${12340 + index}","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-$index"}"""
        }
        `when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)

        // tryAcquireê°€ 1ë²ˆì§¸ ?±ê³µ, 2ë²ˆì§¸ ?¤íŒ¨(ì°¨ë‹¨), 3ë²ˆì§¸(?¬ì‹œ?? ?±ê³µ, ê·??´í›„ ?±ê³µ?˜ë„ë¡??¤ì •
        var rateLimitCallCount = 0
        `when`(redisRateLimiter.tryAcquire(1)).thenAnswer {
            rateLimitCallCount++
            when (rateLimitCallCount) {
                1 -> true   // 1ë²ˆì§¸ ?ŒëŒ: ì¦‰ì‹œ ?ˆìš©
                2 -> false  // 2ë²ˆì§¸ ?ŒëŒ (ì²??œë„): ì°¨ë‹¨ (?€ê¸?
                3 -> true   // 2ë²ˆì§¸ ?ŒëŒ (?¬ì‹œ??: ?€ê¸????ˆìš©
                else -> true // 3ë²ˆì§¸ ?ŒëŒ ?´í›„: ì¦‰ì‹œ ?ˆìš©
            }
        }

        // When
        payloads.forEach { consumer.consumeAlarmJob(it, 1).join() }

        // Then
        // ì´??¸ì¶œ ?Ÿìˆ˜: 
        // 1ë²ˆì§¸ ?ŒëŒ ì²˜ë¦¬ ??1??
        // 2ë²ˆì§¸ ?ŒëŒ ì²˜ë¦¬ ??1???¤íŒ¨) + 1???±ê³µ) = 2??
        // 3ë²ˆì§¸ ?ŒëŒ ì²˜ë¦¬ ??1??
        // ì´?4???¸ì¶œ?˜ì–´????
        verify(redisRateLimiter, times(4)).tryAcquire(1)
    }
}

@Suppress("UNCHECKED_CAST")
private fun <T> anyObject(type: Class<T>): T {
    Mockito.any(type)
    return when (type) {
        AlarmExecutionLog::class.java -> AlarmExecutionLog.reconstruct(
            id = java.util.UUID.randomUUID(),
            alarmId = "dummy",
            triggeredAt = java.time.LocalDateTime.now(),
            status = "SUCCESS",
            errorMessage = null
        ) as T
        else -> null as T
    }
}
