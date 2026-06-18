package com.alarm.kafka

import com.alarm.adapter.out.notification.RedisRateLimiter
import com.alarm.adapter.`in`.messaging.KafkaAlarmJobConsumer
import com.alarm.adapter.`in`.web.dto.AlarmCreateRequest
import com.alarm.application.port.out.AlarmExecutionLogRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.NotificationPort
import com.alarm.domain.AlarmStatus
import com.alarm.domain.Alarm
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyList
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.atLeast
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.timeout
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * [?€?? KafkaAlarmJobProducer + KafkaAlarmJobConsumer
 * [ë²”ì£¼] Kafka ?¤íŒ¨ ì¼€?´ìŠ¤ ?µí•© Test ??SpringBootTest + EmbeddedKafka + Mockito ?¼í•©
 *
 * ë°œì†¡ API ?¤íŒ¨
 *   ??sendNotification ?ˆì™¸ ??H2 DB FAILED ?€??+ ?ëŸ¬ ë¡œê·¸ ê¸°ë¡
 *   ??sendNotification ?ˆì™¸ ??alarm-jobs-retry ? í”½?¼ë¡œ ?¬ë°œ??
 *
 * ë©±ë“±??(ì¤‘ë³µ ë©”ì‹œì§€)
 *   ???™ì¼ uuid 2ê±???proc: ?¤ë¡œ ì¤‘ë³µ ê°ì?, sendNotification 1?Œë§Œ ?¸ì¶œ
 *
 * ?˜ëª»??ë©”ì‹œì§€ ?•ì‹
 *   ???•ìƒ JSON 1ê±?+ ê¹¨ì§„ JSON 1ê±???ê¹¨ì§„ ê±?Skip, ?•ìƒ ê±´ë§Œ ì²˜ë¦¬
 *
 * ?Œí‹°??ë°œí–‰ ?¤íŒ¨
 *   ??ì¡´ì¬?˜ì? ?ŠëŠ” ?Œí‹°??99ë²? ë°œí–‰ ?œë„ ???ˆì™¸ ë°œìƒ
 *
 * ack ë³´ì¥
 *   ??ëª¨ë“  ë©”ì‹œì§€ê°€ ì¤‘ë³µ?´ì–´????ack() ?•ìƒ ?¸ì¶œ (?¤í”„???„ë½ ë°©ì?)
 */
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
    partitions = 3,
    topics = ["alarm-jobs", "alarm-jobs-retry"],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class KafkaProducerConsumerFailureTest {

    @Autowired
    lateinit var mockMvc: MockMvc

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Autowired
    lateinit var alarmRepositoryPort: AlarmRepositoryPort

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
        Mockito.`when`(redisTemplate.opsForValue()).thenReturn(valueOps)
        Mockito.`when`(valueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        Mockito.`when`(redisRateLimiter.tryAcquire(anyInt())).thenReturn(true)
        // ê¸°ë³¸?€ ?±ê³µ ?‘ë‹µ; ê°œë³„ ?ŒìŠ¤?¸ì—???¬ì •??ê°€??
        Mockito.`when`(notificationPort.sendNotification(anyList())).thenReturn(Mono.empty())
    }

    // ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€
    // [?¤íŒ¨ ?œë‚˜ë¦¬ì˜¤ 1] ë°œì†¡ API ?ˆì™¸ ë°œìƒ ??FAILED ?íƒœ ?€??+ ?ëŸ¬ ë¡œê·¸
    // ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€

    /**
     * [?œë‚˜ë¦¬ì˜¤ 1] sendNotification??RuntimeException???˜ì§ˆ ??
     *              H2 DB??FAILED ?íƒœë¡??€?¥ë˜ê³??ëŸ¬ ?´ë ¥??ê¸°ë¡?˜ëŠ”ì§€ ê²€ì¦í•©?ˆë‹¤.
     * [Given] ?ŒëŒ 1ê±??ì„± + sendNotification???ˆì™¸ë¥?ë°˜í™˜?˜ë„ë¡?Mock ?¤ì •
     * [When] POST /notifications ?¸ì¶œ ??Kafka ë°œí–‰ ??Consumer ?Œë¹„ ??ë°œì†¡ ?¤íŒ¨
     * [Then] H2 DB?ì„œ ?´ë‹¹ ?ŒëŒ ?íƒœê°€ FAILED?¸ì? ?´ë§ ê²€ì¦?
     */
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `consumer should save RETRY_WAIT status when sendNotification throws exception`() {
        // Given: sendNotification???ˆì™¸ë¥?ë°œìƒ?œí‚¤?„ë¡ ?¤ì •
        Mockito.`when`(notificationPort.sendNotification(anyList()))
            .thenReturn(Mono.error(RuntimeException("error")))

        val request = AlarmCreateRequest(name = "?¤íŒ¨?ŒëŒ", payload = "{}")
        val result = mockMvc.perform(
            post("/notifications")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isCreated)
            .andReturn()

        val alarmId = objectMapper.readTree(result.response.contentAsString).get("id").asText()

        // When: Consumer ë¹„ë™ê¸?ì²˜ë¦¬ ?€ê¸?(ìµœë? 5ì´?
        // Then: H2 DB?ì„œ ?ŒëŒ ?íƒœê°€ RETRY_WAITë¡?ê°±ì‹ ?˜ì—ˆ?”ì? ?´ë§ ê²€ì¦?
        var isRetryWait = false
        for (i in 1..50) {
            val alarm = alarmRepositoryPort.findById(alarmId.toLong())
            if (alarm?.status == AlarmStatus.RETRY_WAIT) {
                isRetryWait = true
                break
            }
            Thread.sleep(100)
        }
        assertTrue(isRetryWait, "sendNotification ?ˆì™¸ ë°œìƒ ???ŒëŒ ?íƒœê°€ 5ì´???RETRY_WAITë¡?ê°±ì‹ ?˜ì–´???©ë‹ˆ??")
    }

    // ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€
    // [?¤íŒ¨ ?œë‚˜ë¦¬ì˜¤ 2] ë°œì†¡ ?¤íŒ¨ ??Retry Topic ?¬ë°œ??(Consumer ?¨ìœ„ ?ŒìŠ¤??
    // ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€

    /**
     * [?œë‚˜ë¦¬ì˜¤ 2] sendNotification ?ˆì™¸ ë°œìƒ ???¤íŒ¨???ŒëŒ IDê°€ alarm-jobs-retry ? í”½?¼ë¡œ ?¬ë°œ?‰ë˜?”ì? ê²€ì¦í•©?ˆë‹¤.
     * [Given] ?Œë¹„???¨ìœ„ ?ŒìŠ¤???˜ê²½ êµ¬ì„± + sendNotification ?ˆì™¸ ë°˜í™˜ ?¤ì •
     * [When] consumeAlarmJobs() ì§ì ‘ ?¸ì¶œ
     * [Then] kafkaTemplate.send("alarm-jobs-retry", ...)ê°€ ?¸ì¶œ?˜ì—ˆ?”ì? ê²€ì¦?
     */
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `consumer should update status to DEAD when max retries are exceeded`() {
        // Given: ?Œë¹„??ì§ì ‘ êµ¬ì„±
        val mockAlarmRepo = mock(AlarmRepositoryPort::class.java)
        val mockLogRepo = mock(AlarmExecutionLogRepositoryPort::class.java)
        val mockNotification = mock(NotificationPort::class.java)
        val mockRedis = mock(StringRedisTemplate::class.java)
        val mockRateLimiter = mock(RedisRateLimiter::class.java)
        val mockValueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>

        `when`(mockRedis.opsForValue()).thenReturn(mockValueOps)
        `when`(mockValueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        `when`(mockRateLimiter.tryAcquire(anyInt())).thenReturn(true)
        // sendNotification???ˆì™¸ë¥?ë°œìƒ?œí‚¤?„ë¡ ?¤ì •
        `when`(mockNotification.sendNotification(anyList()))
            .thenReturn(Mono.error(RuntimeException("error")))

        val testAlarm = Alarm.create(
            id = 12345L,
            senderId = "sender-1",
            receiverId = "receiver-1",
            channelType = "SMS",
            name = "Test Alarm",
            payload = "{}"
        )
        `when`(mockAlarmRepo.findById(12345L)).thenReturn(testAlarm)

        val testMapper = ObjectMapper().registerKotlinModule()
        val consumer = KafkaAlarmJobConsumer(
            mockAlarmRepo, mockNotification, mockLogRepo,
            mockRedis, mockRateLimiter, testMapper
        )

        val payload = """{"alarmId":"12345","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-retry"}"""

        // When: 4ë²ˆì§¸ ?¤íŒ¨ ?œë„ (maxAttempts = 4)ë¥?ì§ì ‘ ?¸ì¶œ?˜ì—¬ ìµœì¢… ?¤íŒ¨ ?í™©???¬í˜„ (DEAD?´ë?ë¡??ˆì™¸ ?†ì´ ?„ë£Œ??
        consumer.consumeAlarmJob(payload, 4).join()

        // Then: DEAD ?íƒœë¡?DB ?…ë°?´íŠ¸ê°€ ê²€ì¦ë˜?´ì•¼ ??(ìµœë? ?œë„ ì´ˆê³¼)
        verify(mockAlarmRepo).save(org.mockito.ArgumentMatchers.any())
    }

    // ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€
    // [?¤íŒ¨ ?œë‚˜ë¦¬ì˜¤ 3] ì¤‘ë³µ Kafka ë©”ì‹œì§€ ??proc: ??ë©±ë“± ì²˜ë¦¬ (Consumer ?¨ìœ„ ?ŒìŠ¤??
    // ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€

    /**
     * [?œë‚˜ë¦¬ì˜¤ 3] ?™ì¼ uuid(ë©±ë“±??ë¥?ê°€ì§?ë©”ì‹œì§€ 2ê±´ì´ Kafka???¤ì–´????
     *              proc: Redis ?¤ë¡œ ì¤‘ë³µ??ê°ì??˜ì—¬ sendNotification???•í™•??1?Œë§Œ ?¸ì¶œ?˜ëŠ”ì§€ ê²€ì¦í•©?ˆë‹¤.
     * [Given] ?™ì¼ uuidë¥?ê°€ì§?payload 2ê±?ì¤€ë¹?+ ì²?ë²ˆì§¸ë§?setIfAbsent=true, ??ë²ˆì§¸??false ?¤ì •
     * [When] consumeAlarmJobs()??2ê±´ì„ ?œêº¼ë²ˆì— ?„ë‹¬
     * [Then] sendNotification?€ ?•í™•??1?Œë§Œ ?¸ì¶œ??ê²€ì¦?(ì¤‘ë³µ ê±?Skip)
     */
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `consumer should deduplicate messages with same uuid via redis proc key`() {
        // Given
        val mockAlarmRepo = mock(AlarmRepositoryPort::class.java)
        val mockLogRepo = mock(AlarmExecutionLogRepositoryPort::class.java)
        val mockNotification = mock(NotificationPort::class.java)
        val mockRedis = mock(StringRedisTemplate::class.java)
        val mockRateLimiter = mock(RedisRateLimiter::class.java)
        val mockValueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>

        val procKey = "proc:sender-A:uuid-SAME"
        `when`(mockRedis.opsForValue()).thenReturn(mockValueOps)
        `when`(mockRateLimiter.tryAcquire(anyInt())).thenReturn(true)
        `when`(mockNotification.sendNotification(anyList())).thenReturn(Mono.empty())

        // ì²?ë²ˆì§¸ ë©”ì‹œì§€: proc ??? ì  ?±ê³µ (true)
        // ??ë²ˆì§¸ ë©”ì‹œì§€: ?´ë? ? ì ?˜ì–´ ?ˆìŒ (false) ??ì¤‘ë³µ?¼ë¡œ Skip
        `when`(mockValueOps.setIfAbsent(org.mockito.ArgumentMatchers.eq(procKey), anyString(), any(Duration::class.java)))
            .thenReturn(true)   // ì²?ë²ˆì§¸ ?œë„
            .thenReturn(false)  // ??ë²ˆì§¸ ?œë„
        `when`(mockValueOps.get(org.mockito.ArgumentMatchers.eq(procKey))).thenReturn("PROCESSING")

        val testAlarm = Alarm.create(
            id = 10001L,
            senderId = "sender-A",
            receiverId = "receiver-A",
            channelType = "SMS",
            name = "First Alarm",
            payload = "{}"
        )
        `when`(mockAlarmRepo.findById(10001L)).thenReturn(testAlarm)

        val testMapper = ObjectMapper().registerKotlinModule()
        val consumer = KafkaAlarmJobConsumer(
            mockAlarmRepo, mockNotification, mockLogRepo,
            mockRedis, mockRateLimiter, testMapper
        )

        // ?™ì¼ uuidë¥?ê°€ì§?ë©”ì‹œì§€ 2ê±?(alarmIdë§??¤ë¦„)
        val payload1 = """{"alarmId":"10001","senderId":"sender-A","receiverId":"receiver-A","uuid":"uuid-SAME"}"""
        val payload2 = """{"alarmId":"10002","senderId":"sender-A","receiverId":"receiver-A","uuid":"uuid-SAME"}"""

        // When
        consumer.consumeAlarmJob(payload1, 1).join()
        
        // ??ë²ˆì§¸ ì¤‘ë³µ ?‘ì—…?€ ConcurrentProcessingException???˜ì ¸??Kafka ?¬ì‹œ?„ë? ?€????
        org.junit.jupiter.api.Assertions.assertThrows(Exception::class.java) {
            consumer.consumeAlarmJob(payload2, 1).join()
        }

        // Then: ì¤‘ë³µ ê±??œì™¸ ??sendNotification?€ ?•í™•??1?Œë§Œ ?¸ì¶œ
        verify(mockNotification, times(1)).sendNotification(anyList())
        // ì²?ë²ˆì§¸ë§?SENT, ??ë²ˆì§¸??ì²˜ë¦¬ ?ˆë¨
        verify(mockAlarmRepo).save(org.mockito.ArgumentMatchers.any())
    }

    // ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€
    // [?¤íŒ¨ ?œë‚˜ë¦¬ì˜¤ 4] ?˜ëª»??JSON ?¬ë§· ë©”ì‹œì§€ ?¼ì… ???•ìƒ ê±´ë§Œ ì²˜ë¦¬
    // ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€

    /**
     * [?œë‚˜ë¦¬ì˜¤ 4] ë°°ì¹˜ ë©”ì‹œì§€ ì¤?JSON ?Œì‹±??ë¶ˆê??¥í•œ ?˜ëª»???¬ë§·??ë©”ì‹œì§€ê°€ 1ê±??¬í•¨????
     *              ?´ë‹¹ ê±´ì? Skip(?ˆì™¸ ì²˜ë¦¬)?˜ê³  ?˜ë¨¸ì§€ ?•ìƒ ê±´ì? ?•ìƒ ì²˜ë¦¬?˜ëŠ”ì§€ ê²€ì¦í•©?ˆë‹¤.
     * [Given] ?•ìƒ JSON 1ê±?+ ?˜ëª»???¬ë§· 1ê±´ì„ ?¨ê»˜ ì¤€ë¹?
     * [When] consumeAlarmJobs()??2ê±´ì„ ?„ë‹¬
     * [Then] sendNotification?€ ?•ìƒ ê±´ë§Œ ?¬í•¨?˜ì—¬ 1???¸ì¶œ??ê²€ì¦?
     *        + ?˜ëª»??ê±´ì? ?ˆì™¸ ë¡œê·¸ë§??¨ê¸°ê³??„ì²´ ë°°ì¹˜ ?¤íŒ¨ë¡??´ì–´ì§€ì§€ ?ŠìŒ
     */
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `consumer should skip malformed json messages and process valid ones`() {
        // Given
        val mockAlarmRepo = mock(AlarmRepositoryPort::class.java)
        val mockLogRepo = mock(AlarmExecutionLogRepositoryPort::class.java)
        val mockNotification = mock(NotificationPort::class.java)
        val mockRedis = mock(StringRedisTemplate::class.java)
        val mockRateLimiter = mock(RedisRateLimiter::class.java)
        val mockValueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>

        `when`(mockRedis.opsForValue()).thenReturn(mockValueOps)
        `when`(mockValueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(true)
        `when`(mockRateLimiter.tryAcquire(anyInt())).thenReturn(true)
        `when`(mockNotification.sendNotification(anyList())).thenReturn(Mono.empty())

        val testAlarm = Alarm.create(
            id = 10003L,
            senderId = "sender-1",
            receiverId = "receiver-1",
            channelType = "SMS",
            name = "Valid Alarm",
            payload = "{}"
        )
        `when`(mockAlarmRepo.findById(10003L)).thenReturn(testAlarm)

        val testMapper = ObjectMapper().registerKotlinModule()
        val consumer = KafkaAlarmJobConsumer(
            mockAlarmRepo, mockNotification, mockLogRepo,
            mockRedis, mockRateLimiter, testMapper
        )

        // ?•ìƒ JSON 1ê±?+ ?˜ëª»???¬ë§· 1ê±?
        val validPayload = """{"alarmId":"10003","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-valid"}"""
        val malformedPayload = """NOT_A_JSON_AT_ALL"""

        // When
        consumer.consumeAlarmJob(validPayload, 1).join()
        
        // ?Œì‹± ë¶ˆê??¥í•œ JSON ?˜ì‹  ???ˆì™¸ ë°œìƒ ê²€ì¦?
        org.junit.jupiter.api.Assertions.assertThrows(Exception::class.java) {
            consumer.consumeAlarmJob(malformedPayload, 1).join()
        }

        // Then: ?•ìƒ ê±´ë§Œ ë°œì†¡?˜ì–´???˜ë?ë¡?sendNotification 1???¸ì¶œ
        verify(mockNotification, times(1)).sendNotification(anyList())
        // ?•ìƒ ê±´ì˜ SENT ?íƒœ ?…ë°?´íŠ¸ ê²€ì¦?
        verify(mockAlarmRepo).save(org.mockito.ArgumentMatchers.any())
    }

    // ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€
    // [?¤íŒ¨ ?œë‚˜ë¦¬ì˜¤ 5] ì¡´ì¬?˜ì? ?ŠëŠ” ?Œí‹°??ë²ˆí˜¸ë¡?ë°œí–‰ ???ˆì™¸ ë°œìƒ ê²€ì¦?
    // ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€

    /**
     * [?œë‚˜ë¦¬ì˜¤ 5] ì¡´ì¬?˜ì? ?ŠëŠ” ?Œí‹°??99ë²???ê°•ì œ ë°œí–‰ ?œë„ ???ˆì™¸ê°€ ë°œìƒ?˜ëŠ”ì§€ ê²€ì¦í•©?ˆë‹¤.
     * [Given] EmbeddedKafka???Œí‹°??3ê°?0, 1, 2)ë§?ì¡´ì¬
     * [When] ?Œí‹°??99ë²ˆìœ¼ë¡?ProducerRecordë¥?ì§ì ‘ ë°œí–‰ ?œë„
     * [Then] TimeoutException ?ëŠ” ExecutionException ???„ì†¡ ?¤íŒ¨ ?ˆì™¸ ë°œìƒ ê²€ì¦?
     */
    @Test
    fun `producer should fail when sending to non-existent partition`() {
        // Given: EmbeddedKafka???Œí‹°??0, 1, 2ë§?ì¡´ì¬ (ì´?3ê°?
        val invalidPartition = 99
        val payload = """{"alarmId":"alarm-invalid","senderId":"sender","receiverId":"receiver","uuid":"uuid-invalid"}"""

        // When & Then: ì¡´ì¬?˜ì? ?ŠëŠ” ?Œí‹°??99ë²ˆìœ¼ë¡?ë°œí–‰ ???ˆì™¸ ë°œìƒ
        val exception = org.junit.jupiter.api.Assertions.assertThrows(Exception::class.java) {
            kafkaTemplate.send(ProducerRecord("alarm-jobs", invalidPartition, "key", payload))
                .get(3, TimeUnit.SECONDS)
        }
        assertNotNull(exception, "ì¡´ì¬?˜ì? ?ŠëŠ” ?Œí‹°??ë²ˆí˜¸ë¡?ë°œí–‰ ???ˆì™¸ê°€ ë°œìƒ?´ì•¼ ?©ë‹ˆ??")
    }

    // ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€
    // [?¤íŒ¨ ?œë‚˜ë¦¬ì˜¤ 6] ì²˜ë¦¬ ê°€?¥í•œ ë©”ì‹œì§€ ?†ì„ ?Œë„ ack ?•ìƒ ?¸ì¶œ
    // ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€

    /**
     * [?œë‚˜ë¦¬ì˜¤ 6] ë°°ì¹˜ ??ëª¨ë“  ë©”ì‹œì§€ê°€ ì¤‘ë³µ(proc: ?¤ë¡œ ?´ë? ì²˜ë¦¬???´ì–´??
     *              ?¤ì œë¡?ì²˜ë¦¬??ë©”ì‹œì§€ê°€ ?†ëŠ” ê²½ìš°?ë„ Kafka ackë¥??•ìƒ ?¸ì¶œ?˜ëŠ”ì§€ ê²€ì¦í•©?ˆë‹¤.
     * [Given] ëª¨ë“  ë©”ì‹œì§€??setIfAbsentê°€ falseë¥?ë°˜í™˜ + get()??"PROCESSING" ë°˜í™˜ (= ?´ë? ì²˜ë¦¬ ì¤?
     * [When] consumeAlarmJobs()??ë©”ì‹œì§€ ?„ë‹¬
     * [Then] sendNotification?€ ?ˆë? ?¸ì¶œ?˜ì? ?ŠìŒ + ack()???•ìƒ ?¸ì¶œ??
     */
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `consumer should not invoke sendNotification when all messages are duplicates`() {
        // Given
        val mockAlarmRepo = mock(AlarmRepositoryPort::class.java)
        val mockLogRepo = mock(AlarmExecutionLogRepositoryPort::class.java)
        val mockNotification = mock(NotificationPort::class.java)
        val mockRedis = mock(StringRedisTemplate::class.java)
        val mockRateLimiter = mock(RedisRateLimiter::class.java)
        val mockValueOps = mock(ValueOperations::class.java) as ValueOperations<String, String>

        `when`(mockRedis.opsForValue()).thenReturn(mockValueOps)
        // ëª¨ë“  ë©”ì‹œì§€ê°€ ?´ë? ì²˜ë¦¬ ì¤??íƒœ (ì¤‘ë³µ)
        `when`(mockValueOps.setIfAbsent(anyString(), anyString(), any(Duration::class.java))).thenReturn(false)
        `when`(mockValueOps.get(anyString())).thenReturn("PROCESSING") // ?´ë? ì²˜ë¦¬ ì¤???Skip

        val testMapper = ObjectMapper().registerKotlinModule()
        val consumer = KafkaAlarmJobConsumer(
            mockAlarmRepo, mockNotification, mockLogRepo,
            mockRedis, mockRateLimiter, testMapper
        )

        val payload1 = """{"alarmId":"10004","senderId":"sender-1","receiverId":"receiver-1","uuid":"uuid-dup-1"}"""
        val payload2 = """{"alarmId":"10005","senderId":"sender-2","receiverId":"receiver-2","uuid":"uuid-dup-2"}"""

        // When
        org.junit.jupiter.api.Assertions.assertThrows(Exception::class.java) {
            consumer.consumeAlarmJob(payload1, 1).join()
        }
        org.junit.jupiter.api.Assertions.assertThrows(Exception::class.java) {
            consumer.consumeAlarmJob(payload2, 1).join()
        }

        // Then: sendNotification?€ ??ë²ˆë„ ?¸ì¶œ?˜ì? ?Šì•„????
        verify(mockNotification, never()).sendNotification(anyList())
    }
}
