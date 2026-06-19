package com.alarm.adapter.out.messaging

import com.alarm.application.port.out.AlarmOutboxEventRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.MessageQueuePort
import com.alarm.domain.Alarm
import com.alarm.domain.AlarmOutboxEvent
import com.alarm.domain.AlarmStatus
import com.alarm.domain.OutboxStatus
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.transaction.support.TransactionCallback
import org.springframework.transaction.support.TransactionTemplate
import java.time.LocalDateTime

@ExtendWith(MockitoExtension::class)
class OutboxEventPublisherTest {

    @Mock
    private lateinit var alarmOutboxEventRepositoryPort: AlarmOutboxEventRepositoryPort

    @Mock
    private lateinit var alarmRepositoryPort: AlarmRepositoryPort

    @Mock
    private lateinit var messageQueuePort: MessageQueuePort

    @Mock
    private lateinit var transactionTemplate: TransactionTemplate

    private lateinit var outboxEventPublisher: OutboxEventPublisher

    private val dummyAlarm = Alarm.create(
        id = 123L,
        senderId = "s",
        receiverId = "r",
        channelType = "SMS",
        name = "test",
        payload = "{}"
    )

    @BeforeEach
    fun setUp() {
        outboxEventPublisher = OutboxEventPublisher(
            alarmOutboxEventRepositoryPort,
            alarmRepositoryPort,
            messageQueuePort,
            transactionTemplate,
            100
        )

        // Mock transactionTemplate.execute to just run the callback
        `when`(transactionTemplate.execute(any<TransactionCallback<List<AlarmOutboxEvent>>>())).thenAnswer {
            val callback = it.getArgument<TransactionCallback<List<AlarmOutboxEvent>>>(0)
            callback.doInTransaction(mock())
        }

        // Mock transactionTemplate.executeWithoutResult to just run the callback
        lenient().doAnswer {
            val callback = it.getArgument<java.util.function.Consumer<org.springframework.transaction.TransactionStatus>>(0)
            callback.accept(mock())
        }.`when`(transactionTemplate).executeWithoutResult(any())
    }

    @Test
    @DisplayName("Outbox 정상 발행 시 상태가 PUBLISHED로 변경된다")
    fun `publishPendingEvents - success`() {
        // Given
        val event = AlarmOutboxEvent.create(dummyAlarm)
        `when`(alarmOutboxEventRepositoryPort.findPendingEventsForUpdate(any(), eq(100)))
            .thenReturn(listOf(event))

        // When
        outboxEventPublisher.publishPendingEvents()

        // Then
        verify(messageQueuePort).publishAlarmJobFromOutbox(event)
        
        // processing mark and then published mark
        verify(alarmOutboxEventRepositoryPort, atLeastOnce()).save(event)
        assert(event.status == OutboxStatus.PUBLISHED)
        assert(event.publishedAt != null)
    }

    @Test
    @DisplayName("Outbox 발행 일시 실패 시 지수 백오프 기반 재시도 예약을 수행한다")
    fun `publishPendingEvents - temporary failure with backoff`() {
        // Given
        val event = AlarmOutboxEvent.create(dummyAlarm)
        `when`(alarmOutboxEventRepositoryPort.findPendingEventsForUpdate(any(), eq(100)))
            .thenReturn(listOf(event))
        
        // Force an exception during publish
        `when`(messageQueuePort.publishAlarmJobFromOutbox(event)).thenThrow(RuntimeException("Network Error"))

        // When
        outboxEventPublisher.publishPendingEvents()

        // Then
        verify(alarmOutboxEventRepositoryPort, atLeastOnce()).save(event)
        assert(event.status == OutboxStatus.PROCESSING) // Retains processing status for retry
        assert(event.retryCount == 1)
        assert(event.nextRetryAt != null)
        assert(event.nextRetryAt!!.isAfter(LocalDateTime.now())) // 지수 백오프로 다음 시간 지정됨
        
        // Alarm is not failed yet
        verify(alarmRepositoryPort, never()).findById(anyLong())
    }

    @Test
    @DisplayName("Outbox 발행 3회 초과 실패 시 최종 FAILED로 동기화된다")
    fun `publishPendingEvents - final failure syncs original alarm status`() {
        // Given
        val event = AlarmOutboxEvent.create(dummyAlarm)
        // Set retry count to 3, so next failure makes it 4 (exceeds max 3)
        for (i in 1..3) event.recordFailure()
        
        `when`(alarmOutboxEventRepositoryPort.findPendingEventsForUpdate(any(), eq(100)))
            .thenReturn(listOf(event))
        
        // Mock alarm lookup for sync
        val alarmSpy = spy(dummyAlarm)
        `when`(alarmRepositoryPort.findById(dummyAlarm.id)).thenReturn(alarmSpy)

        // Force an exception during publish
        `when`(messageQueuePort.publishAlarmJobFromOutbox(event)).thenThrow(RuntimeException("Network Error"))

        // When
        outboxEventPublisher.publishPendingEvents()

        // Then
        assert(event.status == OutboxStatus.PUBLISH_FAILED)
        assert(event.retryCount == 4)
        
        verify(alarmSpy).fail("PUBLISHER")
        verify(alarmRepositoryPort).save(alarmSpy)
        assert(alarmSpy.status == AlarmStatus.FAILED)
    }
}
