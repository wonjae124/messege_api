package com.alarm.adapter.out.messaging

import com.alarm.application.port.out.AlarmOutboxEventRepositoryPort
import com.alarm.application.port.out.AlarmRepositoryPort
import com.alarm.application.port.out.MessageQueuePort
import com.alarm.domain.Alarm
import com.alarm.domain.AlarmOutboxEvent
import com.alarm.domain.OutboxStatus
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.*
import org.springframework.transaction.support.TransactionCallback
import org.springframework.transaction.support.TransactionTemplate
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class OutboxEventPublisherTest {

    private lateinit var alarmOutboxEventRepositoryPort: AlarmOutboxEventRepositoryPort
    private lateinit var alarmRepositoryPort: AlarmRepositoryPort
    private lateinit var messageQueuePort: MessageQueuePort
    private lateinit var transactionTemplate: TransactionTemplate
    private lateinit var publisher: OutboxEventPublisher

    @BeforeEach
    fun setUp() {
        alarmOutboxEventRepositoryPort = mock<AlarmOutboxEventRepositoryPort>()
        alarmRepositoryPort = mock<AlarmRepositoryPort>()
        messageQueuePort = mock<MessageQueuePort>()
        transactionTemplate = mock<TransactionTemplate>()

        whenever(transactionTemplate.execute(any<TransactionCallback<List<AlarmOutboxEvent>>>())).thenAnswer {
            val callback = it.arguments[0] as TransactionCallback<*>
            callback.doInTransaction(mock<org.springframework.transaction.TransactionStatus>())
        }

        whenever(transactionTemplate.executeWithoutResult(any())).thenAnswer {
            val callback = it.arguments[0] as java.util.function.Consumer<org.springframework.transaction.TransactionStatus>
            callback.accept(mock<org.springframework.transaction.TransactionStatus>())
        }

        publisher = OutboxEventPublisher(
            alarmOutboxEventRepositoryPort,
            alarmRepositoryPort,
            messageQueuePort,
            transactionTemplate,
            100
        )
    }

    @Test
    fun `publishOutboxEvents - 이벤트 발행 성공 시 PUBLISHED 상태로 변경된다`() {
        // given
        val alarm = Alarm.create(1L, "sender1", "receiver1", "SMS", "Test", "Hello")
        val event = AlarmOutboxEvent.create(alarm)
        
        whenever(alarmOutboxEventRepositoryPort.findPendingEventsForUpdate(any(), eq(100)))
            .thenReturn(listOf(event))
            
        val future = CompletableFuture.completedFuture<Any>(Any())
        whenever(messageQueuePort.publishAlarmJobFromOutbox(event)).thenReturn(future)

        // when
        publisher.publishOutboxEvents()

        // then
        assertEquals(OutboxStatus.PUBLISHED, event.status)
        verify(alarmOutboxEventRepositoryPort, times(1)).saveAll(any())
    }

    @Test
    fun `publishOutboxEvents - 이벤트 발행 일시 실패 시 PENDING 상태를 유지하고 retryCount를 증가시킨다`() {
        // given
        val alarm = Alarm.create(1L, "sender1", "receiver1", "SMS", "Test", "Hello")
        val event = AlarmOutboxEvent.create(alarm)
        
        whenever(alarmOutboxEventRepositoryPort.findPendingEventsForUpdate(any(), eq(100)))
            .thenReturn(listOf(event))
            
        val future = CompletableFuture<Any>()
        future.completeExceptionally(RuntimeException("Kafka error"))
        whenever(messageQueuePort.publishAlarmJobFromOutbox(event)).thenReturn(future)

        // when
        publisher.publishOutboxEvents()

        // then
        assertEquals(OutboxStatus.PENDING, event.status)
        assertEquals(1, event.retryCount)
        verify(alarmOutboxEventRepositoryPort, times(1)).saveAll(any())
    }

    @Test
    fun `publishOutboxEvents - 3회 재시도 초과 시 PUBLISH_FAILED 상태로 변경하고 원본 알람을 FAILED 처리한다`() {
        // given
        val alarm = Alarm.create(1L, "sender1", "receiver1", "SMS", "Test", "Hello")
        val event = AlarmOutboxEvent.create(alarm)
        event.retryCount = 3 // 4번째 시도에서 실패하도록 설정
        
        whenever(alarmOutboxEventRepositoryPort.findPendingEventsForUpdate(any(), eq(100)))
            .thenReturn(listOf(event))
            
        val future = CompletableFuture<Any>()
        future.completeExceptionally(RuntimeException("Kafka error"))
        whenever(messageQueuePort.publishAlarmJobFromOutbox(event)).thenReturn(future)
        
        whenever(alarmRepositoryPort.findById(alarm.id)).thenReturn(alarm)

        // when
        publisher.publishOutboxEvents()

        // then
        assertEquals(OutboxStatus.PUBLISH_FAILED, event.status)
        assertEquals(4, event.retryCount)
        assertEquals(com.alarm.domain.AlarmStatus.FAILED, alarm.status)
        verify(alarmRepositoryPort, times(1)).save(alarm)
        verify(alarmOutboxEventRepositoryPort, times(1)).saveAll(any())
    }
}
