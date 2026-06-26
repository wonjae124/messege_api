package com.alarm.adapter.out.notification

import com.alarm.application.port.out.AlarmBatchItem
import com.alarm.exception.CircuitBreakerOpenException
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.retry.Retry
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.mockito.kotlin.doReturn

class MockApiNotificationAdapterTest {

    @Test
    fun `sendNotificationFallback - 서킷 브레이커 OPEN 상태일 때 커스텀 예외를 던진다`() {
        // given
        val dummyRetry = mock<Retry>()
        val adapter = MockApiNotificationAdapter(
            "http://localhost:8081",
            5000, 5, 5, 5, 100, 5, 30, 300, 60, true,
            dummyRetry
        )
        val item = AlarmBatchItem(1L, "sender", "receiver", "SMS", "uuid", "msg")
        val cb = io.github.resilience4j.circuitbreaker.CircuitBreaker.ofDefaults("test_cb")
        val ex = CallNotPermittedException.createCallNotPermittedException(cb)

        // when & then
        val exception = assertThrows(CircuitBreakerOpenException::class.java) {
            adapter.sendNotificationFallback(item, ex).block()
        }
        
        assertTrue(exception.message!!.contains("Circuit breaker is OPEN"))
    }
}
