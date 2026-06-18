package com.alarm.adapter.out.messaging

import com.alarm.application.port.out.MessageQueuePort
import org.slf4j.MDC
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

/**
 * [파일명] KafkaAlarmJobProducer.kt
 * [수정일자] 2026-06-18
 * [기능] Outbox 폴러(OutboxEventPublisher)가 사용하는 Kafka 동기 발행 처리
 * [관점] Outbound Adapter: MessageQueuePort 구체 클래스로서, Outbox 이벤트를 카프카 브로커에
 *        JSON 형태로 발행하고 send().get()으로 ACK를 동기 확인합니다.
 */
@Component
class KafkaAlarmJobProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>
) : MessageQueuePort {

    override fun publishAlarmJobFromOutbox(event: com.alarm.domain.AlarmOutboxEvent) {
        val partitionKey = "${event.senderId}:${event.receiverId}"
        val requestId = MDC.get("requestId")
        val spanContext = io.opentelemetry.api.trace.Span.current().spanContext
        val traceId = if (spanContext.isValid) spanContext.traceId else null
        
        val payload = """{"alarmId":"${event.alarmId}","senderId":"${event.senderId}","receiverId":"${event.receiverId}","uuid":"${event.uuid}","requestId":${if (requestId != null) "\"$requestId\"" else "null"},"traceId":${if (traceId != null) "\"$traceId\"" else "null"},"retryCount":0}"""
        
        kafkaTemplate.send("alarm-jobs", partitionKey, payload).get(5, TimeUnit.SECONDS)
    }
}
