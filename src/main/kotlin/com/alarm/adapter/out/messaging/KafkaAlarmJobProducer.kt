package com.alarm.adapter.out.messaging

import com.alarm.application.port.out.MessageQueuePort
import com.alarm.domain.Alarm
import org.slf4j.MDC
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

/**
 * [파일명] KafkaAlarmJobProducer.kt
 * [수정일자] 2026-06-15
 * [기능] Kafka 비동기 알람 메시지 발행 처리
 * [관점] Outbound Adapter: MessageQueuePort 구체 클래스로서, 생성된 알람에 대한 비동기 발송 작업을 카프카 브로커에 JSON 형태로 발행(Publish)하며 request 추적 메타데이터를 추가합니다.
 */
@Component
class KafkaAlarmJobProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>
) : MessageQueuePort {

    override fun publishAlarmJob(alarm: Alarm) {
        val partitionKey = "${alarm.senderId}:${alarm.receiverId}"
        val uuidClean = alarm.id.toString()
        
        val requestId = MDC.get("requestId")
        val spanContext = io.opentelemetry.api.trace.Span.current().spanContext
        val traceId = if (spanContext.isValid) spanContext.traceId else null
        
        val payload = """{"alarmId":"${alarm.id}","senderId":"${alarm.senderId}","receiverId":"${alarm.receiverId}","uuid":"$uuidClean","requestId":${if (requestId != null) "\"$requestId\"" else "null"},"traceId":${if (traceId != null) "\"$traceId\"" else "null"},"retryCount":0}"""
        
        kafkaTemplate.send("alarm-jobs", partitionKey, payload)
    }

    override fun publishAlarmJobFromOutbox(event: com.alarm.domain.AlarmOutboxEvent) {
        val partitionKey = "${event.senderId}:${event.receiverId}"
        val requestId = MDC.get("requestId")
        val spanContext = io.opentelemetry.api.trace.Span.current().spanContext
        val traceId = if (spanContext.isValid) spanContext.traceId else null
        
        val payload = """{"alarmId":"${event.alarmId}","senderId":"${event.senderId}","receiverId":"${event.receiverId}","uuid":"${event.uuid}","requestId":${if (requestId != null) "\"$requestId\"" else "null"},"traceId":${if (traceId != null) "\"$traceId\"" else "null"},"retryCount":0}"""
        
        kafkaTemplate.send("alarm-jobs", partitionKey, payload).get(5, java.util.concurrent.TimeUnit.SECONDS)
    }
}
