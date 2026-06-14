package com.alarm.adapter.out.messaging

import com.alarm.application.port.out.MessageQueuePort
import com.alarm.domain.Alarm
import com.alarm.domain.AlarmOutboxEvent
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

/**
 * [파일명] KafkaAlarmJobProducer.kt
 * [수정일자] 2026-06-14
 * [기능] Kafka 비동기 알람 메시지 발행 처리
 * [관점] Outbound Adapter: MessageQueuePort 구체 클래스로서, 생성된 알람에 대한 비동기 발송 작업을 카프카 브로커에 JSON 형태로 발행(Publish)합니다.
 *        publishAlarmJobFromOutbox()는 OutboxEventPublisher 스케줄러가 호출하며, Outbox Pattern에 의해 메시지 유실 없는 발행을 보장합니다.
 */
@Component
class KafkaAlarmJobProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>
) : MessageQueuePort {

    override fun publishAlarmJob(alarm: Alarm) {
        val partitionKey = "${alarm.senderId}:${alarm.receiverId}"
        val uuidClean = alarm.id.toString().replace("-", "")
        val payload = """{"alarmId":"${alarm.id}","senderId":"${alarm.senderId}","receiverId":"${alarm.receiverId}","uuid":"$uuidClean"}"""
        kafkaTemplate.send("alarm-jobs", partitionKey, payload)
    }

    // Outbox 폴러가 호출하는 발행 메서드 - 결과를 동기로 확인하여 실패 시 예외 발생
    override fun publishAlarmJobFromOutbox(event: AlarmOutboxEvent) {
        val partitionKey = "${event.senderId}:${event.receiverId}"
        val payload = """{"alarmId":"${event.alarmId}","senderId":"${event.senderId}","receiverId":"${event.receiverId}","uuid":"${event.uuid}"}"""
        kafkaTemplate.send("alarm-jobs", partitionKey, payload).get() // 브로커 ACK 동기 대기
    }
}
