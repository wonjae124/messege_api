package com.alarm.adapter.out.messaging

import com.alarm.application.port.out.MessageQueuePort
import com.alarm.domain.Alarm
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class KafkaAlarmJobProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>
) : MessageQueuePort {

    override fun publishAlarmJob(alarm: Alarm) {
        val key = alarm.id.toString()
        val payload = alarm.id.toString()
        // 토픽: alarm-jobs, 키: alarmId (파티션 정렬용 키), 값: alarmId
        kafkaTemplate.send("alarm-jobs", key, payload)
    }
}
