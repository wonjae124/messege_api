package com.alarm.adapter.`in`.messaging

import com.alarm.application.port.`in`.TriggerAlarmUseCase
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.UUID

@Component
class KafkaAlarmJobConsumer(
    private val triggerAlarmUseCase: TriggerAlarmUseCase
) {
    private val logger = LoggerFactory.getLogger(KafkaAlarmJobConsumer::class.java)

    @KafkaListener(topics = ["alarm-jobs"], groupId = "alarm-group", concurrency = "3")
    fun consumeAlarmJob(alarmIdStr: String) {
        logger.info("Received alarm job event from Kafka: alarmId = {}", alarmIdStr)
        try {
            val alarmId = UUID.fromString(alarmIdStr)
            triggerAlarmUseCase.triggerAlarm(alarmId)
            logger.info("Successfully triggered alarm via async Kafka consumption: alarmId = {}", alarmId)
        } catch (e: IllegalArgumentException) {
            logger.error("Invalid Alarm ID format received: {}", alarmIdStr, e)
        } catch (e: Exception) {
            logger.error("Failed to process alarm job for ID: {}", alarmIdStr, e)
        }
    }
}
