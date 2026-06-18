package com.alarm.adapter.`in`.messaging

import com.alarm.application.port.`in`.RecordAlarmDlqUseCase
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.DltHandler
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component

@Component("dltProcessor")
class DltProcessor(
    private val recordAlarmDlqUseCase: RecordAlarmDlqUseCase,
    private val objectMapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(DltProcessor::class.java)

    @DltHandler
    fun handle(
        record: ConsumerRecord<String, String>,
        @Header(KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) exceptionMessage: String?,
        @Header(KafkaHeaders.DLT_EXCEPTION_STACKTRACE, required = false) exceptionStackTrace: String?
    ) {
        val payloadStr = record.value()
        logger.error("DLT received - payload {} - offset {} - error: {}", payloadStr, record.offset(), exceptionMessage)
        
        var alarmId: Long? = null
        try {
            val rootNode = objectMapper.readTree(payloadStr)
            if (rootNode.has("alarmId")) {
                alarmId = rootNode.get("alarmId").asLong()
            }
        } catch (e: Exception) {
            logger.warn("Failed to parse alarmId from DLT payload: {}", payloadStr, e)
        }

        if (alarmId != null) {
            recordAlarmDlqUseCase.recordDlq(
                alarmId = alarmId,
                payload = payloadStr,
                exceptionMessage = exceptionMessage,
                stackTrace = exceptionStackTrace
            )
        } else {
            logger.error("Cannot record DLQ because alarmId is missing. Payload: {}", payloadStr)
        }
    }
}
