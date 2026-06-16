package com.alarm.adapter.`in`.messaging

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.DltHandler
import org.springframework.stereotype.Component

@Component("dltProcessor")
class DltProcessor {
    private val logger = LoggerFactory.getLogger(DltProcessor::class.java)

    @DltHandler
    fun handle(record: ConsumerRecord<String, String>) {
        logger.error("DLT received - payload {} - offset {}", record.value(), record.offset())
        // TODO: Slack, Email 등 알림 전송 로직 혹은 별도 Dead-Letter DB 저장 로직 연동
    }
}
