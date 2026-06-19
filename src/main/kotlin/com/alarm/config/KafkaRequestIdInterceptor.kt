package com.alarm.config

import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.MDC
import java.util.Map

/**
 * [파일명] KafkaRequestIdInterceptor.kt
 * [기능] Kafka 메시지 발행(Send) 시 MDC에 보관된 requestId가 존재하면 Kafka Header("X-Request-Id")에 삽입하여 전파합니다.
 */
class KafkaRequestIdInterceptor : ProducerInterceptor<Any, Any> {
    
    override fun configure(configs: MutableMap<String, *>) {}

    override fun onSend(record: ProducerRecord<Any, Any>): ProducerRecord<Any, Any> {
        val requestId = MDC.get("requestId")
        if (!requestId.isNullOrBlank()) {
            record.headers().add("X-Request-Id", requestId.toByteArray(Charsets.UTF_8))
        }
        return record
    }

    override fun onAcknowledgement(metadata: RecordMetadata?, exception: java.lang.Exception?) {}

    override fun close() {}
}
