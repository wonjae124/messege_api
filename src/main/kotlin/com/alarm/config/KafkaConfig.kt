package com.alarm.config

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

/**
 * [파일명] KafkaConfig.kt
 * [수정일자] 2026-06-24
 * [기능] Kafka 토픽 구성 및 관리 빈 등록 설정
 * [관점] Configuration: 스프링 및 카프카 연동에 필요한 토픽(alarm-jobs, alarm-jobs-retry)의 파티션 개수와 복제본 정책을 구성하는 인프라스트럭처 설정 클래스입니다.
 */
@Configuration
class KafkaConfig(
    @Value("\${spring.kafka.topic.partitions:3}") private val partitions: Int,
    @Value("\${spring.kafka.topic.replication-factor:3}") private val replicas: Int
) {

    @Bean
    fun alarmJobsTopic(): NewTopic {
        return TopicBuilder.name("alarm-jobs")
            .partitions(partitions)
            .replicas(replicas)
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
            .build()
    }

    @Bean
    fun alarmJobsRetryTopic(): NewTopic {
        return TopicBuilder.name("alarm-jobs-retry")
            .partitions(partitions)
            .replicas(replicas)
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
            .build()
    }
}
