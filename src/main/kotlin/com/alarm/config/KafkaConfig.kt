package com.alarm.config

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

/**
 * [파일명] KafkaConfig.kt
 * [수정일자] 2026-06-11
 * [기능] Kafka 토픽 구성 및 관리 빈 등록 설정
 * [관점] Configuration: 스프링 및 카프카 연동에 필요한 토픽(alarm-jobs, alarm-jobs-retry)의 파티션 개수와 복제본 정책을 구성하는 인프라스트럭처 설정 클래스입니다.
 */
@Configuration
class KafkaConfig {

    @Bean
    fun alarmJobsTopic(): NewTopic {
        return TopicBuilder.name("alarm-jobs")
            .partitions(3)
            .replicas(3)  // replication.factor=3: 리더 죽어도 팔로워 복제본에 메시지 보존
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")  // 최소 2개 ISR 확인 후 ACK
            .build()
    }

    @Bean
    fun alarmJobsRetryTopic(): NewTopic {
        return TopicBuilder.name("alarm-jobs-retry")
            .partitions(3)
            .replicas(3)  // replication.factor=3
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
            .build()
    }
}
