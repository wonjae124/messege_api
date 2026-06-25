package com.alarm.config

import net.javacrumbs.shedlock.core.LockProvider
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.EventListener
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.EnableScheduling
import javax.sql.DataSource

/**
 * [파일명] ShedLockConfig.kt
 * [기능] 다중 서버 환경에서 스케줄러 중복 실행을 방지하기 위한 ShedLock 설정
 * [관점] Infrastructure: Redis 없이 기존 RDB(PostgreSQL/H2)의 테이블을 활용하여 분산 락을 제공합니다.
 */
@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "10m")
class ShedLockConfig {

    @Bean
    fun lockProvider(dataSource: DataSource): LockProvider {
        return JdbcTemplateLockProvider(
            JdbcTemplateLockProvider.Configuration.builder()
                .withJdbcTemplate(JdbcTemplate(dataSource))
                .usingDbTime() // DB의 시간을 기준으로 락 만료 판단 (서버 간 시간차로 인한 동시성 이슈 완벽 차단)
                .build()
        )
    }

    /**
     * 애플리케이션 시작 시 DB에 shedlock 테이블이 없으면 자동 생성합니다.
     * (별도의 flyway/schema.sql 작업 없이 즉시 적용되도록 하기 위함)
     */
    @EventListener(ApplicationReadyEvent::class)
    fun createShedLockTable(event: ApplicationReadyEvent) {
        val jdbcTemplate = event.applicationContext.getBean(JdbcTemplate::class.java)
        
        val createTableSql = """
            CREATE TABLE IF NOT EXISTS shedlock (
                name VARCHAR(64) NOT NULL,
                lock_until TIMESTAMP NOT NULL,
                locked_at TIMESTAMP NOT NULL,
                locked_by VARCHAR(255) NOT NULL,
                PRIMARY KEY (name)
            )
        """.trimIndent()
        
        try {
            jdbcTemplate.execute(createTableSql)
        } catch (e: Exception) {
            // 테이블이 이미 존재하거나 권한 부족 등의 이슈 발생 시 로깅만 수행
            println("[ShedLockConfig] shedlock 테이블 생성 중 오류 발생 또는 이미 존재함: ${e.message}")
        }
    }
}
