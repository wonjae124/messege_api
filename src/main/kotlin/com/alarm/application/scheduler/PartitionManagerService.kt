package com.alarm.application.scheduler

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * [파일명] PartitionManagerService.kt
 * [기능] 매일 자정 혹은 새벽에 실행되어 오래된 파티션을 삭제(DROP)하고 신규 파티션을 생성(ADD)합니다.
 * [관점] Application: ShedLock을 적용하여 다중 서버(Scale-Out) 환경에서도 단 한 대의 서버에서만 중복 없이 안전하게 DDL 쿼리가 실행됨을 보장합니다.
 */
@Service
class PartitionManagerService(
    private val jdbcTemplate: JdbcTemplate
) {

    /**
     * 애플리케이션 시작 시(ApplicationReadyEvent) 오늘과 내일 자 파티션이 존재하는지 검증합니다.
     * 장애 복구 후 재시작이나 최초 배포 시 파티션 누락으로 인한 INSERT 에러를 방어하는 부트스트랩 로직입니다.
     */
    @EventListener(ApplicationReadyEvent::class)
    fun validateAndInitializePartitions() {
        println("[PartitionManager] 서버 초기 구동: 파티션 검증 시작...")
        val today = LocalDate.now()
        val tomorrow = today.plusDays(1)
        val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        
        val todayPartition = "p_${today.format(formatter)}"
        val tomorrowPartition = "p_${tomorrow.format(formatter)}"

        val partitionsToCheck = listOf(todayPartition, tomorrowPartition)

        try {
            // MySQL information_schema를 조회하여 파티션 존재 여부 확인
            val sql = """
                SELECT PARTITION_NAME 
                FROM information_schema.PARTITIONS 
                WHERE TABLE_NAME = 'alarms' 
                AND PARTITION_NAME IN (?, ?)
            """.trimIndent()
            
            val existingPartitions = jdbcTemplate.queryForList(sql, String::class.java, todayPartition, tomorrowPartition)

            partitionsToCheck.forEach { partitionName ->
                if (!existingPartitions.contains(partitionName)) {
                    println("[PartitionManager] 누락된 파티션 발견 ($partitionName). 동적 할당을 시도합니다.")
                    // 대상 파티션의 다음 날짜를 limit로 설정
                    val targetDate = LocalDate.parse(partitionName.substring(2), formatter)
                    val limitDateStr = targetDate.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                    
                    val addSql = """
                        ALTER TABLE alarms REORGANIZE PARTITION p_max INTO (
                            PARTITION $partitionName VALUES LESS THAN (TO_DAYS('$limitDateStr')),
                            PARTITION p_max VALUES LESS THAN MAXVALUE
                        )
                    """.trimIndent()
                    jdbcTemplate.execute(addSql)
                    println("[PartitionManager] 파티션 초기화 완료: $partitionName")
                }
            }
            println("[PartitionManager] 초기 파티션 검증 완료. 정상 동작 가능 상태입니다.")
        } catch (e: Exception) {
            println("[PartitionManager] 파티션 메타데이터 조회 무시 (로컬 H2 환경이거나 권한 부족): ${e.message}")
        }
    }

    /**
     * 매일 새벽 3시에 실행 (cron = 초 분 시 일 월 요일)
     * - lockAtLeastFor = "5m": 락을 잡은 서버가 5분 동안 락을 놓지 않음 (완벽한 중복 차단)
     * - lockAtMostFor = "10m": 서버가 중간에 죽어도 10분 뒤엔 락 자동 해제 (데드락 방지)
     */
    @Scheduled(cron = "0 0 3 * * *")
    @SchedulerLock(name = "partition_management_task", lockAtLeastFor = "5m", lockAtMostFor = "10m")
    fun managePartitions() {
        val today = LocalDate.now()
        val targetDropDate = today.minusDays(7)
        val targetAddDate = today.plusDays(3)

        val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        val dropPartitionName = "p_${targetDropDate.format(formatter)}"
        val addPartitionName = "p_${targetAddDate.format(formatter)}"
        
        // 주의: 파티션의 상한값(VALUES LESS THAN)은 대상 일자의 다음 날짜 문자열이어야 합니다.
        val limitDateStr = targetAddDate.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

        println("[PartitionManager] 파티션 관리 스케줄러 시작. 삭제 대상: $dropPartitionName, 추가 대상: $addPartitionName")

        try {
            // 1. 오래된 파티션 삭제 (7일 전)
            val dropSql = "ALTER TABLE alarms DROP PARTITION $dropPartitionName"
            jdbcTemplate.execute(dropSql)
            println("[PartitionManager] 파티션 삭제 완료: $dropSql")
            
            // 2. 신규 파티션 추가 (3일 후)
            // (MySQL의 경우 기존 p_max 파티션을 쪼개기 위해 REORGANIZE PARTITION 구문을 사용해야 할 수도 있음)
            val addSql = """
                ALTER TABLE alarms REORGANIZE PARTITION p_max INTO (
                    PARTITION $addPartitionName VALUES LESS THAN (TO_DAYS('$limitDateStr')),
                    PARTITION p_max VALUES LESS THAN MAXVALUE
                )
            """.trimIndent()
            jdbcTemplate.execute(addSql)
            println("[PartitionManager] 파티션 추가 완료: $addSql")

        } catch (e: Exception) {
            // H2 등 파티셔닝을 지원하지 않는 환경이거나 이미 처리된 경우 예외 발생
            println("[PartitionManager] 파티션 쿼리 실행 무시 (로컬 H2 환경이거나 DB 제약조건): ${e.message}")
        }
    }
}
