package com.alarm.adapter.out.persistence.repository

import com.alarm.adapter.out.persistence.entity.AlarmEntity
import org.springframework.data.jpa.repository.JpaRepository

import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import java.time.LocalDateTime

/**
 * [파일명] SpringDataAlarmRepository.kt
 * [수정일자] 2026-06-11
 * [기능] Spring Data JPA 기반의 알람 테이블 데이터 액세스 인터페이스
 * [관점] Outbound Infrastructure: Spring Data JPA 프레임워크 기술을 활용하여 기본적인 CRUD 쿼리를 자동으로 처리하고 DB 물리 레이어에 접근합니다.
 */

interface SpringDataAlarmRepository : JpaRepository<AlarmEntity, Long> {

    @Query("""
        SELECT a FROM AlarmEntity a 
        WHERE a.senderId = :senderId 
        AND (:status IS NULL OR a.status = :status)
        AND a.createdAt >= :cutoffDate 
        AND (:cursorCreatedAt IS NULL OR 
             a.createdAt < :cursorCreatedAt OR 
             (a.createdAt = :cursorCreatedAt AND a.id < :cursorId))
        ORDER BY a.createdAt DESC, a.id DESC
    """)
    fun findHistoryBySenderId(
        @Param("senderId") senderId: String,
        @Param("status") status: com.alarm.domain.AlarmStatus?,
        @Param("cutoffDate") cutoffDate: LocalDateTime,
        @Param("cursorCreatedAt") cursorCreatedAt: LocalDateTime?,
        @Param("cursorId") cursorId: Long?,
        pageable: Pageable
    ): List<AlarmEntity>

    @Query("""
        SELECT a FROM AlarmEntity a 
        WHERE a.receiverId = :receiverId 
        AND (:status IS NULL OR a.status = :status)
        AND a.createdAt >= :cutoffDate 
        AND (:cursorCreatedAt IS NULL OR 
             a.createdAt < :cursorCreatedAt OR 
             (a.createdAt = :cursorCreatedAt AND a.id < :cursorId))
        ORDER BY a.createdAt DESC, a.id DESC
    """)
    fun findHistoryByReceiverId(
        @Param("receiverId") receiverId: String,
        @Param("status") status: com.alarm.domain.AlarmStatus?,
        @Param("cutoffDate") cutoffDate: LocalDateTime,
        @Param("cursorCreatedAt") cursorCreatedAt: LocalDateTime?,
        @Param("cursorId") cursorId: Long?,
        pageable: Pageable
    ): List<AlarmEntity>
}
