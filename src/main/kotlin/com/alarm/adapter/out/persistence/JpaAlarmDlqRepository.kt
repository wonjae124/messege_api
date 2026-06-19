package com.alarm.adapter.out.persistence

import com.alarm.adapter.out.persistence.entity.AlarmDlqEntity
import org.springframework.data.jpa.repository.JpaRepository

/**
 * [파일명] JpaAlarmDlqRepository.kt
 * [기능] Spring Data JPA 인터페이스
 */
interface JpaAlarmDlqRepository : JpaRepository<AlarmDlqEntity, Long>
