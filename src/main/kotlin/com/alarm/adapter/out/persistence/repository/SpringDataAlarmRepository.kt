package com.alarm.adapter.out.persistence.repository

import com.alarm.adapter.out.persistence.entity.AlarmEntity
import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

interface SpringDataAlarmRepository : JpaRepository<AlarmEntity, UUID>
