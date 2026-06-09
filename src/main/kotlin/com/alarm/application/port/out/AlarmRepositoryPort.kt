package com.alarm.application.port.out

import com.alarm.domain.Alarm
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import java.util.UUID

interface AlarmRepositoryPort {
    fun save(alarm: Alarm): Alarm
    fun findById(id: UUID): Alarm?
    fun findAll(pageable: Pageable): Page<Alarm>
    fun delete(alarm: Alarm)
}
