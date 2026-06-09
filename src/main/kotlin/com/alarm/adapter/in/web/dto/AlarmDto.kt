package com.alarm.adapter.`in`.web.dto

import com.alarm.domain.Alarm
import com.alarm.domain.AlarmStatus
import java.time.LocalDateTime
import java.util.UUID

data class AlarmCreateRequest(
    val name: String,
    val payload: String? = null
)

data class AlarmUpdateRequest(
    val name: String? = null,
    val status: AlarmStatus? = null,
    val payload: String? = null
)

data class AlarmResponse(
    val id: UUID,
    val name: String,
    val status: AlarmStatus,
    val payload: String?,
    val createdAt: LocalDateTime,
    val updatedAt: LocalDateTime
) {
    companion object {
        fun fromDomain(alarm: Alarm): AlarmResponse {
            return AlarmResponse(
                id = alarm.id,
                name = alarm.name,
                status = alarm.status,
                payload = alarm.payload,
                createdAt = alarm.createdAt,
                updatedAt = alarm.updatedAt
            )
        }
    }
}
