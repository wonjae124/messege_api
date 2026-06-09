package com.alarm.domain

import java.time.LocalDateTime
import java.util.UUID

class Alarm private constructor(
    val id: UUID,
    var name: String,
    var status: AlarmStatus,
    var payload: String?,
    val createdAt: LocalDateTime,
    var updatedAt: LocalDateTime
) {
    companion object {
        // 신규 알람 생성
        fun create(
            name: String,
            payload: String?
        ): Alarm {
            require(name.isNotBlank()) { "알람 이름은 비어있을 수 없습니다." }
            
            val now = LocalDateTime.now()
            return Alarm(
                id = UUID.randomUUID(),
                name = name,
                status = AlarmStatus.ACTIVE,
                payload = payload,
                createdAt = now,
                updatedAt = now
            )
        }

        // DB 등 외부에서 기존 객체 재구성
        fun reconstruct(
            id: UUID,
            name: String,
            status: AlarmStatus,
            payload: String?,
            createdAt: LocalDateTime,
            updatedAt: LocalDateTime
        ): Alarm {
            return Alarm(id, name, status, payload, createdAt, updatedAt)
        }
    }

    // 비즈니스 행위 메서드 (도메인 내에서 상태 전이 규칙 제어)
    fun activate() {
        this.status = AlarmStatus.ACTIVE
        this.updatedAt = LocalDateTime.now()
    }

    fun deactivate() {
        this.status = AlarmStatus.INACTIVE
        this.updatedAt = LocalDateTime.now()
    }

    fun trigger() {
        this.status = AlarmStatus.TRIGGERED
        this.updatedAt = LocalDateTime.now()
    }

    fun fail() {
        this.status = AlarmStatus.FAILED
        this.updatedAt = LocalDateTime.now()
    }

    fun update(name: String?, payload: String?) {
        name?.let {
            require(it.isNotBlank()) { "알람 이름은 비어있을 수 없습니다." }
            this.name = it
        }
        payload?.let { this.payload = it }
        this.updatedAt = LocalDateTime.now()
    }
}
