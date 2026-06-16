package com.alarm.domain

import com.github.f4b6a3.uuid.UuidCreator
import java.time.LocalDateTime

/**
 * [파일명] Alarm.kt
 * [수정일자] 2026-06-11
 * [기능] 알람 도메인 비즈니스 논리 및 상태 관리 엔티티 정의
 * [관점] Domain Entity: 비즈니스 핵심 규칙 및 제약조건을 가지며, 외부 의존성 없이 순수한 코틀린 객체로 상태 전이(create, sent, fail, update)를 통제합니다.
 */
class Alarm private constructor(
    val id: Long,
    val senderId: String,
    val receiverId: String,
    val channelType: String,
    var name: String,
    var status: AlarmStatus,
    var payload: String?,
    val createdAt: LocalDateTime,
    var updatedAt: LocalDateTime
) {
    companion object {
        // 신규 알람 생성
        fun create(
            id: Long,
            senderId: String,
            receiverId: String,
            channelType: String = "SMS",
            name: String,
            payload: String?
        ): Alarm {
            require(name.isNotBlank()) { "알람 이름은 비어있을 수 없습니다." }
            
            val now = LocalDateTime.now()
            return Alarm(
                id = id,
                senderId = senderId,
                receiverId = receiverId,
                channelType = channelType,
                name = name,
                status = AlarmStatus.PENDING,
                payload = payload,
                createdAt = now,
                updatedAt = now
            )
        }

        // DB 등 외부에서 기존 객체 재구성
        fun reconstruct(
            id: Long,
            senderId: String,
            receiverId: String,
            channelType: String,
            name: String,
            status: AlarmStatus,
            payload: String?,
            createdAt: LocalDateTime,
            updatedAt: LocalDateTime
        ): Alarm {
            return Alarm(id, senderId, receiverId, channelType, name, status, payload, createdAt, updatedAt)
        }
    }

    fun isFinalStatus(): Boolean {
        return this.status == AlarmStatus.SENT || this.status == AlarmStatus.FAILED || this.status == AlarmStatus.DEAD
    }

    // 비즈니스 행위 메서드 (도메인 내에서 상태 전이 규칙 제어)
    fun markPending() {
        this.status = AlarmStatus.PENDING
        this.updatedAt = LocalDateTime.now()
    }

    fun processing() {
        this.status = AlarmStatus.PROCESSING
        this.updatedAt = LocalDateTime.now()
    }

    fun sent() {
        this.status = AlarmStatus.SENT
        this.updatedAt = LocalDateTime.now()
    }

    fun retryWait() {
        this.status = AlarmStatus.RETRY_WAIT
        this.updatedAt = LocalDateTime.now()
    }

    fun fail() {
        this.status = AlarmStatus.FAILED
        this.updatedAt = LocalDateTime.now()
    }

    fun dead() {
        this.status = AlarmStatus.DEAD
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
