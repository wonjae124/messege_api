package com.alarm.adapter.`in`.web.dto

import java.time.LocalDateTime

/**
 * [파일명] ErrorResponse.kt
 * [기능] REST API 전역 에러 발생 시 공통 응답 포맷
 */
data class ErrorResponse(
    val timestamp: LocalDateTime,
    val status: Int,
    val error: String,
    val code: String,
    val message: String,
    val requestId: String?,
    val path: String
)
