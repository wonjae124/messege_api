package com.alarm.exception

/**
 * [파일명] ExternalApiException.kt
 * [기능] 외부 API 호출 실패 시 발생하는 커스텀 예외
 */
class ExternalApiException(
    val statusCode: Int,
    message: String
) : RuntimeException(message)
