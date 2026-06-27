package com.alarm.exception

/**
 * [파일명] ReadTimeoutExternalException.kt
 * [기능] 외부 API 읽기 타임아웃 발생 시 발생하는 커스텀 예외
 */
class ReadTimeoutExternalException(message: String) : RuntimeException(message)
