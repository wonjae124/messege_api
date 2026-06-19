package com.alarm.exception

/**
 * [파일명] DuplicateRequestException.kt
 * [기능] 멱등성 검증 시 중복된 요청이 감지되었을 때 발생하는 커스텀 예외
 */
class DuplicateRequestException(message: String) : RuntimeException(message)
