package com.alarm.exception

/**
 * [파일명] AlarmNotFoundException.kt
 * [기능] 알람 엔티티를 찾지 못했을 때 발생하는 커스텀 예외
 */
class AlarmNotFoundException(message: String) : RuntimeException(message)
