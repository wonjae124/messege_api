package com.alarm.exception

/**
 * [파일명] CircuitBreakerOpenException.kt
 * [기능] 서킷 브레이커가 OPEN 상태일 때 외부 호출이 차단되면 발생하는 커스텀 예외
 */
class CircuitBreakerOpenException(message: String) : RuntimeException(message)
