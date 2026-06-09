package com.alarm.application.port.out

interface LockTemplate {
    fun <T> executeWithLock(lockKey: String, action: () -> T): T
}
