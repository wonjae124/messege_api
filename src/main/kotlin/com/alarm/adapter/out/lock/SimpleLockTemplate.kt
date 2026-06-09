package com.alarm.adapter.out.lock

import com.alarm.application.port.out.LockTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

@Component
class SimpleLockTemplate : LockTemplate {
    private val locks = ConcurrentHashMap<String, ReentrantLock>()

    override fun <T> executeWithLock(lockKey: String, action: () -> T): T {
        val lock = locks.computeIfAbsent(lockKey) { ReentrantLock() }
        lock.lock()
        try {
            return action()
        } finally {
            lock.unlock()
            // 대기하는 스레드가 없을 경우 맵에서 락 객체 제거
            if (!lock.hasQueuedThreads()) {
                locks.remove(lockKey, lock)
            }
        }
    }
}
