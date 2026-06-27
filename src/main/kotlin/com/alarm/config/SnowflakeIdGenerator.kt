package com.alarm.config

object SnowflakeIdGenerator {
    private const val START_EPOCH = 1700000000000L // Custom epoch (Nov 2023)
    private const val NODE_ID_BITS = 10
    private const val SEQUENCE_BITS = 12

    private const val MAX_NODE_ID = (1 shl NODE_ID_BITS) - 1
    private const val MAX_SEQUENCE = (1 shl SEQUENCE_BITS) - 1

    private val nodeId = System.getenv("NODE_ID")?.toLongOrNull()?.also { require(it in 0..MAX_NODE_ID) } ?: throw IllegalStateException("NODE_ID environment variable is required and must be between 0 and ${MAX_NODE_ID}")

    private var lastTimestamp = -1L
    private var sequence = 0L

    @Synchronized
    fun nextId(): Long {
        var timestamp = System.currentTimeMillis()

        if (timestamp < lastTimestamp) {
            throw RuntimeException("Clock moved backwards. Refusing to generate id for ${lastTimestamp - timestamp} milliseconds")
        }

        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) and MAX_SEQUENCE.toLong()
            if (sequence == 0L) {
                timestamp = waitNextMillis(lastTimestamp)
            }
        } else {
            sequence = 0L
        }

        lastTimestamp = timestamp

        return ((timestamp - START_EPOCH) shl (NODE_ID_BITS + SEQUENCE_BITS)) or
                (nodeId shl SEQUENCE_BITS) or
                sequence
    }

    private fun waitNextMillis(lastTimestamp: Long): Long {
        var timestamp = System.currentTimeMillis()
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis()
        }
        return timestamp
    }
}

fun idempotencyKeyToLong(key: String): Long {
    var hash = -0x7ee2623b31687c17L
    val fnvPrime = 0x100000001b3L
    for (i in 0 until key.length) {
        hash = hash xor key[i].code.toLong()
        hash *= fnvPrime
    }
    return hash and Long.MAX_VALUE
}
