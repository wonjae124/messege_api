package com.alarm.domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class AlarmTest {

    @Test
    fun `should create alarm with valid inputs`() {
        val alarm = Alarm.create("Test Alarm", "{}")

        assertNotNull(alarm.id)
        assertEquals("Test Alarm", alarm.name)
        assertEquals(AlarmStatus.ACTIVE, alarm.status)
    }

    @Test
    fun `should fail to create alarm when name is blank`() {
        assertThrows<IllegalArgumentException> {
            Alarm.create("   ", "{}")
        }
    }

    @Test
    fun `should update status correctly on trigger`() {
        val alarm = Alarm.create("Test Alarm", "{}")
        
        alarm.trigger()
        assertEquals(AlarmStatus.TRIGGERED, alarm.status)
    }
}
