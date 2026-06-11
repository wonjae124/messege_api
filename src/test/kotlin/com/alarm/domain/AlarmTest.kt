package com.alarm.domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class AlarmTest {

    @Test
    fun `should create alarm with valid inputs`() {
        val alarm = Alarm.create(senderId = "12345", receiverId = "9999", name = "Test Alarm", payload = "{}")

        assertNotNull(alarm.id)
        assertEquals("Test Alarm", alarm.name)
        assertEquals(AlarmStatus.PENDING, alarm.status)
    }

    @Test
    fun `should fail to create alarm when name is blank`() {
        assertThrows<IllegalArgumentException> {
            Alarm.create(senderId = "12345", receiverId = "9999", name = "   ", payload = "{}")
        }
    }

    @Test
    fun `should update status correctly on trigger`() {
        val alarm = Alarm.create(senderId = "12345", receiverId = "9999", name = "Test Alarm", payload = "{}")
        
        alarm.sent()
        assertEquals(AlarmStatus.SENT, alarm.status)
    }
}
