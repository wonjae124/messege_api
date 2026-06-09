package com.alarm.adapter.`in`.web

import com.alarm.adapter.`in`.web.dto.AlarmCreateRequest
import com.alarm.domain.AlarmStatus
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.*
import java.time.LocalDateTime

import org.springframework.boot.test.mock.mockito.MockBean
import com.alarm.application.port.out.NotificationPort

@SpringBootTest
@AutoConfigureMockMvc
class AlarmControllerTest {

    @Autowired
    lateinit var mockMvc: MockMvc

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @MockBean
    lateinit var notificationPort: NotificationPort

    @Test
    fun `should create and retrieve alarm via web adapter`() {
        val request = AlarmCreateRequest(
            name = "Web Test Alarm",
            payload = "{\"key\":\"val\"}"
        )

        val createResult = mockMvc.perform(
            post("/alarms")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.name").value("Web Test Alarm"))
            .andExpect(jsonPath("$.status").value(AlarmStatus.ACTIVE.name))
            .andReturn()

        val responseBody = createResult.response.contentAsString
        val alarmId = objectMapper.readTree(responseBody).get("id").asText()

        mockMvc.perform(get("/alarms/$alarmId"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.name").value("Web Test Alarm"))
    }

    @Test
    fun `should trigger alarm successfully`() {
        val request = AlarmCreateRequest(
            name = "Trigger Test Alarm",
            payload = "{\"key\":\"val\"}"
        )

        val createResult = mockMvc.perform(
            post("/alarms")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isCreated)
            .andReturn()

        val responseBody = createResult.response.contentAsString
        val alarmId = objectMapper.readTree(responseBody).get("id").asText()

        // 알람 즉시 발생 API 호출
        mockMvc.perform(post("/alarms/$alarmId/trigger"))
            .andExpect(status().isNoContent)

        // 알람 상태가 TRIGGERED로 변경되었는지 확인
        mockMvc.perform(get("/alarms/$alarmId"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.status").value(AlarmStatus.TRIGGERED.name))
    }
}
