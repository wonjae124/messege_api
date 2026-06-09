package com.alarm.grading

import com.alarm.domain.AlarmStatus
import com.alarm.adapter.`in`.web.dto.AlarmCreateRequest
import com.alarm.application.port.out.NotificationPort
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.mockito.Mockito.verify
import org.mockito.ArgumentMatchers.any
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.http.MediaType
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = ["alarm-jobs"], bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class AlarmImmediateTriggerTest {

    @Autowired
    lateinit var mockMvc: MockMvc

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @MockBean
    lateinit var notificationPort: NotificationPort

    @Test
    fun `채점자 확인용 - 즉시 알람 발송 기능 동작 검증`() {
        // 1. 알람 신규 생성 요청 전송
        val request = AlarmCreateRequest(
            name = "Immediate Trigger Grader Test",
            payload = "{\"message\":\"Grader test payload\"}"
        )

        val createResult = mockMvc.perform(
            post("/alarms")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.name").value("Immediate Trigger Grader Test"))
            .andExpect(jsonPath("$.status").value(AlarmStatus.ACTIVE.name))
            .andReturn()

        val responseBody = createResult.response.contentAsString
        val alarmId = objectMapper.readTree(responseBody).get("id").asText()

        // 2. 알람 즉시 발생(Trigger) API 호출
        mockMvc.perform(post("/alarms/$alarmId/trigger"))
            .andExpect(status().isNoContent)

        // 3. 외부 API 발송(NotificationPort)이 실제로 호출되었는지 검증 (Mock API 검증)
        verify(notificationPort).sendNotification(java.util.UUID.fromString(alarmId), "{\"message\":\"Grader test payload\"}")

        // 4. 즉시 발생 후 알람 상태가 TRIGGERED로 변경되었는지 검증
        mockMvc.perform(get("/alarms/$alarmId"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.status").value(AlarmStatus.TRIGGERED.name))
    }
}
