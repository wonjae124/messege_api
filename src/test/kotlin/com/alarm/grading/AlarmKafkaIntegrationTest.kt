package com.alarm.grading

import com.alarm.domain.AlarmStatus
import com.alarm.adapter.`in`.web.dto.AlarmCreateRequest
import com.alarm.application.port.out.NotificationPort
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.mockito.Mockito.verify
import org.mockito.Mockito.timeout
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
import java.util.UUID

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = ["alarm-jobs"], bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class AlarmKafkaIntegrationTest {

    @Autowired
    lateinit var mockMvc: MockMvc

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @MockBean
    lateinit var notificationPort: NotificationPort

    @Test
    fun `카프카 기반 비동기 알람 발송 및 멱등성 검증`() {
        // 1. 알람 신규 생성 요청 전송
        val request = AlarmCreateRequest(
            name = "Async Kafka Integration Test",
            payload = "{\"message\":\"Async test payload\"}"
        )

        val createResult = mockMvc.perform(
            post("/alarms")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.name").value("Async Kafka Integration Test"))
            .andExpect(jsonPath("$.status").value(AlarmStatus.ACTIVE.name))
            .andReturn()

        val responseBody = createResult.response.contentAsString
        val alarmId = objectMapper.readTree(responseBody).get("id").asText()
        val alarmUuid = UUID.fromString(alarmId)

        // 2. 비동기적으로 NotificationPort.sendNotification이 호출되는지 검증 (최대 5초 대기)
        verify(notificationPort, timeout(5000)).sendNotification(alarmUuid, "{\"message\":\"Async test payload\"}")

        // 3. 비동기 발생 후 최종 상태가 TRIGGERED로 자동 변경되었는지 검증 (최대 5초 대기/폴링)
        var statusIsTriggered = false
        for (i in 1..50) {
            val getResult = mockMvc.perform(get("/alarms/$alarmId"))
                .andExpect(status().isOk)
                .andReturn()
            
            val statusVal = objectMapper.readTree(getResult.response.contentAsString).get("status").asText()
            if (statusVal == AlarmStatus.TRIGGERED.name) {
                statusIsTriggered = true
                break
            }
            Thread.sleep(100)
        }
        
        assert(statusIsTriggered) { "알람 상태가 TRIGGERED로 변경되지 않았습니다." }
    }
}
