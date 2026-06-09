package com.alarm.adapter.out.notification

import com.alarm.application.port.out.NotificationPort
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import java.util.UUID

@Component
class MockApiNotificationAdapter(
    @Value("\${mock.api.url:http://localhost:8081}") private val mockApiUrl: String
) : NotificationPort {

    private val restClient = RestClient.builder().baseUrl(mockApiUrl).build()

    override fun sendNotification(alarmId: UUID, payload: String?) {
        restClient.post()
            .uri("/alarms/trigger")
            .contentType(MediaType.APPLICATION_JSON)
            .body(mapOf(
                "alarmId" to alarmId.toString(),
                "payload" to payload
            ))
            .retrieve()
            .toBodilessEntity()
    }
}
