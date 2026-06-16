package com.alarm.adapter.`in`.web

import com.alarm.adapter.`in`.web.dto.AlarmCreateRequest
import com.alarm.adapter.`in`.web.dto.AlarmResponse
import com.alarm.adapter.`in`.web.dto.AlarmUpdateRequest
import com.alarm.application.port.`in`.*
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.web.PageableDefault
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import com.github.f4b6a3.uuid.UuidCreator

/**
 * [파일명] AlarmController.kt
 * [수정일자] 2026-06-15
 * [기능] 알람 관리를 위한 REST API 엔드포인트 제공
 * [관점] Inbound Adapter: HTTP 요청을 처리하여 도메인의 알람 생성 및 조회/수정/삭제/발생 유스케이스로 커맨드를 매핑 및 전달하며 API 레벨 멱등키를 추출합니다.
 */
@RestController
@Tag(name = "Alarm API", description = "단발성 알람 관리를 위한 REST API 엔드포인트")
class AlarmController(
    private val createAlarmUseCase: CreateAlarmUseCase,
    private val getAlarmQuery: GetAlarmQuery,
    private val updateAlarmUseCase: UpdateAlarmUseCase,
    private val deleteAlarmUseCase: DeleteAlarmUseCase,
    private val triggerAlarmUseCase: TriggerAlarmUseCase
) {

    @PostMapping("/notifications")
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "알람 신규 생성", description = "지정된 이름과 페이로드로 알람을 생성합니다.")
    fun createAlarm(
        @RequestHeader(value = "Idempotency-Key", required = false) idempotencyKey: String?,
        @RequestBody request: AlarmCreateRequest
    ): AlarmResponse {
        val parsedId: String
        val parsedSenderId: String
        
        if (!idempotencyKey.isNullOrBlank()) {
            parsedId = idempotencyKey
            if (idempotencyKey.length > 32) {
                parsedSenderId = idempotencyKey.substring(0, idempotencyKey.length - 32)
            } else {
                parsedSenderId = "12345"
            }
        } else {
            val uuidPart = UuidCreator.getTimeOrderedEpoch().toString().replace("-", "")
            parsedSenderId = "12345"
            parsedId = "${parsedSenderId}${uuidPart}"
        }

        val command = CreateAlarmUseCase.CreateAlarmCommand(
            id = parsedId,
            senderId = parsedSenderId,
            receiverId = request.receiverId ?: "defaultReceiver",
            name = request.name,
            payload = request.payload
        )
        val alarm = createAlarmUseCase.createAlarm(command)
        return AlarmResponse.fromDomain(alarm)
    }

    @GetMapping("/notifications/{id}")
    @Operation(summary = "알람 단건 조회", description = "특정 ID에 해당하는 알람 정보를 조회합니다.")
    fun getAlarm(@PathVariable id: String): AlarmResponse {
        val alarm = getAlarmQuery.getAlarm(id)
        return AlarmResponse.fromDomain(alarm)
    }

    @GetMapping("/notifications")
    @Operation(summary = "알람 목록 조회", description = "페이지네이션을 기반으로 알람 목록을 조회합니다.")
    fun listAlarms(
        @PageableDefault(page = 0, size = 10) pageable: Pageable
    ): Page<AlarmResponse> {
        return getAlarmQuery.listAlarms(pageable).map { AlarmResponse.fromDomain(it) }
    }

    @PutMapping("/notifications/{id}")
    @Operation(summary = "알람 정보 수정", description = "알람 이름, 페이로드, 상태 등을 수정합니다.")
    fun updateAlarm(
        @PathVariable id: String,
        @RequestBody request: AlarmUpdateRequest
    ): AlarmResponse {
        val command = UpdateAlarmUseCase.UpdateAlarmCommand(
            id = id,
            name = request.name,
            status = request.status,
            payload = request.payload
        )
        val alarm = updateAlarmUseCase.updateAlarm(command)
        return AlarmResponse.fromDomain(alarm)
    }

    @DeleteMapping("/notifications/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(summary = "알람 삭제", description = "특정 ID에 해당하는 알람을 삭제합니다.")
    fun deleteAlarm(@PathVariable id: String) {
        deleteAlarmUseCase.deleteAlarm(id)
    }

    @PostMapping("/notifications/{id}/trigger")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(summary = "알람 즉시 발생", description = "알람을 비동기 큐가 아닌 외부 API로 즉시 전송합니다.")
    fun triggerAlarm(@PathVariable id: String) {
        triggerAlarmUseCase.triggerAlarm(id)
    }
}
