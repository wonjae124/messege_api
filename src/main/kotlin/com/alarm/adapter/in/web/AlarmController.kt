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
import java.util.UUID

@RestController
@RequestMapping("/alarms")
@Tag(name = "Alarm API", description = "단발성 알람 관리를 위한 REST API 엔드포인트")
class AlarmController(
    private val createAlarmUseCase: CreateAlarmUseCase,
    private val getAlarmQuery: GetAlarmQuery,
    private val updateAlarmUseCase: UpdateAlarmUseCase,
    private val deleteAlarmUseCase: DeleteAlarmUseCase,
    private val triggerAlarmUseCase: TriggerAlarmUseCase
) {

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "알람 신규 생성", description = "지정된 이름과 페이로드로 알람을 생성합니다.")
    fun createAlarm(@RequestBody request: AlarmCreateRequest): AlarmResponse {
        val command = CreateAlarmUseCase.CreateAlarmCommand(
            name = request.name,
            payload = request.payload
        )
        val alarm = createAlarmUseCase.createAlarm(command)
        return AlarmResponse.fromDomain(alarm)
    }

    @GetMapping("/{id}")
    @Operation(summary = "알람 상세 조회", description = "지정된 UUID의 알람 상세 정보를 조회합니다.")
    fun getAlarm(@PathVariable id: UUID): AlarmResponse {
        val alarm = getAlarmQuery.getAlarm(id)
        return AlarmResponse.fromDomain(alarm)
    }

    @GetMapping
    @Operation(summary = "알람 목록 조회", description = "페이지네이션을 적용하여 전체 알람 목록을 조회합니다.")
    fun listAlarms(
        @PageableDefault(size = 20) pageable: Pageable
    ): Page<AlarmResponse> {
        return getAlarmQuery.listAlarms(pageable).map { AlarmResponse.fromDomain(it) }
    }

    @PutMapping("/{id}")
    @Operation(summary = "알람 설정 수정", description = "알람의 이름, 상태 및 페이로드를 수정합니다.")
    fun updateAlarm(
        @PathVariable id: UUID,
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

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(summary = "알람 삭제", description = "알람을 완전히 삭제하고 예약을 취소합니다.")
    fun deleteAlarm(@PathVariable id: UUID) {
        deleteAlarmUseCase.deleteAlarm(id)
    }

    @PostMapping("/{id}/trigger")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(summary = "알람 즉시 발생", description = "스케줄 시각과 무관하게 알람을 즉시 발생시키고 실행 이력을 기록합니다.")
    fun triggerAlarm(@PathVariable id: UUID) {
        triggerAlarmUseCase.triggerAlarm(id)
    }
}
