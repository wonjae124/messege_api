package com.alarm.adapter.`in`.web

import com.alarm.adapter.`in`.web.dto.ErrorResponse
import com.alarm.exception.*
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import jakarta.servlet.http.HttpServletRequest
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import java.time.LocalDateTime

/**
 * [파일명] GlobalExceptionHandler.kt
 * [기능] 애플리케이션 전역에서 발생하는 예외를 가로채어 통일된 ErrorResponse 형식으로 클라이언트에 반환
 */
@RestControllerAdvice
class GlobalExceptionHandler {

    private val log = LoggerFactory.getLogger(GlobalExceptionHandler::class.java)

    @ExceptionHandler(AlarmNotFoundException::class)
    fun handleAlarmNotFound(ex: AlarmNotFoundException, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        return buildResponse(HttpStatus.NOT_FOUND, "ALARM_NOT_FOUND", ex.message ?: "Alarm not found", request)
    }

    @ExceptionHandler(DuplicateRequestException::class)
    fun handleDuplicateRequest(ex: DuplicateRequestException, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        return buildResponse(HttpStatus.CONFLICT, "DUPLICATE_REQUEST", ex.message ?: "Duplicate request", request)
    }

    @ExceptionHandler(IllegalArgumentException::class)
    fun handleIllegalArgument(ex: IllegalArgumentException, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        return buildResponse(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", ex.message ?: "Invalid request argument", request)
    }

    @ExceptionHandler(ExternalApiException::class)
    fun handleExternalApi(ex: ExternalApiException, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        return buildResponse(HttpStatus.BAD_GATEWAY, "EXTERNAL_API_FAILURE", ex.message ?: "External API failure", request)
    }

    @ExceptionHandler(CircuitBreakerOpenException::class)
    fun handleCircuitBreakerOpen(ex: CircuitBreakerOpenException, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        return buildResponse(HttpStatus.SERVICE_UNAVAILABLE, "CIRCUIT_BREAKER_OPEN", ex.message ?: "Circuit breaker is open", request)
    }

    @ExceptionHandler(CallNotPermittedException::class)
    fun handleCallNotPermitted(ex: CallNotPermittedException, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        return buildResponse(HttpStatus.SERVICE_UNAVAILABLE, "CIRCUIT_BREAKER_OPEN", "Circuit breaker is open - call not permitted", request)
    }

    @ExceptionHandler(Exception::class)
    fun handleFallback(ex: Exception, request: HttpServletRequest): ResponseEntity<ErrorResponse> {
        log.error("Unhandled exception occurred: ", ex)
        return buildResponse(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Internal server error occurred", request)
    }

    private fun buildResponse(
        status: HttpStatus,
        code: String,
        message: String,
        request: HttpServletRequest
    ): ResponseEntity<ErrorResponse> {
        val requestId = MDC.get("requestId")
        val errorResponse = ErrorResponse(
            timestamp = LocalDateTime.now(),
            status = status.value(),
            error = status.reasonPhrase,
            code = code,
            message = message,
            requestId = requestId,
            path = request.requestURI
        )
        return ResponseEntity.status(status).body(errorResponse)
    }
}
