package com.alarm.config

import com.fasterxml.uuid.Generators
import jakarta.servlet.*
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.MDC
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component

/**
 * [파일명] RequestIdFilter.kt
 * [기능] HTTP 요청 진입 시 Request ID를 추출(헤더가 없는 경우 UUIDv7 생성)하여 MDC에 설정하고 응답 헤더로 반환합니다.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class RequestIdFilter : Filter {

    private val generator = Generators.timeBasedEpochGenerator()

    override fun init(filterConfig: FilterConfig?) {}

    override fun doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
        if (request is HttpServletRequest && response is HttpServletResponse) {
            var requestId = request.getHeader("X-Request-Id")
            if (requestId.isNullOrBlank()) {
                requestId = generator.generate().toString()
            }
            MDC.put("requestId", requestId)
            response.setHeader("X-Request-Id", requestId)
        }
        try {
            chain.doFilter(request, response)
        } finally {
            MDC.remove("requestId")
        }
    }

    override fun destroy() {}
}
