# Alarm API 성능 측정 및 모니터링 가이드 (PERFORMANCE.md)

이 문서는 Alarm API 시스템의 성능을 측정하고 상태를 모니터링하기 위해 구성된 메트릭 분류와 수집 위치를 설명합니다. 이 프로젝트는 **Spring Boot Actuator + Micrometer + OpenTelemetry + Grafana LGTM 스택**을 기반으로 실시간 관측성(Observability)을 제공합니다.

---

## 1. 전체 모니터링 아키텍처

```
┌─────────────────────────────────┐
│           Alarm API             │
├─────────────────────────────────┤
│ - Micrometer Metrics            ├─┐ (OTLP HTTP / 4318)
│ - OpenTelemetry Tracing & Logs  ├─┼────────────────────────┐
└─────────────────────────────────┘ │                        ▼
                                    │               ┌────────────────┐
┌─────────────────────────────────┐ │               │   Grafana UI   │
│       alarm-api (Replica 2)     │ │               │ (http:localhost│
├─────────────────────────────────┤─┘               │     :3000)     │
│ - Micrometer Metrics            │                 └───────┬────────┘
│ - OpenTelemetry Tracing & Logs  │                         │ (Queries)
└─────────────────────────────────┘                         ▼
                                                    ┌────────────────┐
                                                    │   otel-lgtm    │
                                                    │ (All-in-One)   │
                                                    │ - Prometheus   │
                                                    │ - Grafana      │
                                                    │ - Loki         │
                                                    │ - Tempo        │
                                                    └────────────────┘
```

---

## 2. 영역별 성능 측정 지표 및 수집 위치

성능 측정은 애플리케이션(JVM), 외부 API 연동, 카프카 컨슈머/프로듀서, 그리고 데이터베이스(HikariCP)의 4대 핵심 영역으로 나뉩니다.

### ① 애플리케이션 단 (JVM & System)
애플리케이션 자체의 자원 사용량과 JVM 내부 상태를 추적하여 메모리 누수, Full GC 지연 등의 장애 징후를 감지합니다.

| 측정 대상 | 주요 메트릭명 | 설명 | 임계치 및 조치 |
| :--- | :--- | :--- | :--- |
| **CPU 사용률** | `system_cpu_usage` | 시스템 전체 CPU 사용량 (0.0 ~ 1.0) | 0.8 (80%) 이상 지속 시 스케일 아웃 |
| **JVM 힙 메모리** | `jvm_memory_used_bytes{area="heap"}` | 현재 점유 중인 힙 메모리 크기 | 최대값의 85% 초과 시 GC 튜닝 필요 |
| **Full GC 빈도** | `jvm_gc_pause_seconds_count` | JVM GC 일시정지 발생 횟수 (Full GC 분류 필터) | 급증 시 메모리 누수 의심 |
| **GC 일시정지 시간** | `jvm_gc_pause_seconds_sum` | GC로 인한 총 일시정지 시간 (Pause Time) | 단건 pause > 1초 발생 시 Stop-the-world 감지 |

*   **수집 위치**: Micrometer JVM Binder (`JvmMemoryMetrics`, `JvmGcMetrics` 등 스프링 부트 자동 활성화)
*   **Grafana 대시보드 패널**: "JVM (Micrometer)" 공식 대시보드 연동 권장

### ② 외부 API 연동 단 (Mock API / Circuit Breaker)
외부 알림 API (`MockApiNotificationAdapter`) 호출의 지연 시간 분포와 서킷 브레이커 상태를 측정합니다.

| 측정 대상 | 주요 메트릭명 | 설명 | 측정 위치 및 형태 |
| :--- | :--- | :--- | :--- |
| **응답 레이턴시** | `http_client_requests_seconds` | 외부 API 호출 성공/실패 시 소요된 경과 시간 | WebClient Filter 인터셉트 |
| **백분위 지연** | `http_client_requests_seconds_bucket` | p50, p95, p99 레이턴시 스냅샷 수집 | histogram 기반 백분위 계산 |
| **서킷 상태** | `resilience4j_circuitbreaker_state` | 서킷 브레이커 현재 상태 (`closed`, `open`, `half_open`) | `Resilience4j` 메트릭 바인더 |
| **호출 실패율** | `resilience4j_circuitbreaker_failure_rate` | 서킷 슬라이딩 윈도우 내 실패율 (%) | 50% 초과 시 서킷 OPEN 전이 |

*   **수집 위치**: WebClient 및 Resilience4j AOP 인터셉터
*   **성능 요건**: 외부 API p99 레이턴시가 3초 이상 지속될 시 서킷 브레이커 자동 오픈 구성 완료

### ③ 카프카 단 (Kafka Consumer & Producer)
카프카 메시지 처리 지연과 유입 대기량(Lag)을 실시간으로 추적하여 메시지 백로그 장애를 예방합니다.

| 측정 대상 | 주요 메트릭명 | 설명 | 중요도 |
| :--- | :--- | :--- | :--- |
| **컨슈머 렉(Lag)** | `kafka_consumergroup_lag` | 토픽의 최신 오프셋과 컨슈머 그룹의 커밋 오프셋 차이 | **최우선 모니터링**: Lag 누적 시 컨슈머 증설 또는 스레드 튜닝 필요 |
| **폴링 소요 시간** | `kafka_consumer_poll_seconds` | Kafka 브로커로부터 메시지 배치를 가져오기 위한 폴링 시간 | 브로커 및 네트워크 지연 판정 |
| **메시지 발행 시간** | `kafka_producer_request_latency_max` | 프로듀서가 Kafka 브로커로 메시지를 발행할 때의 최대 지연 | 프로듀서 버퍼 꽉 참 현상 진단 |

*   **수집 위치**: `spring-kafka` 내부 메트릭 연동 + Actuator Kafka Metrics Binder

### ④ 데이터베이스 단 (HikariCP & JPA)
H2 TCP 서버와의 커넥션 점유 및 쿼리 지연을 측정하여 DB 병목을 모니터링합니다.

| 측정 대상 | 주요 메트릭명 | 설명 | 임계치 및 조치 |
| :--- | :--- | :--- | :--- |
| **활성 커넥션 수** | `hikaricp_connections_active` | 현재 쿼리를 수행 중인 액티브 커넥션 수 | 최대 풀 크기(10)에 근접하면 커넥션 병목 발생 |
| **대기 스레드 수** | `hikaricp_connections_pending` | 커넥션을 얻기 위해 대기 중인 스레드 수 | 0 초과 지속 시 `connection-timeout` 발생 위험 |
| **커넥션 획득 시간** | `hikaricp_connections_acquire_seconds` | 커넥션 풀에서 유휴 커넥션을 얻을 때까지 걸린 시간 | p95 > 250ms 시 풀 크기 증가 필요 |
| **실행로그 누적 시간**| `alarm_execution_logs.elapsed_time_ms` | 개별 알림 처리부터 DB 적재까지의 총 경과 시간 (RDB 직접 기록) | RDB 쿼리를 통해 슬로우 인스턴스/배치 성능 감지 |

*   **수집 위치**: HikariDataSource 메트릭 등록기 (`HikariMetricsTracker`), RDB 테이블 `alarm_execution_logs` 내 `elapsed_time_ms` 컬럼

---

## 3. Grafana 대시보드 구성 가이드

Docker Compose로 가동된 `otel-lgtm` 스택을 통해 수집된 메트릭을 가시화합니다.

1.  **Grafana 접속**: `http://localhost:3000` (최초 계정: `admin` / `admin` 또는 패스워드 없음)
2.  **데이터 소스 등록**: Prometheus (`http://localhost:9090`), Loki (`http://localhost:3100`), Tempo (`http://localhost:3200`)가 기본적으로 자동 연동되어 있습니다.
3.  **트레이스-로그-메트릭 연계(Trace-Log correlation)**:
    *   Grafana Loki에서 로그를 조회하면, 로그 패턴에 삽입된 `traceId`와 `requestId`를 추출할 수 있습니다.
    *   Loki 로그의 `traceId` 우측 **"Tempo" 링크**를 클릭하면 해당 로그를 유발한 REST API 호출부터 외부 API 호출까지의 전체 분산 트랜잭션 흐름을 시각적으로 모니터링할 수 있습니다.
