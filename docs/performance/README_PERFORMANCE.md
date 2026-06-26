# 알림 시스템 부하 테스트 및 성능 측정 계획서 (PERFORMANCE_PLAN.md)

이 문서는 알림 발송 시스템(`alarm-api`)의 한계를 파악하고 병목을 분석하기 위한 성능 테스트 계획, 환경 구성, 그리고 메트릭 측정 기준을 정의합니다.

## 🚀 빠른 테스트 실행 가이드

부하 테스트를 즉시 실행하고 결과를 확인하려면 다음 단계를 따르세요.

1. **테스트 스크립트 실행**
   - 성능 테스트 스크립트는 `docs/performance/` 디렉토리에 위치해 있습니다.
   - k6를 이용한 부하 테스트 실행 (RPS와 테스트 시간을 조절할 수 있습니다):
     ```bash
     k6 run -e RPS=100 -e DURATION=30s docs/performance/k6_load_test.js
     ```
   - 또는 준비된 자동화 스크립트(파워셸)를 실행하여 단계별 테스트를 진행할 수 있습니다.
     ```powershell
     ./docs/performance/run_rps_table.ps1
     ```

2. **Grafana 대시보드 모니터링**
   - 브라우저에서 **[http://localhost:3000](http://localhost:3000)** 에 접속합니다.
   - 테스트를 실행하면서 시스템 병목, API 응답 시간, 컨테이너 자원 사용량 등을 실시간으로 확인할 수 있습니다.

---

## 1. 테스트 환경 구성 (컴퓨팅 리소스)

성능 테스트의 베이스라인(기준점)을 명확히 잡기 위해 단일 인스턴스(Single Instance) 구조로 리소스를 통제합니다.

- **실행 환경**: 로컬 머신 (Docker Compose)
- **실행 명령**: `docker-compose -f docker-compose.perf.yml up -d`
- **컴포넌트별 리소스 할당 (`docker-compose.perf.yml` 기준)**:
  - **App 서버 (`alarm-api-perf`)**: 
    - CPU: 제한 없음 (호스트 CPU 자원 전체 활용하여 Spring Boot의 멀티스레드 동시성 보장)
    - Memory: `2048M` (2GB) 상한 제한 (OOM 발생 임계점 확인 용도)
    - 인스턴스 수: 1개 (로드밸런서 변인 통제)
  - **DB 서버 (`h2-db-perf`)**: 로컬 컨테이너 (In-memory 모드가 아닌 TCP 모드로 실제 I/O 지연 일부 모사)
  - **기타 인프라**: `kafka`, `zookeeper`, `redis`, `sendmock`, `otel-lgtm`(Prometheus/Grafana) 모두 로컬 컨테이너로 동시 구동

---

## 2. 모니터링 연동 및 수집 방법 (Integration & Code Locations)

Spring Boot의 다양한 내부 지표를 프로메테우스(Prometheus)로 스크랩하기 위해 다음과 같이 연동되어 있습니다.

1. **의존성 (수집기)**: `build.gradle.kts`
   - `io.micrometer:micrometer-registry-prometheus`: Spring의 각종 지표(JVM, Tomcat, Hikari, Kafka)를 Prometheus가 읽을 수 있는 포맷으로 변환.
   - `io.github.resilience4j:resilience4j-micrometer`: 서킷 브레이커 상태 및 지표 수집.
2. **설정 (엔드포인트 노출)**: `application.yml`
   - `management.endpoints.web.exposure.include: health,info,prometheus,metrics`: Actuator를 통해 `/actuator/prometheus` 엔드포인트를 외부에 개방.
   - `management.metrics.export.prometheus.enabled: true`
   - DB 풀 / 외부 HTTP 커넥션 풀 메트릭 활성화 (`metrics-enabled: true`, `pool-name: AlarmDBPool` 등 명시).
3. **수집 및 시각화 (인프라)**: `docker-compose.perf.yml`
   - `otel-lgtm` 컨테이너 내장 Prometheus가 `alarm-api-perf:8080/actuator/prometheus`를 주기적으로 스크랩(Pull).
   - Grafana (http://localhost:3000) 를 통해 스크랩된 메트릭 시각화.

---

## 3. 수집 지표 (영역별 측정 지표 및 정상/비정상 판단 기준)

첨부해주신 가이드라인에 맞추어, 프로메테우스(Grafana)에서 조회해야 할 영역별 핵심 메트릭 요약표와 각 지표의 상세 판단 기준을 정리했습니다.

### 📊 전체 수집 지표 요약표

| 영역 | 지표 (확인할 내용) | 출처(프로메테우스 메트릭 명) |
| :--- | :--- | :--- |
| **Producer (수신부)** | 수신 TPS, 수락 p95/p99, 에러율 | `http_server_requests_seconds_*` |
| **Relay/Kafka (전달부)** | 발행 rate, consumer lag (topic별) | `spring_kafka_template_*`,<br>`kafka_consumer_fetch_manager_records_lag_max` |
| **Consumer (처리부)** | 처리 TPS, 처리 p95/p99, 결과별 분포 | `spring_kafka_listener_seconds_*` |
| **External API (외부)** | sendmock 호출 p95/p99, 결과별 분포 | `http_client_requests_seconds_*`<br>또는 서킷브레이커 메트릭 |
| **DB** | Hikari active/pending/idle, acquire p95 | `hikaricp_connections_*` |
| **JVM** | heap, GC pause(평균/최대/빈도), CPU, threads | `jvm_memory_used_bytes`, `jvm_gc_pause_seconds_*`, `process_cpu_usage`, `tomcat_threads_busy_threads` |
| **컨테이너** | CPU(cores) - 메모리 vs limit | `container_cpu_usage_seconds_total`, `container_memory_usage_bytes` (또는 docker stats) |

---

### 🔎 지표별 상세 분석 및 체감 사례

각 영역(Domain)별로 프로메테우스에서 확인해야 할 정상/비정상 판단 수치와, 비정상일 때 실제로 어떤 일이 벌어지는지(체감 사례)를 구체적으로 분류했습니다.

#### 1. Producer (수신부 - Tomcat/웹)
- **출처(메트릭)**: `http_server_requests_seconds_*`, `tomcat_threads_busy_threads`
- **정상값**: p95 응답시간 **< 500ms**, 에러율 **0%**, Tomcat Active Threads < Max(기본 200)
- **비정상값**: p95 응답시간 **> 1초**, 5xx 에러율 급증, Tomcat Threads가 200에 고정됨
- **🚨 구체적 체감 사례**:
  - 클라이언트(사용자)가 '알림 발송' 버튼을 눌렀는데 화면에 5초 넘게 로딩 스피너만 빙글빙글 돕니다.
  - 결국 브라우저나 앱에서 타임아웃이 발생하며 "서버 응답이 없습니다(504 Gateway Timeout)" 팝업이 뜹니다.

### 2. Relay/Kafka (전달부 - 큐)
- **출처(메트릭)**: `spring_kafka_template_*`, `kafka_consumer_fetch_manager_records_lag_max`
- **정상값**: Consumer Lag이 특정 수치(예: 100~500) 내에서 유지되며, 부하 테스트가 끝나면 즉시(수십 초 내) Lag이 0으로 떨어짐(Drain 완료).
- **비정상값**: 부하를 주는 족족 Lag이 우상향으로 치솟음. 부하 주입이 끝났는데도 Lag 해소(Drain)에 수십 분이 걸리거나 전혀 줄지 않음.
- **🚨 구체적 체감 사례**:
  - 시스템 상으로는 "알림 발송 요청 성공(HTTP 200)"이 떴습니다.
  - 그런데 사용자가 "어? 보냈다고 떴는데 10분이 지나도 카카오톡이나 앱 푸시가 안 와요!" 하고 고객센터에 항의 전화를 합니다. (뒤쪽 큐가 꽉 막혀 처리되지 않는 상태)

### 3. Consumer & External API (처리부 및 외부 호출)
- **출처(메트릭)**: `spring_kafka_listener_seconds_*`, `http_client_requests_seconds_*`, `resilience4j_circuitbreaker_state`
- **정상값**: `sendmock` 응답이 일정하며, 서킷 브레이커 상태가 **CLOSED** 유지.
- **비정상값**: HTTP Connection Pool이 고갈(Pending 치솟음). 외부 응답 지연으로 서킷 브레이커가 **OPEN**으로 전환됨.
- **🚨 구체적 체감 사례**:
  - (서킷브레이커 미적용 시) 외부 카카오톡 API 서버가 3초씩 지연되자, 우리 서버의 모든 쓰레드가 외부 응답을 기다리느라 멈춰버립니다. 결국 알림 발송뿐만 아니라 회원가입, 로그인 등 우리 서버 전체가 먹통이 됩니다.
  - (서킷브레이커 적용 시) 외부 장애를 감지하고 즉시 차단(OPEN)하여 0.1초 만에 "지금은 알림을 보낼 수 없습니다"라고 빠르게 실패(Fail-Fast)시켜, 우리 서버의 다른 기능들은 살려냅니다.

### 4. DB (데이터베이스 커넥션)
- **출처(메트릭)**: `hikaricp_connections_*` (active, pending, idle)
- **정상값**: `active` < `max-pool-size` (기본 10), `pending` **== 0**
- **비정상값**: `active`가 10에 꽉 차서 수평선을 그리고, `pending`(커넥션 획득 대기) 수치가 치솟음.
- **🚨 구체적 체감 사례**:
  - DB 접속을 기다리다가 타임아웃이 나면서 스프링 콘솔에 `Connection is not available, request timed out after 3000ms` 에러 로그가 폭포수처럼 쏟아집니다. 시스템이 DB 병목으로 인해 완전히 마비된 상태입니다.

### 5. JVM 및 컨테이너 (인프라 자원)
- **출처(메트릭)**: `jvm_memory_used_bytes`, `container_cpu_usage_seconds_total`
- **정상값**: CPU 사용률 60~80% 선에서 부하 소화. JVM 힙 메모리는 주기적인 GC로 인해 일정한 '톱니바퀴' 모양 그래프를 그림.
- **비정상값**: CPU 100% 고정 (과도한 쓰레드 경합). 메모리가 우상향하다가 제한(2GB)을 뚫고 뚝 떨어짐(OOM 종료).
- **🚨 구체적 체감 사례**:
  - 갑자기 모니터링 알람(Slack 등)이 울립니다.
  - Docker 컨테이너가 뻗었다가 스스로 재시작되어 Uptime이 0초로 초기화되어 있습니다. 재시작되는 그 십여 초 동안 사용자의 모든 요청은 증발했습니다.

> 💡 **측정 시 공통 보정 (매우 중요)**
> - **로깅 제거**: 건당 SQL 로깅 제거 (이미 `application-perf.yml`에 `show-sql: false` 적용 완료)
> - **Lag 0 출발 보장**: 매 시나리오(예: 5 RPS 테스트 끝내고 10 RPS 시작 전)를 실행하기 전에는 반드시 남은 메시지를 모두 소진(Drain)시키거나, Redis/Kafka 오프셋을 리셋하여 **Lag이 0인 깨끗한 상태에서 출발**해야 정확한 측정이 가능합니다.

---

## 4. 단계별 부하 테스트 실험 시나리오

K6 스크립트(`k6_load_test.js`)를 통해 진행할 실험의 4단계 로드맵입니다. (기존 VU 기준에서 직관적인 **TPS/RPS 기준**으로 변경)

### 🧪 1단계: 베이스라인(Baseline) 측정
- **목적**: 현재 단일 인스턴스가 안정적으로 소화할 수 있는 최대 평상시 부하 파악
- **부하 설정**: **50 TPS** (약 2분 유지) (`k6 run -e RPS=50 -e DURATION=120s k6_load_test.js`)
- **확인 사항**: 위 지표들이 모두 '정상(Normal)' 범위 내에 들어오는지 확인.

### 🧪 2단계: 스트레스(Stress) 테스트 (한계점 탐색)
- **목적**: TPS를 점진적으로 늘려가며 시스템이 무너지는 최초의 병목 지점 확인
- **부하 설정**: **100 TPS -> 300 TPS -> 500 TPS** 계단식 증가
- **확인 사항**: 
  - 어떤 지표가 가장 먼저 '비정상' 상태로 진입하는가? (예: HikariCP Pending 증가 -> Tomcat Busy Thread 증가 -> HTTP Timeout 발생 순서)
  - 병목을 확인한 후, 설정(DB max-pool 등)을 튜닝하여 2단계를 재반복.

  > **💡 최적점 판단 기준 방법 (모니터링 및 병목 찾기)**
  > - **CPU 사용률이 100%인가?** -> 스레드나 커넥션 수를 늘리면 안 됩니다. (서버 스케일 아웃이 필요)
  > - **DB CPU가 100%인가?** -> 쿼리 튜닝(인덱스 등)을 하거나 DB 스펙을 올려야 합니다.
  > - **서버 자원(CPU/RAM)은 넉넉한데 TPS가 안 오르고 에러(Timeout)가 나는가?** -> 이때 비로소 스레드 풀이나 DB 커넥션 풀 개수를 조금씩 올려보며 병목을 해소합니다.

### 🧪 3단계: 외부 지연(Resilience / Slow Mock API) 내성 테스트
- **목적**: 연동된 외부 API가 느려질 때, 알림 서버 전체가 다운되는지, 아니면 서킷 브레이커가 동작해 정상적으로 Fail-Fast 하는지 검증
- **부하 설정**: **50 TPS** + `sendmock` 컨테이너 응답 지연(2~3초) 인위적 주입
- **확인 사항**: 
  - 서킷 브레이커가 **OPEN** 상태로 전환되는가?
  - 외부 지연에도 불구하고 서버의 Tomcat 쓰레드가 전부 고갈되지 않고 버텨내는가?

### 🧪 4단계: 비동기 스파이크(Spike) 테스트
- **목적**: 이벤트 당첨 등으로 알림이 순간적으로 폭주할 때 Kafka 큐가 메시지 유실 없이 잘 완충해주는지 확인
- **부하 설정**: 아주 짧은 시간(예: 10초) 동안 극단적인 부하(**1000 TPS**) 주입 (`k6 run -e RPS=1000 -e DURATION=10s k6_load_test.js`)
- **확인 사항**: 
  - HTTP 요청은 200 OK를 받고 빠르게 끝나는가?
  - 이후 백그라운드에서 Kafka Lag이 점진적으로 줄어들며 알림이 모두 발송 처리되는가?
