# 2주차 과제 - 아키텍처 및 동시성 제어 의사결정

## 1. Outbox Pattern 폴링 동시성 제어 (멀티 인스턴스 환경)

#### ❓ 고민하게 된 구체적 문제 상황
- **Outbox Pattern 도입**: PENDING 상태 이벤트를 1초마다 읽어 Kafka로 발행하는 스케줄러 구현
- **이슈 발생 (멀티 인스턴스)**: 서버 스케일 아웃 시 여러 인스턴스가 동일한 PENDING 레코드를 동시에 조회
- **결과**: 동일 알람이 Kafka에 다중 적재되는 **중복 발행(Race Condition) 위험** 노출
- **해결 요구사항**: 
  - 동시성 제어(Lock) 필수 적용
  - 메시지 순서 보장은 불필요
  - 데드락(Deadlock) 절대 회피
  - 다중 서버 환경의 자원을 효율적으로 활용한 빠른 처리(병렬성) 보장

---

#### ⚖️ 대안 비교

**A안: 애플리케이션 레벨의 독점 락 (ShedLock, Redis 분산 락 등)**
- **동작 방식**: 락을 선점한 단 1대의 인스턴스만 Outbox 테이블을 혼자서 처리
- **장점**: 
  - 코드가 매우 직관적이고 구현이 단순함
  - DB 레벨의 물리적 락 경합이 전혀 발생하지 않음
- **단점**: 
  - 나머지 서버들은 대기(유휴 상태)하므로 **스케일 아웃 효과 상실**
  - 대량 트래픽 상황에서 단일 처리로 인한 **심각한 성능 병목 우려**

**B안: DB 비관적 락을 활용한 병렬 처리 (`FOR UPDATE SKIP LOCKED`)**
- **동작 방식**: 여러 서버가 동시 접근할 때, 이미 락이 걸린 행(Row)은 기다리지 않고 과감히 건너뜀(Skip)
- **장점**: 
  - 대기 시간이 없어 **데드락(Deadlock)이 원천 차단됨**
  - 여러 인스턴스가 큐에 남은 데이터를 나눠서 **동시에 병렬 처리하므로 압도적으로 속도가 빠름**
- **단점**: 
  - 데이터베이스 벤더 스펙(MySQL 8.0+, PostgreSQL, H2 2.x 등)에 의존적임
  - 단순 독점 락 대비 쿼리 작성이 다소 까다로움

---

#### ✅ 결정: B안 (`FOR UPDATE SKIP LOCKED` 기반 비관적 락 병렬 처리)
- **선택 사유**: 
  - 다중 서버 환경에서 락 대기로 인한 유휴 자원 낭비를 최소화하기 위함
  - 순서 보장보다 **성능과 처리량이 더 중요한 알람 시스템의 특성**을 고려하여, 데드락 위험 없이 병렬 처리량을 극대화할 수 있기 때문임

#### ❌ 포기한 것
- ShedLock 등 스케줄러 단일 실행을 보장하는 라이브러리 기반의 단순하고 빠른 구현 방식

---

#### 💻 적용 쿼리 예시
```sql
-- 다른 인스턴스가 이미 락을 잡은 PENDING 레코드는 대기하지 않고 즉시 SKIP
-- 락이 걸리지 않은 다음 레코드들을 선점하여 곧바로 병렬 발행 처리 진행
SELECT * FROM alarm_outbox_events 
WHERE status = 'PENDING' 
FOR UPDATE SKIP LOCKED;
```

<br>

## 2. Outbox 발행 재시도(Retry) 및 원본 상태 동기화

#### ❓ 고민하게 된 구체적 문제 상황
- **Outbox 발행 실패**: Kafka 브로커 일시 장애나 네트워크 단절 시, Outbox 이벤트 발행이 실패하게 됨.
- **상태 불일치 문제**: 발행 실패 시 Outbox 상태를 즉시 `FAILED`로 변경하면 스케줄러가 다시 처리하지 않아 메시지가 유실됨. 반면 무한 재시도를 하면 서버 리소스 낭비 및 에러 로그 폭주 우려.
- **원본 알람 상태의 괴리**: Outbox 발행이 실패해도 클라이언트가 조회하는 원본 `Alarm` 엔티티는 여전히 `PENDING`으로 남아 있어, 실제로는 카프카 전송조차 실패한 것인지 구분이 불가능함.

---

#### ⚖️ 대안 비교

**A안: 실패 시 상태를 PENDING으로 유지 (무한 재시도)**
- **특징**: 에러만 로깅하고 아무 상태도 변경하지 않음. 다음 스케줄러가 1초 뒤에 또 가져가서 재시도함.
- **장점**: 구현이 가장 심플하며, 브로커 복구 즉시 알아서 전량 재발송됨.
- **단점**: 장애 지속 시 1초마다 모든 인스턴스가 쏟아내는 에러 로그로 시스템이 마비될 수 있음.

**B안: 최대 재시도 횟수(Max Retry) + 지수 백오프(Exponential Backoff)**
- **특징**: `retry_count`와 `next_retry_at` 컬럼을 도입. 
- **장점**: 
  - 재시도 간격을 점진적으로 늘려(5초 ➡️ 10초 ➡️ 20초) 단기 장애 시엔 빠르게 복구하고, 장기 장애 시엔 시스템 리소스 소모를 방어함.
  - 최대 횟수 초과 시 최종 실패(`PUBLISH_FAILED`) 처리하여 데드 레터 큐(DLQ) 형태로 보관 및 수동 처리 가능.
- **단점**: 엔티티와 테이블 스키마에 필드 추가가 필요함.

---

#### ✅ 결정: B안 (지수 백오프 기반의 제한적 재시도 + 원본 상태 동기화)
- **선택 사유**:
  - 알람 시스템의 안정성을 지키면서 불필요한 시스템 리소스 낭비를 막기 위해 채택함.
  - 카프카 장애의 경우 1분 내외로 복구되는 일시적 네트워크 순단이 많으므로, 짧은 백오프(5s, 10s, 20s)로 재시도하여 빠른 복구 도모.
- **추가 결정 사항 (상태 동기화)**:
  - 원본 알람(`Alarm`) 데이터에도 `PUBLISHED`, `PUBLISH_FAILED` 상태를 추가함.
  - 스케줄러가 카프카 발행 성공 시 원본 상태를 `PUBLISHED`로, 최종 3회 실패 시 `PUBLISH_FAILED`로 업데이트하여 클라이언트 측에서 명확한 진행/실패 상태를 인지할 수 있도록 설계함.

#### ❌ 포기한 것
- 장애 상황에 대한 대비 없이 무작정 `PENDING`으로 두고 1초마다 무한히 재시도하는 비효율적 방식.

---

#### 💻 적용 코드 예시 (핵심 요약)
```kotlin
// 1. 지수 백오프 재시도 시간 계산 (5초 ➡️ 10초 ➡️ 20초 대기)
val backoffSeconds = 5L * (1L shl (retryCount - 1)) 
nextRetryAt = LocalDateTime.now().plusSeconds(backoffSeconds)

// 2. 카프카 발행 결과에 따른 원본 알람 상태 동기화
if (publishSuccess) {
    alarmRepository.updateStatusesInBatch(alarmId, AlarmStatus.PUBLISHED)
} else if (retryCount > 3) {
    alarmRepository.updateStatusesInBatch(alarmId, AlarmStatus.PUBLISH_FAILED)
}
```

<br>

## 3. Consumer 멱등성이 휘발성 Redis 키에만 의존하는 문제 해결

#### ❓ 고민하게 된 구체적 문제 상황
- **기존 구조의 한계**: 기존 컨슈머는 중복 판단을 오직 Redis의 `proc:` 키(TTL 3분)에만 의존했으며, DB의 알람 상태는 조회하지 않았습니다.
- **유실 시나리오**:
  1. 컨슈머가 `PROCESSING` 선점 후 외부 API 호출 전에 크래시됨 (ack 전)
  2. 카프카가 장애 복구를 위해 똑같은 메시지를 재전달함
  3. 하지만 3분이 지나지 않아 Redis엔 여전히 `PROCESSING` 키가 살아있어 중복으로 오판하고 스킵함
  4. 스킵된 채 `ack.acknowledge()`가 호출되어, 메시지는 발송되지 않고 영원히 증발함
- **중복 시나리오**:
  - 발송 성공 기록(`SENT`)이 DB에만 저장되고 컨슈머는 이를 참조하지 않음.
  - 3분이 지나 Redis 키가 사라진 뒤 카프카에서 예전 메시지가 재전달되면, 멱등성을 방어할 수단이 없어 그대로 중복 발송됨.

---

#### ⚖️ 대안 비교 및 결정

**✅ 결정: DB를 Source of Truth로 활용하는 DCL(Double-Checked Locking) 구조 도입**

- **1차 검증 (DB)**: 수신한 배치 데이터의 상태를 DB에서 대량 조회 후, 이미 `SENT`, `FAILED`, `DEAD`인 건은 필터링하여 Redis 락 경합조차 하지 않음.
- **분산 락 (Redis)**: 남은 대상들만 Redis `setIfAbsent`를 통해 찰나의 동시성(Race Condition)을 방어.
- **2차 검증 (DB)**: 락을 획득한 이후 DB 상태를 다시 한번 조회하여, 락 대기 중 다른 인스턴스가 처리해버린 건을 차단.
- **상태 전이(SENDING) 및 안전한 Lock 해제**: 검증을 통과한 건만 `SENDING`으로 상태를 변경한 뒤 API를 호출하고, 예외 발생 여부와 무관하게 `finally` 블록에서 Redis 락을 해제. 모든 DB 반영이 끝난 뒤에만 오프셋 커밋(Ack) 수행.

#### 💡 결과 및 기대 효과
- **완벽한 중복 차단**: 수 밀리초 내의 동시 요청은 Redis가 튕겨내고, 장기적인 오프셋 재전달은 DB가 튕겨내는 2중 방어선 구축.
- **완벽한 유실 차단**: API 호출 전 크래시가 나더라도, `finally`에서 락이 해제되거나 TTL 만료 후 다음 재전달 시 DB 상태가 여전히 `PUBLISHED`이므로 정상적으로 재처리됨.
- **명확한 에러 라우팅**: HTTP 400 계열은 즉시 `FAILED`로, 500 계열/Timeout은 `RETRY` 토픽으로 재발행하며, 재시도 초과 시 `DEAD` 상태로 분류하여 운영 대응력을 극대화함.

---

#### 💻 적용 코드 예시 (핵심 요약)
```kotlin
// 1. 1차 DB 상태 검증 (SENT, FAILED, DEAD는 무시)
val initialAlarms = alarmRepository.findAllByIds(alarmIds)
val validPayloads = payloads.filter { isValidStatus(initialAlarms[it.alarmId]) }

// 2. Redis 분산 락 획득 시도 (단기 동시성 방어)
for (payload in validPayloads) {
    if (redisTemplate.setIfAbsent("proc:${payload.uuid}", "PROCESSING", 3분) == true) {
        lockedPayloads.add(payload)
    }
}

try {
    // 3. 2차 DB 상태 검증 (Double-Checked Locking)
    val finalAlarms = alarmRepository.findAllByIds(lockedAlarmIds)
    val finalPayloads = lockedPayloads.filter { isValidStatus(finalAlarms[it.alarmId]) }
    
    // 4. 상태 전이(SENDING) 및 API 대량 호출
    alarmRepository.updateStatusesInBatch(finalIds, AlarmStatus.SENDING)
    notificationPort.sendNotification(batchItems)
    
    // 5. 발송 결과 저장 및 에러 라우팅 (SENT, FAILED, RETRY, DEAD)
    alarmRepository.updateStatusesInBatch(successfulIds, AlarmStatus.SENT)
    
} finally {
    // 6. 예외 발생 여부와 무관하게 무조건 Redis 락 해제 및 Offset 커밋
    redisTemplate.delete(lockKeys)
    ack.acknowledge()
}
```

<br>

## 4. Producer(API) 멱등성 및 DB 장애 복구 설계

#### ❓ 고민하게 된 구체적 문제 상황
- **캐시와 DB의 라이프사이클 불일치**: `createAlarm` API에서 중복 요청을 방어하기 위해 Redis의 `setIfAbsent`를 이용해 5분짜리 락(Idempotency Key)을 잡고 DB에 알람과 Outbox를 저장함.
- **장애 시나리오 (재시도 불가 문제)**:
  - Redis 락을 잡은 직후, DB 커넥션 풀 고갈이나 데드락 등의 이유로 DB 저장이 실패함.
  - 트랜잭션이 롤백되고 클라이언트에게 500 에러가 반환됨.
  - 클라이언트가 즉시 재시도를 하지만, Redis에는 여전히 5분짜리 락이 남아있어 `409 Conflict` 에러를 뱉음.
  - 결국 **DB 문제로 실패한 요청은 5분 동안 절대 재처리가 불가능해지는 치명적 버그** 발생.

---

#### ⚖️ 대안 비교 및 결정

**A안: DB의 Unique Index 제약 조건을 Source of Truth로 활용**
- **방식**: Redis를 쓰지 않고(혹은 방어용으로만 쓰고), DB의 `(sender_id, uuid)`에 유니크 키를 걸어 중복 Insert 시 예외를 잡는 방식.
- **단점**: 악의적인 대량의 중복 요청(DDoS)이나 클라이언트 버그로 수백 번의 재시도가 들어올 경우, 모든 요청이 DB까지 도달하여 **DB 커넥션 풀을 심각하게 고갈시킬 위험**이 큼.

**B안: `try-catch` 기반의 Redis 락 보상 트랜잭션 (선택)**
- **방식**: DB 저장 로직을 `try-catch`로 감싸고, 예외 발생 시 `finally` 또는 `catch` 블록에서 선점했던 Redis 락을 즉시 지워버리는(`delete`) 방식.
- **장점**: 대량의 악성 트래픽은 메모리(Redis) 단에서 빠르게 쳐내어 DB를 보호하고, 내부 서버 오류로 인한 정상적인 재시도 요청은 즉시 허용할 수 있음.

**✅ 결정: B안 (Redis 락 해제) + DB 상태 확인(재처리 상태 반환)**
- **의사결정 사유**: 시스템 전체의 목줄인 **DB 커넥션 고갈을 방어하는 것이 1순위**이므로 1안(Redis 보상 트랜잭션)을 채택.
- **추가 해결책 (재처리 상태 확인)**:
  - 클라이언트가 락에 막혔을 때(Redis 키가 존재할 때) 무작정 409 에러를 뱉는 대신, DB를 한 번 찔러봄(`findById`).
  - DB에 이미 저장되어 있다면 과거에 성공한 요청이므로 **기존 알람 정보를 그대로 반환(200 OK)**하여 우아하게 멱등성을 보장함.
  - DB에 없다면 현재 다른 스레드가 방금 락을 잡고 처리 중인 것이므로 409 Conflict를 반환.

#### 💻 적용 코드 예시 (핵심 요약)
```kotlin
// 1. 단기 동시성 방어 (Redis 락 선점)
if (redisTemplate.setIfAbsent(idemKey, "PROCESSING", 5분) != true) {
    // 2. 재처리 상태 확인: DB에 이미 반영되었는지 확인 (Source of Truth)
    val existingAlarm = alarmRepository.findById(command.id)
    if (existingAlarm != null) return existingAlarm
    
    throw ResponseStatusException(HttpStatus.CONFLICT, "처리 중입니다.")
}

try {
    // 3. DB 저장 (알람, Outbox)
    val savedAlarm = alarmRepository.save(Alarm.create(...))
    alarmOutboxEventRepository.save(AlarmOutboxEvent.create(savedAlarm))
    return savedAlarm
} catch (e: Exception) {
    // 4. 장애 복구 로직: DB 저장 실패 시 클라이언트가 즉시 재시도할 수 있도록 Redis 키 삭제
    redisTemplate.delete(idemKey)
    throw e
}
```

<br>

## 5. 기타 아키텍처 및 설정 보완 내역 (feature2 브랜치 반영분)

main 브랜치의 기존 `README.md` 아키텍처 문서와 비교하여, 현재(feature2) 브랜치에서 추가적으로 고도화 및 보완된 사항들입니다.

### 5.1 각 아키텍처 결정에 대한 실제 구현 코드 스니펫 추가
기존 의사결정 내역들에 대해 실제 적용된 **코드 레벨의 증명(Code Snippets)**이 `README.md` 각 섹션 하단에 대폭 보강되었습니다.
- **[3.4] Rate Limiting**: `redisRateLimiter.tryAcquire`를 통한 초당 500건 제어 로직 추가.
- **[4.1] 큐 엔진**: `ack.acknowledge()` 수동 커밋을 통한 Kafka 장애 재처리 보장 로직 추가.
- **[4.2 / 4.4] 동시성 제어**: `redisTemplate.setIfAbsent`를 활용한 분산 락 로직 및 실패 시 키 삭제(`delete`)를 통한 재시도 허용 로직 추가.
- **[4.5] 에러 복구**: `WebClient`의 지수 백오프(`Retry.backoff`) 및 Kafka Retry Topic(`alarm-jobs-retry`) 재발행 로직 추가.

### 5.2 신규 챕터: Kafka 브로커 메시지 유실 및 중복 저장 방지 (4.6)
단일 브로커 및 기본 설정의 한계(메시지 유실 및 중복)를 해결하기 위한 인프라 레벨의 핵심 의사결정이 추가되었습니다.

#### ❓ 고민하게 된 구체적 문제 상황
- **메시지 유실**: `replication.factor=1` 상태에서 리더 브로커 장애 시, 팔로워에 복제되지 않은 메시지는 영구 유실됨.
- **메시지 중복**: 프로듀서가 메시지를 보냈으나 네트워크 오류로 ACK를 못 받아 재전송할 경우, 브로커에 2건이 중복 저장되어 유저에게 알림이 2번 감.

#### ⚖️ 대안 비교 및 결정
- **결정**: 브로커 3대 클러스터링 기반의 **`replication.factor=3` + `min.insync.replicas=2` + `enable.idempotence=true`** 적용.
- **기대 효과**: 리더 브로커가 죽어도 팔로워에 안전하게 메시지가 보존되며, 프로듀서의 재시도 시 브로커가 시퀀스 넘버(Sequence Number)를 통해 스스로 중복 저장을 차단함.

#### 💻 적용 코드 예시 (핵심 요약)
```kotlin
// [KafkaConfig.kt] 토픽 생성 시 replication 및 ISR 설정
TopicBuilder.name("alarm-jobs")
    .partitions(3)
    .replicas(3)  // 리더가 죽어도 팔로워에 데이터 보존
    .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")  // 최소 2곳 저장 확인 후 ACK
    .build()

// [application.properties]
// spring.kafka.producer.acks=all           ← enable.idempotence의 전제 조건
// enable.idempotence=true (Kafka 3.0+ 기본값) ← 재시도 시 브로커 중복 적재 원천 차단
```

<br>

## 6. PK 채번 방식 및 UUIDv7 전략 도입

#### ❓ 고민하게 된 구체적 문제 상황
- **UUIDv4와 클러스터드 인덱스 단편화**: 기존 시스템은 레코드 식별자(PK)로 `UUIDv4`를 사용하고 있었습니다. UUIDv4는 완전한 난수(랜덤)이므로, 데이터베이스의 PK(클러스터드 인덱스)로 사용될 경우 새로운 레코드가 인덱스 중간중간에 삽입되어 **페이지 분할(Page Split) 현상**을 심하게 유발합니다.
- **성능 저하**: 삽입(INSERT) 위치가 불규칙하여 물리적 디스크 I/O가 증가하고, 인덱스가 비대해지며 전반적인 쓰기 성능이 크게 저하되는 문제가 발생했습니다.

#### ⚖️ 대안 및 결정사항

**✅ 결정 1: 사용하는 테이블의 PK를 `BIGINT AUTO_INCREMENT` 로 변경**
- **이유**: 클러스터드 인덱스를 순차적으로 쌓이게 만들어 삽입 속도를 극대화하고 페이지 분할 및 인덱스 파편화를 원천 차단합니다.
- **방식**: JPA의 `@GeneratedValue(strategy = GenerationType.IDENTITY)` 및 `Long` 타입 PK를 도입하여, DB 레벨에서 안전하고 순차적인 식별자를 발급하도록 리팩토링했습니다.

**✅ 결정 2: 비즈니스 키(id, uuid) 등에 `UUIDv7` 전면 도입**
- **이유**: 분산 환경의 멱등성 보장이나 식별 편의상 외부 노출용 고유 키(UUID)는 여전히 필요합니다. 하지만 랜덤인 UUIDv4는 지양해야 합니다.
- **특징**: `UUIDv7`은 유닉스 타임스탬프(시간)와 난수를 결합하여 만들어지기 때문에 **시간순 정렬**이 가능합니다. 새 데이터가 대체로 인덱스의 뒤쪽에 추가되어 파편화가 적습니다.
- **기대 효과**: UUIDv4 대비 **INSERT 속도 약 2배 향상**, 인덱스 크기 30% 감소, 디스크 밀어내기(I/O 부하) 대폭 완화, 캐시 적중률 증가 효과를 얻을 수 있습니다.

#### 💻 적용 결과 (요약)
- **도메인 및 API 레이어**: 비즈니스 로직(Alarm 생성 등)에서 `UUID.randomUUID()` 대신 `UuidCreator.getTimeOrderedEpoch()` (UUIDv7)을 사용하도록 일괄 변경했습니다.
- **영속성(Persistence) 레이어**: 데이터베이스에는 `BIGINT` PK 필드(`seq`)를 추가하여 인덱스 효율을 챙기고, 도메인 식별자용 `uuid` 컬럼에 Unique 속성을 부여해 Source of Truth 구조를 완성했습니다.
