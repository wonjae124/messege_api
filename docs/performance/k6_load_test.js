import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend, Rate } from 'k6/metrics';
// FNV-1a 64-bit hash to convert Idempotency-Key string to a consistent BIGINT
function idempotencyKeyToLong(str) {
  const FNV_OFFSET_BASIS = 14695981039346656037n;
  const FNV_PRIME = 1099511628211n;
  let hash = FNV_OFFSET_BASIS;
  for (let i = 0; i < str.length; i++) {
    hash ^= BigInt(str.charCodeAt(i));
    hash = (hash * FNV_PRIME) & ((1n << 64n) - 1n); // keep 64 bits
  }
  return hash;
}

// Simple Snowflake-like ID generator as fallback (timestamp + random bits)
function generateSnowflakeId() {
  const timestamp = BigInt(Date.now());
  const random = BigInt(Math.floor(Math.random() * 0xFFFF));
  return (timestamp << 16n) | random;
}
// 커스텀 메트릭 정의
export const successCounter = new Counter('successful_alarms');
export const failureCounter = new Counter('failed_alarms');
export const responseTimeTrend = new Trend('alarm_response_time');
export const successRate = new Rate('alarm_success_rate');

export const options = {
  scenarios: {
    constant_request_rate: {
      executor: 'constant-arrival-rate',
      rate: __ENV.RPS || 10,
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 2000, // (증가됨) 테스트 시작 전에 미리 VU를 넉넉하게 생성해두어 Dropped Iteration 방지
      maxVUs: 5000, // 서버가 아무리 느려도 K6가 절대 포기하지 않도록 매우 넉넉히 잡음
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<500'],
    http_req_failed: ['rate<0.01'],
    alarm_success_rate: ['rate>0.99'],
  },
};

export default function () {
  // TODO: 실제 알림 발송 API 엔드포인트와 Body 데이터로 수정하세요
  const url = 'http://localhost/notifications';
  const payload = JSON.stringify({
    receiverId: `user-${__VU}`,
    name: "RPS Test Alarm",
    payload: JSON.stringify({ message: "테스트 알림입니다." })
  });

  // Use provided Idempotency-Key or generate a consistent BIGINT via hashing
  const rawIdempotencyKey = __ENV.IDEMPOTENCY_KEY || '';
  const idempotencyLong = rawIdempotencyKey ? idempotencyKeyToLong(rawIdempotencyKey) : generateSnowflakeId();
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': idempotencyLong.toString()
    },
  };

  const res = http.post(url, payload, params);

  // 응답 검증 (2xx 응답 모두 성공 처리)
  const isSuccess = check(res, {
    'is status 2xx': (r) => r.status >= 200 && r.status < 300,
  });

  // 메트릭 수집
  if (isSuccess) {
    successCounter.add(1);
    successRate.add(1);
    responseTimeTrend.add(res.timings.duration);
  } else {
    failureCounter.add(1);
    successRate.add(0);
  }
}
