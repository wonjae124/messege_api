# sendmock

Spring Boot 기반 Mock 발송 API입니다.

## Quick Start

```bash
git clone https://github.com/incutv/sendmock.git
cd sendmock
./gradlew bootRun
```

- 기본 포트: `8081`
- 엔드포인트: `POST /mock/send`

## Example Request

```bash
curl -i -X POST 'http://localhost:8081/mock/send' \
  -H 'Content-Type: application/json' \
  -d '{
    "requestId": "req-001",
    "channelType": "SMS",
    "receiver": "01012345678",
    "message": "test message"
  }'
```

## Requirements

- Java `21`
