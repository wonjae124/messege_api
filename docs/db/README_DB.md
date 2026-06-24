# Database Information

본 문서는 현재 애플리케이션에서 사용 중인 데이터베이스(DB)의 상태, 엔진 설정, 접속 정보 등을 안내합니다.

## 사용 중인 DB 엔진

현재 프로젝트는 **H2 Database**를 사용하고 있습니다. 
주로 개발 및 로컬 테스트, 혹은 가벼운 컨테이너 환경을 위해 설정되어 있으며, Docker Compose를 통해 구동됩니다.

### DB 이미지 및 컨테이너
- **Image**: `oscarfonts/h2:latest`
- **Container Name**: `h2-db`

## 접속 정보 및 상태

Docker Compose 설정에 의해 DB는 TCP 모드와 Web Console 모드를 모두 지원합니다.

### 1. 애플리케이션 연결 정보 (JDBC)
- **URL (내부 컨테이너 통신 시)**: `jdbc:h2:tcp://h2-db:1521/alarmdb`
- **URL (호스트 PC에서 접근 시)**: `jdbc:h2:tcp://localhost:1521/alarmdb`
- **Username / Password**: 기본 H2 설정 (보통 `sa` / 빈 비밀번호)
- **접속 옵션**: `-ifNotExists` (DB가 없으면 자동 생성)

### 2. 웹 콘솔(Web UI) 접근
H2는 브라우저를 통해 DB 내용을 직접 조회할 수 있는 웹 콘솔을 제공합니다.
- **접속 주소**: [http://localhost:8082](http://localhost:8082)
- 웹 콘솔 로그인 시 JDBC URL 항목에 `jdbc:h2:tcp://h2-db:1521/alarmdb` (또는 `localhost`로 치환)을 입력하여 접속 가능합니다.

## 데이터 보존 (Persistence)
데이터베이스의 데이터는 Docker Volume (`h2-data`)을 사용하여 보존됩니다.
컨테이너가 재시작되어도 `h2-data` 볼륨이 삭제되지 않는 한, 저장된 알람 데이터 및 로그는 유지됩니다.

## 스키마 관리 구조
JPA (Hibernate) 연동을 통해 애플리케이션 구동 시 Entity 클래스를 기반으로 테이블 스키마가 자동 맵핑/생성되는 구조를 따릅니다.
자세한 테이블 구조 및 관계는 [docs/ERD.md](docs/ERD.md) 문서를 참고하시기 바랍니다.
