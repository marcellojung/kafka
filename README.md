# Kafka-PostgreSQL 데이터 파이프라인

이 프로젝트는 Kafka로부터 메시지를 컨슈밍하여 PostgreSQL 데이터베이스에 배치로 저장하는 멀티프로세스 기반의 데이터 파이프라인입니다.

## 주요 기능

- Kafka 토픽의 메시지를 멀티프로세스로 컨슈밍
- JSON 메시지 파싱 및 PostgreSQL 데이터베이스에 배치 저장
- 워커 프로세스 자동 재시작 기능
- 로그 로테이션 및 중앙화된 로깅 시스템
- 설정 파일 기반의 유연한 구성

## 시스템 요구사항

- Python 3.x
- Apache Kafka
- PostgreSQL
- 필요한 Python 패키지:
  - kafka-python
  - psycopg2
  - pyyaml

## 설치 방법

1. 저장소 클론:
```bash
git clone [repository-url]
cd [repository-name]
```

2. 필요한 패키지 설치:
```bash
pip install -r requirements.txt
```

3. 설정 파일 생성:
`config.yaml` 파일을 생성하고 다음과 같은 형식으로 설정:

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  group_id: "your-group-id"
  auto_offset_reset: "earliest"

postgres:
  host: "localhost"
  port: 5432
  user: "your-username"
  password: "your-password"
  database: "your-database"

topics:
  topic_name:
    table: "target_table_name"
    partitions: 3
    columns: ["column1", "column2", "column3"]

log_file: "path/to/logfile.log"
batch_size: 500
```

## 실행 방법

```bash
python consumer.py
```

## 아키텍처

- **메인 프로세스**: 워커 프로세스들의 상태를 모니터링하고 관리
- **워커 프로세스**: 각각 특정 Kafka 토픽의 파티션을 담당하여 메시지 처리
- **로깅 프로세스**: 중앙화된 로깅 시스템 관리

## 주요 특징

1. **멀티프로세스 처리**
   - 각 토픽/파티션별로 독립적인 워커 프로세스 실행
   - 워커 프로세스 자동 재시작 기능

2. **배치 처리**
   - 메시지를 배치 단위로 처리하여 데이터베이스 부하 최적화
   - 설정 가능한 배치 크기

3. **안정성**
   - 예외 처리 및 에러 복구 메커니즘
   - 로그 로테이션으로 디스크 공간 관리
   - 프로세스 모니터링 및 자동 재시작

4. **설정 관리**
   - YAML 기반의 설정 파일
   - 토픽별 테이블 매핑 및 컬럼 설정
   - 유연한 데이터베이스 및 Kafka 설정

## 로깅

- 일별 로그 파일 로테이션
- 표준 출력 및 파일 로깅
- 중앙화된 로그 관리 시스템

## 주의사항

- 데이터베이스 연결 정보는 안전하게 관리해야 합니다.
- Kafka 컨슈머 그룹 ID는 고유해야 합니다.
- 배치 크기는 시스템 리소스에 맞게 조정해야 합니다.