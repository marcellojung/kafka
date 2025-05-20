#!/usr/bin/env python3
"""
Kafka로부터 메시지를 컨슈밍하여, 메시지를 파싱한 후 PostgreSQL 데이터베이스에 배치로 저장하는 기능을 제공
다중 프로세스로 워커를 실행하여 각 토픽 및 파티션에 대한 처리를 수행하며, 부모 프로세스에서 워커의 상태를 모니터링하여 종료된 워커를 자동으로 재시작
"""

import json
from logging.handlers import TimedRotatingFileHandler
import logging
import os
import signal
import sys
import time
from timeit import default_timer as timer
from multiprocessing import Process, current_process, Queue  # 다중 프로세싱을 위한 모듈
import yaml

from kafka import KafkaConsumer
import psycopg2

# SafeTimedRotatingFileHandler를 정의하여 disk full 등의 에러 발생 시 예외를 무시
class SafeTimedRotatingFileHandler(TimedRotatingFileHandler):
    def handleError(self, record):
        import sys
        sys.stderr.write("Logging error (possibly disk full). Log record dropped.\n")

# 설정파일 경로 지정 (YAML 포맷)
CONFIG_FILE = "config.yaml"

def load_config():
    """
    외부 YAML 설정파일을 로드하는 함수.
    
    - 설정 파일을 열어 YAML 형식으로 파싱하여 Python 객체(dict)로 반환
    - 파일 열기나 파싱 중 예외가 발생할 경우, 에러 로그를 남기고 프로그램을 종료
    """
    try:
        with open(CONFIG_FILE, "r") as f:
            # YAML 형식의 설정 파일을 안전하게 로드
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        # 설정 파일 로드 실패 시 기본 로깅 설정 (stderr 및 stdout) 후 에러 기록
        logging.basicConfig(
            level=logging.ERROR,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        logging.error(f"설정파일({CONFIG_FILE}) 로드 실패: {e}")
        sys.exit(1)

def setup_logging(config, log_queue):
    """
    로그 기록을 위한 핸들러를 설정하는 함수.
    
    - config에 정의된 'log_file' 항목을 참고하여, 
      TimedRotatingFileHandler를 사용해 일자별로 로그 파일을 분할하고,
      stdout 핸들러를 추가합니다.
    - 먼저 기존에 등록된 모든 핸들러를 제거한 후 새롭게 설정합니다.
    - log_queue가 제공되면 QueueHandler를 사용하여 로그 메시지를 큐로 전달합니다.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # 기존 등록된 핸들러들을 제거하여 중복 로그 기록 방지
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    from logging.handlers import QueueHandler
    qh = QueueHandler(log_queue)
    logger.addHandler(qh)

def logging_listener_process(log_queue, config):
    """
    로그 기록을 전담하는 프로세스 함수.
    워커 및 메인 프로세스로부터 전달받은 로그 메시지를 처리하기 위해,
    SafeTimedRotatingFileHandler와 StreamHandler를 설정하고, QueueListener를 실행합니다.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # 기존 핸들러 제거
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    handlers = []
    log_file = config.get("log_file")
    if log_file:
        # SafeTimedRotatingFileHandler를 사용하여 disk full 상황에서도 예외를 무시함
        file_handler = SafeTimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=3)
        file_handler.suffix = "%Y-%m-%d"
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        handlers.append(file_handler)
    
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    handlers.append(stream_handler)
    
    listener = logging.handlers.QueueListener(log_queue, *handlers)
    listener.start()
    while True:
        time.sleep(1)

def build_insert_query(table_name, columns):
    cols = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    return query

def process_batch(messages, topic, conn, config):
    """
    한 배치의 JSON 메시지를 파싱하여 DB에 저장
    
      1. 각 토픽에 대한 설정 정보 (대상 테이블, 컬럼 목록)를 config에서 조회
      2. 각 메시지에서 기본 필드와 추가 측정값을 추출하여, 설정된 컬럼 순서에 맞게 데이터를 구성
         - 기본 필드: cell, localDn, beginTime
         - 그 이후의 필드는 config에 정의된 컬럼 순서대로 채움
      3. 구성된 데이터에 대해 SQL insert 쿼리를 실행하여 DB에 배치로 저장
      4. 예외 발생 시 해당 메시지를 건너뛰고 에러 로그를 기록
    """
    topic_conf = config["topics"][topic]
    table_name = topic_conf["table"]
    columns = topic_conf["columns"]

    fieldAnalysisTimer = None
    dbQueryTimer = None
    
    # 배치 insert를 위한 SQL 쿼리 생성
    insert_query = build_insert_query(table_name, columns)
    rows = []  # insert할 데이터 행들을 담을 리스트

    # 메시지 리스트를 순회하면서 각 메시지에서 데이터를 추출하며 최소 beginTime과 최대 beginTime을 기록
    minBeginTime = None
    maxBeginTime = None
    for msg in messages:
        try:
            # 기본 필드 추출: 메시지 구조에 따라 cell,beginTime 정보를 가져옴
            cell = msg.get("cell")
            beginTime = msg.get("beginTime")

            if minBeginTime == None or beginTime < minBeginTime:
                minBeginTime = beginTime
            if maxBeginTime == None or beginTime > maxBeginTime:
                maxBeginTime = beginTime
            
            # 측정값은 'values' 항목에 리스트 형태로 존재하며, 각 원소는 [컬럼명, 값] 형태임
            measurement_values = {}
            for pair in msg.get("values", []):
                # 리스트 길이가 2인 경우에만 유효한 데이터로 간주
                if len(pair) == 2:
                    col_name, value = pair
                    measurement_values[col_name] = value
            
            # 기본 필드 2개 후, config에 정의된 측정 컬럼 순서대로 값을 채움 (값이 없으면 None)
            row = [cell, beginTime]
            for col in columns[3:]:
                row.append(measurement_values.get(col))
            
            # 최종적으로 tuple 형태로 행 데이터를 추가
            rows.append(tuple(row))
        except Exception as e:
            logging.error(f"메시지 파싱 에러: {e}")

    fieldAnalysisTimer = timer() # json 필드 추출 및 최종 tuple 형태로 행 데이터 작성 완료 후 타이머 기록

    # 만약 파싱된 데이터가 없으면 DB 작업 없이 종료
    if not rows:
        return

    try:
        cur = conn.cursor()
        # 다수의 행을 한 번에 insert하는 executemany() 실행
        cur.executemany(insert_query, rows)
        conn.commit()  # 커밋하여 데이터베이스에 반영
        cur.close()
        # logging.info(f"[{current_process().name}] {table_name} 테이블에 {len(rows)} 건({minBeginTime} ~ {maxBeginTime}) 적재")
    except Exception as e:
        logging.error(f"DB 적재 에러 (테이블 {table_name}): {e}")
        conn.rollback()  # 에러 발생 시 롤백

    dbQueryTimer = timer() # DB insert 쿼리 완료 후 타이머 기록
    # 타이머 기록을 위한 리턴
    return (fieldAnalysisTimer, dbQueryTimer, len(rows))


def worker_process(topic, partition, config, log_queue):
    """
    개별 워커 프로세스의 역할:
      - 특정 Kafka 토픽의 특정 파티션을 담당하여 메시지를 컨슈밍
      - 일정 배치 단위로 메시지를 처리하여 DB에 저장한 후, 커밋
      - 처리 중 발생하는 예외는 로깅 후 계속해서 처리
    """
    # setup_logging을 사용하여 로그 메시지를 전담 프로세스로 전달
    setup_logging(config, log_queue)
    
    logging.info(f"[{current_process().name}] 시작 - topic: {topic}, partition: {partition}")
    
    # 워커 프로세스 내에서 단일 DB 커넥션을 생성 (커넥션 풀 대신 단일 커넥션 사용)
    postgres_conf = config["postgres"]
    conn = psycopg2.connect(
        host=postgres_conf["host"],
        port=postgres_conf["port"],
        user=postgres_conf["user"],
        password=postgres_conf["password"],
        database=postgres_conf["database"]
    )
    
    # Kafka 관련 설정 정보를 config에서 추출
    kafka_conf = config["kafka"]
    # 배치 처리 시 한 번에 처리할 최대 메시지 수, 기본값 500
    batch_size = config.get("batch_size", 500)
    messages_buffer = []  # 배치 단위로 처리할 메시지 저장용 버퍼

    try:
        # KafkaConsumer 인스턴스 생성
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_conf["bootstrap_servers"],
            group_id=kafka_conf["group_id"],
            auto_offset_reset=kafka_conf.get("auto_offset_reset"),
            enable_auto_commit=False,  # 자동 오프셋 커밋을 비활성화하여 직접 제어
            consumer_timeout_ms=3600000  # 메시지 없을 경우 타임아웃 (1시간)
        )
    except Exception as e:
        logging.error(f"[{current_process().name}] Kafka 컨슈머 생성 실패: {e}")
        return  # 컨슈머 생성 실패 시 해당 워커 종료

    while True:
        fieldAnalysisTimer = None
        dbQueryTimer = None
        rowCount = None
        try:
            # poll()을 통해 batch_size에 해당하는 메시지를 소비 (최대 대기 시간 10s)
            startPollingTimer = timer() # polling 전 타이머 기록
            records = consumer.poll(timeout_ms=10000, max_records=batch_size)
            endPollingTimer = timer() # polling 완료 타이머 기록
            for tp, messages in records.items():
                for message in messages:
                    try:
                        # 메시지의 value를 UTF-8 문자열로 디코딩한 후 JSON 파싱
                        decoded = message.value.decode("utf-8")
                        data = json.loads(decoded)
                        messages_buffer.append(data)
                    except Exception as e:
                        logging.error(f"[{current_process().name}] 메시지 디코딩/파싱 에러: {e}")
            endDecodeAndLoadsTimer = timer() # utf-8 디코딩 및 json 변환 완료 후 타이머 기록

            # 버퍼에 데이터가 있으면 DB 배치 저장 및 오프셋 커밋 처리
            if messages_buffer:
                fieldAnalysisTimer, dbQueryTimer, rowCount = process_batch(messages_buffer, topic, conn, config)
                messages_buffer = []  # 버퍼 초기화
                consumer.commit()    # 커밋을 통해 처리 완료된 메시지의 오프셋 갱신
        except Exception as e:
            # 처리 중 예외 발생 시 에러 로그 기록 후 5초 대기 후 재시도
            logging.error(f"[{current_process().name}] 처리 중 예외: {e}")
            time.sleep(5)
            continue



def handle_termination(signum, frame):
    """
    종료 시그널 처리 함수.
    """
    logging.info(f"프로세스 종료 신호({signum}) 수신: {current_process().name}")
    sys.exit(0)

def main():
    """
    메인 함수:
      - 부모 프로세스는 주기적으로 워커 프로세스의 상태를 모니터링하여 종료된 워커가 있으면 자동으로 재시작
      - KeyboardInterrupt가 발생하면 모든 워커를 종료하고 프로그램을 종료
    """
    # 설정 파일 로드 및 파싱
    config = load_config()

    # 종료 시그널(SIGTERM, SIGINT) 처리 핸들러 등록
    signal.signal(signal.SIGTERM, handle_termination)
    signal.signal(signal.SIGINT, handle_termination)
    
    # 로그 메시지 전달용 큐 생성
    log_queue = Queue(-1)
    
    # 로그 기록을 전담하는 프로세스 시작
    logging_listener = Process(target=logging_listener_process, args=(log_queue, config))
    logging_listener.start()
    
    # 메인 프로세스도 QueueHandler를 사용하여 로그 메시지를 전담 프로세스로 전달
    setup_logging(config, log_queue)


    # 각 워커의 정보를 저장할 리스트 (각 항목은 topic, partition, process 객체를 포함)
    worker_infos = []

    # 설정 파일 내 topics 항목을 순회하며 각 토픽에 대해 정의된 파티션 수 만큼 워커 생성
    for topic, topic_conf in config["topics"].items():
        partitions = topic_conf["partitions"]
        for partition in range(partitions):
            proc = Process(target=worker_process, args=(topic, partition, config, log_queue))
            proc.start()  # 워커 프로세스 시작
            # 생성된 프로세스 정보를 worker_infos 리스트에 저장
            worker_infos.append({"topic": topic, "partition": partition, "process": proc})
            logging.info(f"워커 프로세스 생성 - topic: {topic}, partition: {partition}")

    # 부모 프로세스는 무한 루프를 돌며 각 워커의 상태를 모니터링함
    while True:
        for worker in worker_infos:
            proc = worker["process"]
            # 워커 프로세스가 살아있지 않은 경우 재시작
            if not proc.is_alive():
                logging.error(
                    f"워커 프로세스 종료됨 - topic: {worker['topic']}, partition: {worker['partition']}. 재시작합니다."
                )
                new_proc = Process(
                    target=worker_process,
                    args=(worker["topic"], worker["partition"], config, log_queue)
                )
                new_proc.start()  # 종료된 워커 대신 새 워커 시작
                worker["process"] = new_proc  # 새 프로세스 객체로 업데이트
        time.sleep(5)  # 주기적으로 상태를 확인 (5초 간격)

if __name__ == '__main__':
    main()
